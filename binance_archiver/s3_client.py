import datetime
import hashlib
import hmac
import io
import re
import zipfile
import requests
from pathlib import Path

from binance_archiver.enum_.storage_connection_parameters import StorageConnectionParameters


class OwnLightweightS3Client:

    __slots__ = [
        'storage_connection_parameters',
        'region',
        'service',
        'host'
    ]

    def __init__(
            self,
            storage_connection_parameters: StorageConnectionParameters
    ) -> None:
        self.storage_connection_parameters = storage_connection_parameters

        region_match = re.search(
            r's3\.([^.]+)\.backblazeb2\.com',
            self.storage_connection_parameters.backblaze_endpoint_url
        )
        if not region_match:
            raise Exception('Region not found in endpoint url')
        self.region = region_match.group(1)
        self.service = "s3"
        self.host = f"s3.{self.region}.backblazeb2.com"

    @staticmethod
    def sign(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    def get_signature_key(
            self,
            key: str,
            date_stamp: str,
            region_name: str,
            service_name: str
    ) -> bytes:
        k_date = self.sign(('AWS4' + key).encode('utf-8'), date_stamp)
        k_region = self.sign(k_date, region_name)
        k_service = self.sign(k_region, service_name)
        return self.sign(k_service, 'aws4_request')

    def upload_existing_file(
            self,
            file_path: str
    ) -> None:

        with open(file_path, "rb") as f:
            file_data = f.read()

        object_name = Path(file_path).name

        self._upload_payload(file_data, object_name)

    def upload_zipped_jsoned_string(
            self,
            data: str,
            file_name: str,
            content_type: str = "application/octet-stream"
    ) -> None:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            zipf.writestr(f"{file_name}.json", data)
        zip_buffer.seek(0)
        object_name = f"{file_name}.zip"
        self._upload_payload(zip_buffer.getvalue(), object_name, content_type)

    def _upload_payload(
            self,
            payload: bytes,
            object_name: str,
            content_type: str = "application/octet-stream"
    ) -> None:
        t = datetime.datetime.utcnow()
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = t.strftime('%Y%m%d')
        method = 'PUT'
        canonical_uri = f'/{self.storage_connection_parameters.backblaze_bucket_name}/{object_name}'
        canonical_querystring = ""
        payload_hash = hashlib.sha256(payload).hexdigest()

        canonical_headers = (
            f'host:{self.host}\n'
            f'x-amz-content-sha256:{payload_hash}\n'
            f'x-amz-date:{amz_date}\n'
        )
        signed_headers = 'host;x-amz-content-sha256;x-amz-date'
        canonical_request = (
            f'{method}\n'
            f'{canonical_uri}\n'
            f'{canonical_querystring}\n'
            f'{canonical_headers}\n'
            f'{signed_headers}\n'
            f'{payload_hash}'
        )
        algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = f'{date_stamp}/{self.region}/{self.service}/aws4_request'
        string_to_sign = (
            f'{algorithm}\n'
            f'{amz_date}\n'
            f'{credential_scope}\n'
            f'{hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()}'
        )
        signing_key = self.get_signature_key(
            self.storage_connection_parameters.backblaze_secret_access_key,
            date_stamp,
            self.region,
            self.service
        )
        signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
        authorization_header = (
            f'{algorithm} Credential={self.storage_connection_parameters.backblaze_access_key_id}/{credential_scope}, '
            f'SignedHeaders={signed_headers}, Signature={signature}'
        )
        headers = {
            'x-amz-content-sha256': payload_hash,
            'x-amz-date': amz_date,
            'Authorization': authorization_header,
            'Content-Type': content_type,
        }
        url = self.storage_connection_parameters.backblaze_endpoint_url + canonical_uri

        response = requests.put(url, data=payload, headers=headers)
        response.raise_for_status()
