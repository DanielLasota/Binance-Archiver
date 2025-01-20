from __future__ import annotations

import io
import logging
import os
import threading
import time
import zipfile
from collections import defaultdict
import boto3
import re

# from azure.storage.blob import BlobServiceClient
from botocore.config import Config

from binance_archiver.difference_depth_queue import DifferenceDepthQueue
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.queue_pool import QueuePoolListener, QueuePoolDataSink
from binance_archiver.timestamps_generator import TimestampsGenerator
from binance_archiver.trade_queue import TradeQueue


class StreamDataSaverAndSender:

    __slots__ = [
        'config',
        'logger',
        'azure_blob_parameters_with_key',
        'azure_container_name',
        'backblaze_s3_parameters',
        'backblaze_bucket_name',
        'global_shutdown_flag',
        'azure_blob_service_client',
        'azure_container_client',
        's3_client',
        '_stream_message_pair_pattern'
    ]


    def __init__(
        self,
        logger: logging.Logger,
        config: dict,
        azure_blob_parameters_with_key: str | None = None,
        azure_container_name: str | None = None,
        backblaze_s3_parameters: dict | None = None,
        backblaze_bucket_name: str | None = None,
        global_shutdown_flag: threading.Event = threading.Event()
    ):
        self.config = config
        self.logger = logger
        self.azure_blob_parameters_with_key = azure_blob_parameters_with_key
        self.azure_container_name = azure_container_name
        self.backblaze_s3_parameters = backblaze_s3_parameters
        self.backblaze_bucket_name = backblaze_bucket_name
        self.global_shutdown_flag = global_shutdown_flag
        self._stream_message_pair_pattern = re.compile(r'"stream":"(\w+)@')

        if self.azure_blob_parameters_with_key and self.azure_container_name:
            try:
                self.azure_blob_service_client = BlobServiceClient.from_connection_string(
                    self.azure_blob_parameters_with_key
                )
                self.azure_container_client = self.azure_blob_service_client.get_container_client(
                    self.azure_container_name
                )
                self.logger.debug(f"Connected to Azure Blob container: {self.azure_container_name}")
            except Exception as e:
                self.logger.error(f"Could not connect to Azure: {e}")
                self.azure_blob_service_client = None
                self.azure_container_client = None
        else:
            self.azure_blob_service_client = None
            self.azure_container_client = None

        if self.backblaze_s3_parameters and self.backblaze_bucket_name:
            try:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.backblaze_s3_parameters.get('access_key_id'),
                    aws_secret_access_key=self.backblaze_s3_parameters.get('secret_access_key'),
                    endpoint_url=self.backblaze_s3_parameters.get('endpoint_url'),
                    region_name='us-east-1',
                    config=Config(signature_version='s3v4', retries={'max_attempts': 100,'mode': 'adaptive'})
                )
                self.logger.debug(f"Connected to Backblaze S3 bucket: {self.backblaze_bucket_name}")
            except Exception as e:
                self.logger.error(f"Error whilst connecting to Backblaze S3: {e}")
                self.s3_client = None
        else:
            self.s3_client = None

    def run_data_saver(
        self,
        queue_pool: QueuePoolDataSink | QueuePoolListener,
        dump_path: str,
        file_duration_seconds: int,
        save_to_json: bool,
        save_to_zip: bool,
        send_zip_to_blob: bool
    ):
        file_duration_seconds = self.config['file_duration_seconds']

        for (market, stream_type), queue in queue_pool.queue_lookup.items():
            self.start_stream_writer(
                queue=queue,
                market=market,
                file_duration_seconds=file_duration_seconds,
                dump_path=dump_path,
                stream_type=stream_type,
                save_to_json=save_to_json,
                save_to_zip=save_to_zip,
                send_zip_to_blob=send_zip_to_blob
            )

    def start_stream_writer(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        market: Market,
        file_duration_seconds: int,
        dump_path: str,
        stream_type: StreamType,
        save_to_json: bool,
        save_to_zip: bool,
        send_zip_to_blob: bool
    ) -> None:
        thread = threading.Thread(
            target=self._stream_writer,
            args=(
                queue,
                market,
                file_duration_seconds,
                dump_path,
                stream_type,
                save_to_json,
                save_to_zip,
                send_zip_to_blob
            ),
            name=f'stream_writer: market: {market}, stream_type: {stream_type}'
        )
        thread.start()

    def _stream_writer(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        market: Market,
        file_duration_seconds: int,
        dump_path: str,
        stream_type: StreamType,
        save_to_json: bool,
        save_to_zip: bool,
        send_zip_to_blob: bool
    ):
        while not self.global_shutdown_flag.is_set():
            self._process_stream_data(
                queue,
                market,
                dump_path,
                stream_type,
                save_to_json,
                save_to_zip,
                send_zip_to_blob,
            )
            self._sleep_with_flag_check(self.config['file_duration_seconds'])

        self._process_stream_data(
            queue,
            market,
            dump_path,
            stream_type,
            save_to_json,
            save_to_zip,
            send_zip_to_blob,
        )

        self.logger.info(f"{market} {stream_type}: ended _stream_writer")

    def _sleep_with_flag_check(self, duration: int) -> None:
        interval = 1
        for _ in range(0, duration, interval):
            if self.global_shutdown_flag.is_set():
                break
            time.sleep(interval)

    def _process_stream_data(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        market: Market,
        dump_path: str,
        stream_type: StreamType,
        save_to_json: bool,
        save_to_zip: bool,
        send_zip_to_blob: bool
    ) -> None:
        if not queue.empty():

            stream_data = defaultdict(list)

            while not queue.empty():
                message = queue.get_nowait()

                match = self._stream_message_pair_pattern.search(message)
                pair_found_in_message = match.group(1)

                stream_data[pair_found_in_message].append(message)

            for pair, data in stream_data.items():
                file_name = self.get_file_name(pair, market, stream_type)
                file_path = os.path.join(dump_path, file_name)

                json_content = '[' + ','.join(data) + ']'

                if save_to_json:
                    self.save_to_json(data=json_content, file_path=file_path)

                if save_to_zip:
                    self.save_to_zip(data=json_content, file_name=file_name, file_path=file_path)

                if send_zip_to_blob:
                    self.send_zipped_json_to_cloud_storage(data=json_content, file_name=file_name)

    def save_to_json(self, data, file_path) -> None:
        try:
            with open(f'{file_path}.json', "w") as f:
                f.write(data)
            self.logger.debug(f"Saved to JSON: {file_path}")
        except IOError as e:
            self.logger.error(f"IO Error whilst saving to file {file_path}: {e}")

    def save_to_zip(self, data, file_name, file_path):
        zip_file_path = f"{file_path}.zip"
        try:
            with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
                json_filename = f"{file_name}.json"
                zipf.writestr(json_filename, data)
            self.logger.debug(f"Saved to ZIP: {zip_file_path}")
        except IOError as e:
            self.logger.error(f"IO Error whilst saving to zip: {zip_file_path}: {e}")

    def send_zipped_json_to_cloud_storage(self, data, file_name: str):

        if self.s3_client and self.backblaze_bucket_name:
            self.send_zipped_json_to_backblaze(data, file_name)

        elif self.azure_blob_service_client and self.azure_container_client:
            self.send_zipped_json_to_azure(data, file_name)

        else:
            self.logger.error("No storage client Configured")

    def send_zipped_json_to_azure(self, data, file_name: str):
        try:
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
                json_filename = f"{file_name}.json"
                zipf.writestr(json_filename, data)

            zip_buffer.seek(0)

            blob_client = self.azure_container_client.get_blob_client(blob=f"{file_name}.zip")
            blob_client.upload_blob(zip_buffer, overwrite=True)
            self.logger.debug(f"Successfully sent {file_name}.zip to Azure Blob container: {self.azure_container_name}")
        except Exception as e:
            self.logger.error(f"Error during sending ZIP to Azure Blob: {file_name} {e}")

    def send_zipped_json_to_backblaze(self, data, file_name: str):
        try:
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:

                json_filename = f"{file_name}.json"
                zipf.writestr(json_filename, data)

            zip_buffer.seek(0)

            response = self.s3_client.put_object(
                Bucket=self.backblaze_bucket_name,
                Key=f"{file_name}.zip",
                Body=zip_buffer.getvalue()
            )
            http_status_code = response['ResponseMetadata']['HTTPStatusCode']

            if http_status_code != 200 :
                self.logger.error(f'sth bad with upload response {response}')

            self.logger.debug(f"Successfully sent {file_name}.zip to Backblaze B2 bucket: {self.backblaze_bucket_name}")
        except Exception as e:
            self.logger.error(f"Error whilst uploading ZIP to BackBlaze B2: {file_name} {e}")

    @staticmethod
    def get_file_name(pair: str, market: Market, stream_type: StreamType) -> str:
        formatted_now_timestamp = TimestampsGenerator.get_utc_formatted_timestamp_for_file_name()
        return f"binance_{stream_type.name.lower()}_{market.name.lower()}_{pair.lower()}_{formatted_now_timestamp}"
