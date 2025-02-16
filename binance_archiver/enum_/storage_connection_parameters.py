import os
from dataclasses import dataclass


@dataclass(slots=True)
class StorageConnectionParameters:
    azure_blob_parameters_with_key: str | None = None
    azure_container_name: str | None = None
    backblaze_access_key_id: str | None = None
    backblaze_secret_access_key: str | None = None
    backblaze_endpoint_url: str | None = None
    backblaze_bucket_name: str | None = None


def load_storage_connection_parameters_from_environ() -> StorageConnectionParameters:

    return StorageConnectionParameters(
        azure_blob_parameters_with_key=os.environ.get('AZURE_BLOB_PARAMETERS_WITH_KEY'),
        azure_container_name=os.environ.get('AZURE_CONTAINER_NAME'),
        backblaze_access_key_id=os.environ.get('BACKBLAZE_ACCESS_KEY_ID'),
        backblaze_secret_access_key=os.environ.get('BACKBLAZE_SECRET_ACCESS_KEY'),
        backblaze_endpoint_url=os.environ.get('BACKBLAZE_ENDPOINT_URL'),
        backblaze_bucket_name=os.environ.get('BACKBLAZE_BUCKET_NAME')
    )
