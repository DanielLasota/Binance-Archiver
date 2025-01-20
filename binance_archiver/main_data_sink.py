import json
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
import time
import tracemalloc

from binance_archiver import launch_data_sink
from binance_archiver.load_config import load_config


if __name__ == "__main__":

    load_dotenv('binance-archiver.env')
    config = load_config('almost_production_config.json')

    client = SecretClient(
        vault_url=os.environ.get('VAULT_URL'),
        credential=DefaultAzureCredential()
    )

    # config = json.loads(client.get_secret('archer-main-config').value)
    archer_container_name = client.get_secret('archer-main-container-name')
    backblaze_access_key_id = client.get_secret('backblaze-access-key-id-binance-prod').value
    backblaze_secret_access_key = client.get_secret('backblaze-secret-access-key-binance-prod').value
    backblaze_endpoint_url = client.get_secret('backblaze-endpoint-url-binance-prod').value
    backblaze_bucket_name = client.get_secret('backblaze-bucket-name-binance-prod').value

    # tracemalloc.start()

    data_sink = launch_data_sink(
        config=config,
        backblaze_access_key_id=backblaze_access_key_id,
        backblaze_secret_access_key=backblaze_secret_access_key,
        backblaze_endpoint_url=backblaze_endpoint_url,
        backblaze_bucket_name=backblaze_bucket_name,
        should_dump_logs=True
    )

    while not data_sink.global_shutdown_flag.is_set():
        time.sleep(8)

    data_sink.logger.info(f'data_sink.global_shutdown_flag.is_set() {data_sink.global_shutdown_flag.is_set()}')
    data_sink.logger.info('the program has ended, exiting')
