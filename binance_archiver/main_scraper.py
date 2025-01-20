import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv

from binance_archiver.scraper import download_csv_data


if __name__ == '__main__':

    # env_path = os.path.join(os.path.expanduser("~"), 'binance-archiver.env')
    # load_dotenv(env_path)
    load_dotenv('binance-archiver.env')

    client = SecretClient(
        vault_url=os.environ.get('VAULT_URL'),
        credential=DefaultAzureCredential()
    )

    backblaze_access_key_id_secret_name = os.environ.get('BACKBLAZE_ACCESS_KEY_ID_BINANCE_TEST')
    backblaze_bucket_name_secret_name = os.environ.get('BACKBLAZE_BUCKET_NAME_BINANCE_TEST')
    backblaze_secret_access_key_secret_name = os.environ.get('BACKBLAZE_SECRET_ACCESS_KEY_BINANCE_TEST')
    backblaze_endpoint_url_secret_name = os.environ.get('BACKBLAZE_ENDPOINT_URL_BINANCE_TEST')

    backblaze_access_key_id = client.get_secret(backblaze_access_key_id_secret_name).value
    backblaze_secret_access_key = client.get_secret(backblaze_secret_access_key_secret_name).value
    backblaze_endpoint_url = client.get_secret(backblaze_endpoint_url_secret_name).value
    backblaze_bucket_name = client.get_secret(backblaze_bucket_name_secret_name).value

    download_csv_data(
        # dump_path='C:/Users/defrg/binance_data_main/',
        start_date='15-01-2025',
        end_date='15-01-2025',
        backblaze_access_key_id=backblaze_access_key_id,
        backblaze_secret_access_key=backblaze_secret_access_key,
        backblaze_endpoint_url=backblaze_endpoint_url,
        backblaze_bucket_name=backblaze_bucket_name,
        pairs=["trxusdt"],
        markets=['SPOT', 'USD_M_FUTURES', 'COIN_M_FUTURES'],
        stream_types=['DIFFERENCE_DEPTH_STREAM', 'TRADE_STREAM']
    )
