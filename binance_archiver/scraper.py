import io
import os
import zipfile
from datetime import timedelta, datetime

import boto3
import orjson
from alive_progress import alive_bar
import pandas as pd

from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType
from abc import ABC, abstractmethod
from typing import List

from binance_archiver.logo import binance_archiver_logo

__all__ = ['download_data']


BINANCE_ARCHIVER_LOGO = binance_archiver_logo


def download_data(
        start_date: str,
        end_date: str,
        dump_path: str | None = None,
        blob_connection_string: str | None = None,
        container_name: str | None = None,
        backblaze_access_key_id: str | None = None,
        backblaze_secret_access_key: str | None = None,
        backblaze_endpoint_url: str | None = None,
        backblaze_bucket_name: str | None = None,
        pairs: list[str] | None = None,
        markets: list[str] | None = None,
        stream_types: list[str] | None = None,
        should_save_raw_jsons: bool | None = None
        ) -> None:

    data_scraper = DataScraper(
        blob_connection_string=blob_connection_string,
        container_name=container_name,
        backblaze_access_key_id=backblaze_access_key_id,
        backblaze_secret_access_key=backblaze_secret_access_key,
        backblaze_endpoint_url=backblaze_endpoint_url,
        backblaze_bucket_name=backblaze_bucket_name,
        should_save_raw_jsons=should_save_raw_jsons
    )
    data_scraper.run(start_date=start_date, end_date=end_date, pairs=pairs, markets=markets, stream_types=stream_types,
                     dump_path=dump_path)


class DataScraper:

    __slots__ = ['storage_client', 'should_save_raw_jsons']

    def __init__(
            self,
            blob_connection_string: str | None = None,
            container_name: str | None = None,
            backblaze_access_key_id: str | None = None,
            backblaze_secret_access_key: str | None = None,
            backblaze_endpoint_url: str | None = None,
            backblaze_bucket_name: str | None = None,
            should_save_raw_jsons: bool = False
    ) -> None:

        self.should_save_raw_jsons = should_save_raw_jsons

        if blob_connection_string is not None:
            self.storage_client = AzureClient(
                blob_connection_string=blob_connection_string,
                container_name=container_name
            )
        elif backblaze_access_key_id is not None:
            self.storage_client = BackBlazeS3Client(
                access_key_id=backblaze_access_key_id,
                secret_access_key=backblaze_secret_access_key,
                endpoint_url=backblaze_endpoint_url,
                bucket_name=backblaze_bucket_name)
        else:
            raise ValueError('No storage specified...')

    def run(self, start_date: str, end_date: str, markets: list[str], stream_types: list[str], pairs: list[str],
            dump_path: str | None) -> None:

        if dump_path is None:
            dump_path = os.path.join(os.path.expanduser("~"), 'binance_archival_data').replace('\\', '/')

        if not os.path.exists(dump_path):
            os.makedirs(dump_path)

        dates = self._generate_date_range(start_date, end_date)
        markets = [Market[_.upper()] for _ in markets]
        stream_types = [StreamType[_.upper()] for _ in stream_types]
        pairs = [_.lower() for _ in pairs]

        amount_of_files_to_be_made = len(pairs) * len(markets) * len(stream_types) * len(dates)

        print(BINANCE_ARCHIVER_LOGO)

        print('\033[36m')

        print('############')
        print('')
        print(f'ought to download {amount_of_files_to_be_made} files:')
        for date in dates:
            for market in markets:
                for stream_type in stream_types:
                    for pair in pairs:
                        if market == Market.COIN_M_FUTURES:
                            pair = f'{pair}_perp'
                        print(f'downloading: {self._get_save_file_name(pair=pair, market=market, stream_type=stream_type, date=date)}')
        print('')
        print('############')
        print('')

        for date in dates:
            for market in markets:
                for stream_type in stream_types:
                    for pair in pairs:
                        if market == Market.COIN_M_FUTURES:
                            pair = f'{pair}_perp'
                        print(f'downloading pair: {pair} {stream_type} {market} {date}')
                        self._blob_to_csv(date, market, stream_type, pair, dump_path)

    def _blob_to_csv(self, date: str, market: Market, stream_type: StreamType, pair: str, dump_path: str) -> None:

        files_list_to_download = self._get_target_file_list(
            date=date,
            market=market,
            stream_type=stream_type,
            pair=pair
        )

        df = self._process_data(stream_type, files_list_to_download)

        self._df_check_procedure(df=df)

        file_name = self._get_save_file_name(pair=pair, market=market, stream_type=stream_type, date=date)
        df.to_csv(f'{dump_path}/{file_name}', index=False)

    def _process_data(self, stream_type: StreamType, files_list_to_download: list[str]) -> pd.DataFrame:

        processor_lookup = {
            StreamType.DIFFERENCE_DEPTH: self._difference_depth_processor,
            StreamType.TRADE: self._trade_processor,
            StreamType.DEPTH_SNAPSHOT: self._difference_depth_snapshot_processor
        }

        processor = processor_lookup[stream_type]

        return processor(files_list_to_download)

    def _difference_depth_processor(self, files_list_to_download: list[str]) -> pd.DataFrame:

        with alive_bar(len(files_list_to_download), force_tty=True, spinner='dots_waves') as bar:
            records = []

            for file_name in files_list_to_download:
                response = self.storage_client.read_file(file_name=file_name)
                json_dict: list[dict] = self._convert_response_to_json(response)

                for record in json_dict:
                    event_time = record["data"]["E"]
                    first_update = record["data"]["U"]
                    final_update = record["data"]["u"]
                    bids = record["data"]["b"]
                    asks = record["data"]["a"]
                    timestamp_of_receive = record["_E"]

                    for bid in bids:
                        records.append([
                            event_time,
                            0,
                            float(bid[0]),
                            float(bid[1]),
                            timestamp_of_receive,
                            first_update,
                            final_update
                        ])

                    for ask in asks:
                        records.append([
                            event_time,
                            1,
                            float(ask[0]),
                            float(ask[1]),
                            timestamp_of_receive,
                            first_update,
                            final_update
                        ])

                bar()

            columns = [
                "EventTime",
                "IsAsk",
                "Price",
                "Quantity",
                "TimestampOfReceive",
                "FirstUpdate",
                "FinalUpdate"
            ]

        return pd.DataFrame(data=records, columns=columns)

    def _trade_processor(self, files_list_to_download: list[str]) -> pd.DataFrame:
        with alive_bar(len(files_list_to_download), force_tty=True, spinner='dots_waves') as bar:

            records = []

            for file_name in files_list_to_download:
                response = self.storage_client.read_file(file_name=file_name)
                json_dict: list[dict] = self._convert_response_to_json(response)

                for record in json_dict:
                    event_time = record["data"]["E"]
                    trade_id = record["data"]["t"]
                    price = record["data"]["p"]
                    quantity = record["data"]["q"]
                    # seller_order_id = record["data"]["a"] if "a" in record["data"] else None
                    # buyer_order_id = record["data"]["b"] if "b" in record["data"] else None
                    trade_time = record["data"]["T"]
                    is_buyer_market_maker = record["data"]["m"]
                    timestamp_of_receive = record["_E"]

                    records.append(
                        [
                            event_time,
                            trade_id,
                            price,
                            quantity,
                            # seller_order_id,
                            # buyer_order_id,
                            trade_time,
                            int(is_buyer_market_maker),
                            timestamp_of_receive
                        ]
                    )

                bar()

        columns = [
            "EventTime",
            "TradeId",
            "Price",
            "Quantity",
            "TradeTime",
            "IsBuyerMarketMaker",
            "TimestampOfReceive"
        ]

        return pd.DataFrame(data=records, columns=columns)

    def _difference_depth_snapshot_processor(self, files_list_to_download: list[str]) -> pd.DataFrame:
        ...

    def _df_check_procedure(self, df: pd.DataFrame) -> None:
        ...

    def _get_target_file_list(self, date: str, market: Market, stream_type: StreamType, pair: str) -> list[str]:

        prefixes_list = self._get_prefixes_list_for_target_date(
            date=date,
            market=market,
            stream_type=stream_type,
            pair=pair
        )

        return self.storage_client.list_files_with_prefixes(prefixes=prefixes_list)

    def _get_prefixes_list_for_target_date(self, date: str, market: Market, stream_type: StreamType, pair: str) -> list[str]:
        day_date_before_prefix = self._get_file_name_base_prefix(market=market, stream_type=stream_type,
                                                                 pair=pair)
        target_day_date_prefix = self._get_file_name_base_prefix(market=market, stream_type=stream_type,
                                                                 pair=pair)

        day_date_before = (datetime.strptime(date, '%d-%m-%Y') - timedelta(days=1)).strftime('%d-%m-%Y')

        day_date_before_prefix += f'_{day_date_before}T23-5'
        target_day_date_prefix += f'_{date}'

        return [day_date_before_prefix, target_day_date_prefix]

    @staticmethod
    def _convert_response_to_json(storage_response: bytes) -> list[dict] | None:
        try:
            with zipfile.ZipFile(io.BytesIO(storage_response)) as z:
                for file_name in z.namelist():
                    if file_name.endswith('.json'):
                        with z.open(file_name) as json_file:
                            json_bytes = json_file.read()
                            json_content: list[dict] = orjson.loads(json_bytes)
                            return json_content
            print("Nie znaleziono plików .json w archiwum.")
            return None
        except zipfile.BadZipFile:
            print("Nieprawidłowy format pliku ZIP.")
            return None
        except orjson.JSONDecodeError:
            print("Błąd podczas dekodowania JSON.")
            return None
        except Exception as e:
            print(f"Wystąpił nieoczekiwany błąd: {e}")
            return None

    @staticmethod
    def _get_save_file_name(market: Market, stream_type: StreamType, pair: str, date: str) -> str:
        pair_lower = pair.lower()

        market_mapping = {
            Market.SPOT: "spot",
            Market.USD_M_FUTURES: "futures_usd_m",
            Market.COIN_M_FUTURES: "futures_coin_m",
        }

        data_type_mapping = {
            StreamType.DIFFERENCE_DEPTH: "binance_difference_depth",
            StreamType.DEPTH_SNAPSHOT: "binance_snapshot",
            StreamType.TRADE: "binance_trade",
        }

        market_short_name = market_mapping.get(market, "unknown_market")
        prefix = data_type_mapping.get(stream_type, "unknown_data_type")

        return f"{prefix}_{market_short_name}_{pair_lower}_{date}.csv"

    @staticmethod
    def _get_file_name_base_prefix(pair: str, market: Market, stream_type: StreamType) -> str:
        market_mapping = {
            Market.SPOT: 'spot',
            Market.USD_M_FUTURES: 'futures_usd_m',
            Market.COIN_M_FUTURES: 'futures_coin_m'
        }

        data_type_mapping = {
            StreamType.DIFFERENCE_DEPTH: 'binance_difference_depth',
            StreamType.DEPTH_SNAPSHOT: 'binance_snapshot',
            StreamType.TRADE: 'binance_trade'
        }

        market_short_name = market_mapping.get(market, 'unknown_market')
        prefix = data_type_mapping.get(stream_type, 'unknown_data_type')

        return f'{prefix}_{market_short_name}_{pair}'

    @staticmethod
    def _generate_date_range(start_date_str: str, end_date_str: str) -> list[str]:
        date_format = "%d-%m-%Y"

        try:
            start_date = datetime.strptime(start_date_str, date_format)
            end_date = datetime.strptime(end_date_str, date_format)
        except ValueError as ve:
            raise ValueError(f"invalid date format{ve}")

        if start_date > end_date:
            raise ValueError("start date > end_date")

        date_list = []

        delta = end_date - start_date

        for i in range(delta.days + 1):
            current_date = start_date + timedelta(days=i)
            date_str = current_date.strftime(date_format)
            date_list.append(date_str)

        return date_list


class DataFrameChecker:
    def __init__(self):
        ...

    def check_trade_dataframe_procedure(self, df: pd.DataFrame) -> None:
        ...

    def check_difference_depth_procedure(self, df: pd.DataFrame) -> None:
        ...


class IClientHandler(ABC):
    @abstractmethod
    def list_files_with_prefixes(self, prefixes: List[str]) -> List[str]:
        pass

    @abstractmethod
    def read_file(self, file_name) -> bytes:
        pass


class BackBlazeS3Client(IClientHandler):

    __slots__ = ['_bucket_name', 's3_client']

    def __init__(
            self,
            access_key_id: str,
            secret_access_key: str,
            endpoint_url: str,
            bucket_name: str
    ) -> None:

        self._bucket_name = bucket_name

        self.s3_client = boto3.client(
            service_name='s3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            endpoint_url=endpoint_url
        )

    def list_files_with_prefixes(self, prefixes: str | list[str]) -> list[str]:

        if isinstance(prefixes, str):
            prefixes = [prefixes]

        all_files = []
        for prefix in prefixes:
            try:
                paginator = self.s3_client.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(Bucket=self._bucket_name, Prefix=prefix)

                files = []
                for page in page_iterator:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            files.append(obj['Key'])

                if files:
                    # print(f"Found {len(files)} files with '{prefix}' prefix in '{self._bucket_name}' bucket")
                    all_files.extend(files)
                else:
                    print(f"No files with '{prefix}' prefix in '{self._bucket_name}' bucket")

            except Exception as e:
                print(f"Error whilst listing '{prefix}': {e}")

        return all_files

    def read_file(self, file_name) -> bytes:
        response = self.s3_client.get_object(Bucket=self._bucket_name, Key=file_name)
        return response['Body'].read()


class AzureClient(IClientHandler):

    __slots__ = []

    def __init__(
            self,
            blob_connection_string: str,
            container_name: str
    ) -> None:
        ...

    def list_files_with_prefixes(self, prefixes: str | list[str]) -> list[str]:
        ...

    def read_file(self, file_name) -> None:
        ...
