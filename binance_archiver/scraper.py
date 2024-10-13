from __future__ import annotations
import io
import os
import zipfile
from dataclasses import dataclass
from datetime import timedelta, datetime

import boto3
import numpy as np
import orjson
from alive_progress import alive_bar
import pandas as pd

from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType
from abc import ABC, abstractmethod
from typing import List

from binance_archiver.logo import binance_archiver_logo


BINANCE_ARCHIVER_LOGO = binance_archiver_logo


__all__ = [
    'download_data',
    'conduct_whole_directory_of_csvs_data_quality_analysis',
    'conduct_csv_files_data_quality_analysis'
]


@dataclass(slots=True)
class AssetParameters:
    market: Market
    stream_type: StreamType
    pair: str


@dataclass(slots=True)
class StorageConnectionParameters:
    blob_connection_string: str | None = None
    container_name: str | None = None
    backblaze_access_key_id: str | None = None
    backblaze_secret_access_key: str | None = None
    backblaze_endpoint_url: str | None = None
    backblaze_bucket_name: str | None = None


def download_data(
        start_date: str,
        end_date: str,
        dump_path: str | None = None,
        blob_connection_string: str | None = None,
        azure_container_name: str | None = None,
        backblaze_access_key_id: str | None = None,
        backblaze_secret_access_key: str | None = None,
        backblaze_endpoint_url: str | None = None,
        backblaze_bucket_name: str | None = None,
        pairs: list[str] | None = None,
        markets: list[str] | None = None,
        stream_types: list[str] | None = None
        ) -> None:

    storage_connection_parameters = StorageConnectionParameters(
    blob_connection_string,
    azure_container_name,
    backblaze_access_key_id,
    backblaze_secret_access_key,
    backblaze_endpoint_url,
    backblaze_bucket_name
    )

    data_scraper = DataScraper(storage_connection_parameters)

    data_scraper.run(
        markets=markets,
        stream_types=stream_types,
        pairs=pairs,
        start_date=start_date,
        end_date=end_date,
        dump_path=dump_path
    )


class DataScraper:

    __slots__ = ['storage_client']

    def __init__(
            self,
            storage_connection_parameters
    ) -> None:

        if storage_connection_parameters.blob_connection_string is not None:
            self.storage_client = AzureClient(
                blob_connection_string=storage_connection_parameters.blob_connection_string,
                container_name=storage_connection_parameters.container_name
            )
        elif storage_connection_parameters.backblaze_access_key_id is not None:
            self.storage_client = BackBlazeS3Client(
                access_key_id=storage_connection_parameters.backblaze_access_key_id,
                secret_access_key=storage_connection_parameters.backblaze_secret_access_key,
                endpoint_url=storage_connection_parameters.backblaze_endpoint_url,
                bucket_name=storage_connection_parameters.backblaze_bucket_name
            )
        else:
            raise ValueError('No storage specified...')

    def run(
            self,
            markets: list[str],
            stream_types: list[str],
            pairs: list[str],
            start_date: str,
            end_date: str,
            dump_path: str | None
    ) -> None:

        print(BINANCE_ARCHIVER_LOGO)

        if dump_path is None:
            dump_path = os.path.join(os.path.expanduser("~"), 'binance_archival_data').replace('\\', '/')

        if not os.path.exists(dump_path):
            os.makedirs(dump_path)

        markets = [Market[_.upper()] for _ in markets]
        stream_types = [StreamType[_.upper()] for _ in stream_types]
        pairs = [_.lower() for _ in pairs]
        dates = self._generate_dates_from_range(start_date, end_date)

        asset_parameters_list = [
            AssetParameters(
                market=market,
                stream_type=stream_type,
                pair=(f'{pair[:-1]}_perp' if market == Market.COIN_M_FUTURES else pair)
            )
            for market in markets
            for stream_type in stream_types
            for pair in pairs
        ]
        amount_of_files_to_be_made = len(asset_parameters_list) * len(dates)

        print('\033[36m')
        print(f'ought to download {amount_of_files_to_be_made} files:')
        print('')

        for date in dates:
            for asset_parameters in asset_parameters_list:
                print(
                    f'Downloading pair: '
                    f'{asset_parameters.pair} '
                    f'{asset_parameters.stream_type} '
                    f'{asset_parameters.market} '
                    f'{date}'
                )
                self._download_as_csv(asset_parameters, date, dump_path)

    def _download_as_csv(self, asset_parameters: AssetParameters, date: str, dump_path: str) -> None:

        list_of_prefixes_that_should_be_downloaded = self._get_list_of_prefixes_that_should_be_downloaded(
            asset_parameters=asset_parameters,
            date=date
        )

        files_list_to_download = self.storage_client.list_files_with_prefixes(list_of_prefixes_that_should_be_downloaded)

        stream_type_handler = self._get_stream_type_handler(asset_parameters.stream_type)

        dataframe = stream_type_handler(files_list_to_download)

        file_name = self._get_file_name(asset_parameters=asset_parameters, date=date)
        dataframe.to_csv(f'{dump_path}/{file_name}.csv', index=False)

    def _get_stream_type_handler(self, stream_type: StreamType) -> callable:
        handler_lookup = {
            StreamType.DIFFERENCE_DEPTH_STREAM: self._difference_depth_stream_type_handler,
            StreamType.TRADE_STREAM: self._trade_stream_type_handler,
            StreamType.DEPTH_SNAPSHOT: self._difference_depth_snapshot_stream_type_handler
        }

        return handler_lookup[stream_type]

    def _difference_depth_stream_type_handler(self, files_list_to_download: list[str]) -> pd.DataFrame:

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

    def _trade_stream_type_handler(self, files_list_to_download: list[str]) -> pd.DataFrame:
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

    def _difference_depth_snapshot_stream_type_handler(self, files_list_to_download: list[str]) -> pd.DataFrame:
        ...

    @staticmethod
    def _generate_dates_from_range(start_date_str: str, end_date_str: str) -> list[str]:
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

    @staticmethod
    def _get_list_of_prefixes_that_should_be_downloaded(asset_parameters: AssetParameters, date: str) -> list[str]:

        date_before = (datetime.strptime(date, '%d-%m-%Y') - timedelta(days=1)).strftime('%d-%m-%Y')

        day_date_before_prefix = DataScraper._get_file_name(asset_parameters=asset_parameters, date=date_before) + 'T23-5'
        target_day_date_prefix = DataScraper._get_file_name(asset_parameters=asset_parameters, date=date)

        prefixes_list = [day_date_before_prefix, target_day_date_prefix]

        return prefixes_list

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
            print("file .json not found in archive")
        except zipfile.BadZipFile:
            print("bad zip file")
            return None
        except orjson.JSONDecodeError:
            print("error during json decode")
            return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None

    @staticmethod
    def _get_file_name(asset_parameters: AssetParameters, date: str) -> str:

        market_mapping = {
            Market.SPOT: "spot",
            Market.USD_M_FUTURES: "futures_usd_m",
            Market.COIN_M_FUTURES: "futures_coin_m",
        }

        data_type_mapping = {
            StreamType.DIFFERENCE_DEPTH_STREAM: "binance_difference_depth",
            StreamType.DEPTH_SNAPSHOT: "binance_snapshot",
            StreamType.TRADE_STREAM: "binance_trade",
        }

        market_short_name = market_mapping.get(asset_parameters.market, "unknown_market")
        file_name_prefix = data_type_mapping.get(asset_parameters.stream_type, "unknown_data_type")

        return f"{file_name_prefix}_{market_short_name}_{asset_parameters.pair}_{date}"


class IClientHandler(ABC):
    __slots__ = ()

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
    __slots__ = ()

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


def conduct_whole_directory_of_csvs_data_quality_analysis(csv_nest_directory: str) -> None:
    data_checker = DataChecker()
    data_checker.conduct_whole_directory_of_csvs_data_quality_analysis(csv_nest_directory)


def conduct_csv_files_data_quality_analysis(csv_paths: list[str]) -> None:
    data_checker = DataChecker()
    data_checker.conduct_csv_files_data_quality_analysis(csv_paths)


class DataChecker:
    __slots__ = ()

    def __init__(self):
        ...

    def conduct_whole_directory_of_csvs_data_quality_analysis(self, csv_nest_directory: str) -> None:
        ...

    def conduct_csv_files_data_quality_analysis(self, csv_paths: list[str]):
        for csv_path in csv_paths:
            csv_name = csv_path.split('/')[-1]
            asset_parameters = self._decode_asset_parameters_from_csv_name(csv_name)
            dataframe = pd.read_csv(csv_path)
            self.conduct_dataframe_quality_analysis(dataframe=dataframe, asset_parameters=asset_parameters)

    @staticmethod
    def _decode_asset_parameters_from_csv_name(csv_name: str) -> AssetParameters:
        _csv_name = csv_name.replace('.csv', '')

        market_mapping = {
            'spot': Market.SPOT,
            'futures_usd_m': Market.USD_M_FUTURES,
            'futures_coin_m': Market.COIN_M_FUTURES,
        }

        stream_type_mapping = {
            'difference_depth': StreamType.DIFFERENCE_DEPTH_STREAM,
            'trade': StreamType.TRADE_STREAM,
            'binance_snapshot': StreamType.DEPTH_SNAPSHOT,
        }

        market = next((value for key, value in market_mapping.items() if key in _csv_name), None)
        if market is None:
            raise ValueError(f"Unknown market in CSV name: {_csv_name}")

        stream_type = next((value for key, value in stream_type_mapping.items() if key in _csv_name), None)
        if stream_type is None:
            raise ValueError(f"Unknown stream type in CSV name: {_csv_name}")

        pair = _csv_name.split('_')[-2]

        return AssetParameters(
            market=market,
            stream_type=stream_type,
            pair=pair
        )

    def conduct_dataframe_quality_analysis(self, dataframe: pd.DataFrame, asset_parameters: AssetParameters) -> None:
        stream_type_handlers = {
            StreamType.DIFFERENCE_DEPTH_STREAM : self._analyse_difference_depth_dataframe,
            StreamType.TRADE_STREAM : self._analyse_trade_dataframe,
            StreamType.DEPTH_SNAPSHOT : self._analyse_difference_depth_snapshot
        }
        handler = stream_type_handlers.get(asset_parameters.stream_type)
        handler(dataframe)

    @staticmethod
    def _analyse_difference_depth_dataframe(df: pd.DataFrame) -> None:
        print(df)

    def _analyse_trade_dataframe(self, df: pd.DataFrame) -> None:
        ...

    def _analyse_difference_depth_snapshot(self, df: pd.DataFrame) -> None:
        ...


class IndividualColumnChecker:
    __slots__ = ()

    @staticmethod
    def is_there_only_one_unique_value_in_series(series: pd.Series) -> bool:
        return len(series.unique()) == 1

    @staticmethod
    def is_whole_series_made_of_only_one_expected_value(series: pd.Series, expected_value: any) -> bool:
        return series.unique()[0] == expected_value and len(series.unique()) == 1

    @staticmethod
    def is_each_series_entry_greater_or_equal_to_previous_one(series: pd.Series) -> bool:
        return series.diff().min() == 0

'''
    # DIFFERENCE DEPTH CHECK

    ::["stream"]:
        is unique value len 1?
        is unique value == f'{asset}@depth@100ms'?

    ::["data"]["e"] event type
        is unique value len 1?
        does unique val == 'depth update'

    ::["data"]["E"] - Event Time
        czy kazdy następujący po sobie Event Time jest >= od poprzedniego
        czy kazdy następujący po sobie Event Time jest wiekszy od poprzedniego o 100 +- 1 ms
        Czy każdy z wpisów jest prawidlowym epochem (?)
        Czy każdy z wpisów jest z 1 dnia od T00:00:00.000Z do T23:59:59.000Z

    ::["data"]["T"] - transaction time, !!! FUTURES COIN M AND FUTURES USD M ONLY !!!
        is each transaction_time greater than previous entry?
        is each transaction_time smaller or equal to current's entry event_time?

    ::["data"]["s"] - symbol of an instrument
        is unique value len 1?
        is unique value presumed symbol

    ::["data"]["U"] - first update id
        is each 'first_update_id' bigger than 'first_update_id' from previous entry
        is each 'first_update_id' bigger by 1 than previous entry 'last_update_id'
        is each first_update_id smaller or equal than same entry 'last_update_id'

    ::["data"]["u"] - last update id
        is each 'last_update_id' bigger than 'last_update_id' from previous entry
        is each 'first_update_id' bigger by 1 than previous entry 'last_update_id'
        is each first_update_id greater or equal than same entry 'last_update_id'
        
    ::["data"]["pu"] - final update in last event, FUTURES_COIN_M and FUTURES_USD_M only
        is each current entry final update bigger than that one from previous entry
        is each current entry final update in last event equal to that one from previous entry
        
    ::["data"]["ps"] - pair, FUTURES_COIN_M only
        is unique value len 1?
        is unique value presumed symbol
    
'''

'''
    # TRADES CHECK

    ##################
    ### SPOT Trades Need 10 field to be checked with (+"M" | -"X") fields
    ###
    ### FUTURES USD M and FUTURES COIN M Trades Need 10 field to be checked with (+"X" | -"M") fields
    ##################

    ::["stream"]:
        is unique value len 1?
        is unique value == f'{asset}@depth@100ms'?

    ::["data"]["e"] event type
        is unique value len 1?
        does unique val == 'depth update'

    ::["data"]["E"] - Event Time
        czy kazdy następujący po sobie Event Time jest >= od poprzedniego
        Czy każdy z wpisów jest prawidlowym epochem (?)
        Czy każdy z wpisów jest z 1 dnia od T00:00:00.000Z do T23:59:59.000Z

    ::["data"]["s"] - symbol of an instrument
        is unique value len 1?
        is unique value presumed symbol

    ::["data"]["t"] - trade id
        czy kazdy następujący po sobie trade id jest > od poprzedniego o 1

    ::["data"]["p"] - price
        czy kazda cena jest type float
        max roznica pomiedzy tickami

    ::["data"]["q"] - quantity
        czy kazde quantity jest type float

    ::["data"]["T"] - Trade Time
        czy kazdy następujący po sobie Event Time jest >= od poprzedniego
        Czy każdy z wpisów jest prawidlowym epochem (?)
        Czy każdy z wpisów jest z 1 dnia od T00:00:00.000Z do T23:59:59.000Z
        max roznica pomiedzy trade time a event time 'E' a 'T'
        min roznica pomiedzy trate time a event time E a T i podnies alarm gdy < 0

    ::["data"]["m"] - buyer indicator
        is unique value len 2?

    ::["data"]["M"] - unknown (SPOT ONLY)
        poinformuj tylko wtedy, gdy unique value len == 2 i podaj linie gdzie tak sie stalo
        podnies alarm, jesli M jest w futures coin m

    ::["data"]["X"] - unknown (FUTURES USD M and FUTURES COIN M ONLY)
        poinformuj tylko wtedy, gdy 'X' != 'MARKET' w jakimkolwiek miejscu w pliku
        podnies alarm, jesli X jest w futures usd m

                2024-06-12T12:47:40.808Z  >=2% price move in one tick, buggy data?? 
                {
                  msg: {
                    e: 'trade',
                    E: 1718196460656,
                    T: 1718196460656,
                    s: 'BCHUSDT',
                    t: 794874338,
                    p: '510.20',
                    q: '0.016',
                    X: 'INSURANCE_FUND',
                    m: true
                  },
                  prev_msg: {
                    e: 'trade',
                    E: 1718196460280,
                    T: 1718196460280,
                    s: 'BCHUSDT',
                    t: 794874337,
                    p: '462.98',
                    q: '0.191',
                    X: 'MARKET',
                    m: false
                  }
                }

'''
