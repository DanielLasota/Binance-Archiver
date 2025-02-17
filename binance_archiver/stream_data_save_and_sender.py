from __future__ import annotations

import io
import logging
import os
import threading
import time
import zipfile
from collections import defaultdict
import re

from binance_archiver import DataSinkConfig
from binance_archiver.enum_.asset_parameters import AssetParameters
from binance_archiver.difference_depth_queue import DifferenceDepthQueue
from binance_archiver.enum_.data_save_target_enum import DataSaveTarget
from binance_archiver.exceptions import BadStorageConnectionParameters
from binance_archiver.queue_pool import ListenerQueuePool, DataSinkQueuePool
from binance_archiver.enum_.storage_connection_parameters import StorageConnectionParameters
from binance_archiver.s3_client import OwnLightweightS3Client
from binance_archiver.timestamps_generator import TimestampsGenerator
from binance_archiver.trade_queue import TradeQueue


class StreamDataSaverAndSender:

    __slots__ = [
        'logger',
        'queue_pool',
        'data_sink_config',
        'global_shutdown_flag',
        '_stream_message_pair_pattern',
        'cloud_storage_client'
    ]

    def __init__(
        self,
        queue_pool: DataSinkQueuePool | ListenerQueuePool,
        data_sink_config: DataSinkConfig,
        global_shutdown_flag: threading.Event = threading.Event()
    ):
        self.logger = logging.getLogger('binance_data_sink')
        self.queue_pool = queue_pool
        self.data_sink_config = data_sink_config
        self.global_shutdown_flag = global_shutdown_flag
        self._stream_message_pair_pattern = re.compile(r'"stream":"(\w+)@')
        self.cloud_storage_client = None

    def run(self):

        if self.data_sink_config.data_save_target in [DataSaveTarget.BACKBLAZE, DataSaveTarget.AZURE_BLOB]:
            self._setup_cloud_storage_client()
            self._start_zip_reserve_sender_loop(retry_interval_seconds=60*60)

        for (market, stream_type), queue in self.queue_pool.queue_lookup.items():

            asset_parameters = AssetParameters(
                market=market,
                stream_type=stream_type,
                pairs=[]
            )

            self.start_stream_writer(
                queue=queue,
                asset_parameters=asset_parameters
            )

    def _start_zip_reserve_sender_loop(self, retry_interval_seconds) -> None:
        thread = threading.Thread(
            target=self._failed_zip_reserve_sender_loop,
            args=[retry_interval_seconds],
            name=f'failed_zip_reserve_sender_loop'
        )
        thread.start()

    def _failed_zip_reserve_sender_loop(self, retry_interval_seconds: int):
        while not self.global_shutdown_flag.is_set():
            for _ in range(retry_interval_seconds):
                if self.global_shutdown_flag.is_set():
                    break
                time.sleep(1)

            zip_files = [f for f in os.listdir(self.data_sink_config.file_save_catalog) if f.endswith(".zip")]

            for zip_file_filename in zip_files:
                try:
                    zip_file_path = f'{self.data_sink_config.file_save_catalog}/{zip_file_filename}'
                    self.send_existing_file_to_backblaze_bucket(file_path=zip_file_path)
                    os.remove(zip_file_path)
                except Exception as e:
                    self.logger.error(f"_failed_zip_reserve_sender_loop Error: "
                                      f"{zip_file_filename}: {e} gonna try in another again soon")

    def _setup_cloud_storage_client(self):

        azure_params_ok = (
                self.data_sink_config.storage_connection_parameters.azure_blob_parameters_with_key is not None
                and self.data_sink_config.storage_connection_parameters.azure_container_name is not None
        )

        backblaze_params_ok = (
                self.data_sink_config.storage_connection_parameters.backblaze_access_key_id is not None
                and self.data_sink_config.storage_connection_parameters.backblaze_secret_access_key is not None
                and self.data_sink_config.storage_connection_parameters.backblaze_endpoint_url is not None
                and self.data_sink_config.storage_connection_parameters.backblaze_bucket_name is not None
        )

        if not azure_params_ok and not backblaze_params_ok:
            raise BadStorageConnectionParameters(
                "At least one set of storage parameters (Azure or Backblaze) "
                "must be fully specified. Also whilst creating from os env using:"
                "load_storage_connection_parameters_from_environ() "
                "Check os env variables"
            )

        target_initializers = {
            DataSaveTarget.BACKBLAZE: lambda: self._get_own_lightweight_s3_client(
                self.data_sink_config.storage_connection_parameters),
            DataSaveTarget.AZURE_BLOB: lambda: self._get_azure_container_client(
                self.data_sink_config.storage_connection_parameters)
        }

        initializer = target_initializers.get(self.data_sink_config.data_save_target)

        if initializer:
            self.cloud_storage_client = initializer()

    @staticmethod
    def _get_azure_container_client(storage_connection_parameters):
        try:
            azure_blob_service_client = BlobServiceClient.from_connection_string(
                storage_connection_parameters.azure_blob_parameters_with_key
            )

            azure_container_client = azure_blob_service_client.get_container_client(
                storage_connection_parameters.azure_container_name
            )
            return azure_container_client
        except Exception as e:
            print(f"Could not connect to Azure: {e}")

    @staticmethod
    def _get_own_lightweight_s3_client(storage_connection_parameters: StorageConnectionParameters):
        try:
            return OwnLightweightS3Client(storage_connection_parameters=storage_connection_parameters)
        except Exception as e:
            print(f"Error whilst connecting to Backblaze S3: {e}")

    def start_stream_writer(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        asset_parameters: AssetParameters,
    ) -> None:
        thread = threading.Thread(
            target=self._write_stream_to_target,
            args=(queue, asset_parameters),
            name=f'stream_writer: market: {asset_parameters.market}, stream_type: {asset_parameters.stream_type}'
        )
        thread.start()

    def _write_stream_to_target(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        asset_parameters: AssetParameters
    ) -> None:
        while not self.global_shutdown_flag.is_set():
            self._process_queue_data(
                queue,
                asset_parameters
            )
            self._sleep_with_flag_check(self.data_sink_config.time_settings.file_duration_seconds)

        self._process_queue_data(
            queue,
            asset_parameters
        )

        self.logger.info(f"{asset_parameters.market} {asset_parameters.stream_type}: ended _stream_writer")

    def _sleep_with_flag_check(self, duration: int) -> None:
        interval = 1
        for _ in range(0, duration, interval):
            if self.global_shutdown_flag.is_set():
                break
            time.sleep(interval)

    def _process_queue_data(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        asset_parameters: AssetParameters
    ) -> None:
        if not queue.empty():

            stream_data = defaultdict(list)

            while not queue.empty():
                message = queue.get_nowait()
                match = self._stream_message_pair_pattern.search(message)
                pair_found_in_message = match.group(1)
                stream_data[pair_found_in_message].append(message)

            for pair, data in stream_data.items():

                file_name = self.get_file_name(
                    asset_parameters=asset_parameters.get_asset_parameter_with_specified_pair(pair=pair)
                )

                self.save_data(
                    json_content='[' + ','.join(data) + ']',
                    file_save_catalog=self.data_sink_config.file_save_catalog,
                    file_name=file_name
                )

    def save_data(
            self,
            json_content: str,
            file_save_catalog: str,
            file_name: str
    ) -> None:

        data_savers = {
            DataSaveTarget.JSON:
                lambda: self.write_data_to_json_file(json_content=json_content, file_save_catalog=file_save_catalog, file_name=file_name),
            DataSaveTarget.ZIP:
                lambda: self.write_data_to_zip_file(json_content=json_content, file_save_catalog=file_save_catalog, file_name=file_name),
            DataSaveTarget.AZURE_BLOB:
                lambda: self.send_zipped_json_to_specified_cloud(json_content=json_content, file_save_catalog=file_save_catalog, file_name=file_name),
            DataSaveTarget.BACKBLAZE:
                lambda: self.send_zipped_json_to_specified_cloud(json_content=json_content, file_save_catalog=file_save_catalog, file_name=file_name)
        }

        saver = data_savers.get(self.data_sink_config.data_save_target)
        if saver:
            saver()

    def write_data_to_json_file(self, json_content, file_save_catalog, file_name) -> None:
        file_save_path = f'{file_save_catalog}/{file_name}.json'

        try:
            with open(file_save_path, "w") as f:
                f.write(json_content)
            self.logger.debug(f"Saved to JSON: {file_save_path}")
        except IOError as e:
            self.logger.error(f"IO Error whilst saving to file {file_save_path}: {e}")

    def write_data_to_zip_file(self, json_content: str, file_save_catalog: str, file_name: str) -> None:
        file_save_path = f'{file_save_catalog}/{file_name}.zip'

        try:
            with zipfile.ZipFile(file_save_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
                zipf.writestr(f"{file_name}.json", json_content)
            self.logger.debug(f"Saved to ZIP: {file_save_path}")
        except IOError as e:
            self.logger.error(f"IO Error whilst saving to zip: {file_save_path}: {e}")

    def send_zipped_json_to_specified_cloud(self, json_content: str, file_save_catalog: str, file_name: str) -> None:

        cloud_data_savers = {
            DataSaveTarget.AZURE_BLOB:
                lambda: self.send_zipped_json_to_azure_container(json_content=json_content, file_name=file_name),
            DataSaveTarget.BACKBLAZE:
                lambda: self.send_zipped_json_to_backblaze_bucket(json_content=json_content, file_name=file_name)
        }

        saver = cloud_data_savers.get(self.data_sink_config.data_save_target)

        if saver:
            try:
                saver()

            except Exception as e:
                self.logger.error(
                    f'error sending to backblaze:{e} '
                    f'gonna send on reserve target'
                )

                self.write_data_to_zip_file(
                    json_content=json_content,
                    file_name=file_name,
                    file_save_catalog=file_save_catalog
                )

    def send_zipped_json_to_azure_container(self, json_content: str, file_name: str) -> None:

        try:
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
                json_filename = f"{file_name}.json"
                zipf.writestr(json_filename, json_content)

            zip_buffer.seek(0)

            blob_client = self.cloud_storage_client.get_blob_client(blob=f"{file_name}.zip")
            blob_client.upload_blob(zip_buffer, overwrite=True)
            self.logger.debug(f"Successfully sent {file_name}.zip to Azure Blob container")
        except Exception as e:
            self.logger.error(f"Error during sending ZIP to Azure Blob: {file_name} {e}")

    def send_zipped_json_to_backblaze_bucket(self, json_content: str, file_name: str) -> None:

        self.cloud_storage_client.upload_zipped_jsoned_string(
            data=json_content,
            file_name=file_name
        )

        self.logger.debug(f"Successfully sent {file_name}.zip to Backblaze B2 bucket: "
                          f"{self.data_sink_config.storage_connection_parameters.backblaze_bucket_name}")

    def send_existing_file_to_backblaze_bucket(self, file_path: str) -> None:
        self.logger.info(f'Invocation of send_existing_file_to_backblaze_bucket, {file_path}')
        self.cloud_storage_client.upload_existing_file(file_path=file_path)

    @staticmethod
    def get_file_name(asset_parameters: AssetParameters) -> str:

        if len(asset_parameters.pairs) != 1:
            raise Exception(f"asset_parameters.pairs should've been a string")

        formatted_now_timestamp = TimestampsGenerator.get_utc_formatted_timestamp_for_file_name()
        return (
            f"binance"
            f"_{asset_parameters.stream_type.name.lower()}"
            f"_{asset_parameters.market.name.lower()}"
            f"_{asset_parameters.pairs[0].lower()}"
            f"_{formatted_now_timestamp}"
        )
