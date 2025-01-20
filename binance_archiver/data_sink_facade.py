from __future__ import annotations

import logging
import os
import pprint
import threading

from binance_archiver.commandline_interface import CommandLineInterface
from binance_archiver.stream_data_save_and_sender import StreamDataSaverAndSender
from binance_archiver.exceptions import BadConfigException, BadStorageProviderParameters, WebSocketLifeTimeException
from binance_archiver.fastapi_manager import FastAPIManager
from binance_archiver.logo import binance_archiver_logo
from binance_archiver.queue_pool import QueuePoolDataSink
from binance_archiver.setup_logger import setup_logger
from binance_archiver.snapshot_manager import DataSinkSnapshotStrategy, SnapshotManager
from binance_archiver.stream_service import StreamService

__all__ = [
    'launch_data_sink'
]

def launch_data_sink(
        config,
        azure_blob_parameters_with_key: str | None = None,
        azure_container_name: str | None = None,
        backblaze_access_key_id: str | None = None,
        backblaze_secret_access_key: str | None = None,
        backblaze_endpoint_url: str | None = None,
        backblaze_bucket_name: str | None = None,
        should_dump_logs: bool = False
) -> DataSinkFacade:

    valid_markets = {"spot", "usd_m_futures", "coin_m_futures"}
    instruments = config.get("instruments")

    if not isinstance(instruments, dict) or not (0 < len(instruments) <= 3):
        raise BadConfigException("Config must contain 1 to 3 markets and must be a dictionary.")

    for market, pairs in instruments.items():
        if market not in valid_markets or not isinstance(pairs, list):
            raise BadConfigException(f"Invalid pairs for market {market} or market not handled.")
        if not pairs:
            raise BadConfigException(f"Pairs for market {market} are missing or invalid.")

    send_zip_to_blob = config.get('send_zip_to_blob', False)
    if send_zip_to_blob:
        azure_params_ok = azure_blob_parameters_with_key is not None and azure_container_name is not None
        backblaze_params_ok = (backblaze_access_key_id is not None and
                               backblaze_secret_access_key is not None and
                               backblaze_endpoint_url is not None and
                               backblaze_bucket_name is not None)

        if not azure_params_ok and not backblaze_params_ok:
            raise BadStorageProviderParameters(
                'At least one of the Azure or Backblaze parameter sets must be fully specified.')

    if not (60 <= config.get("websocket_life_time_seconds", 0) <= 60 * 60 * 23):
        raise WebSocketLifeTimeException('Invalid websocket_life_time_seconds')

    logger = setup_logger(should_dump_logs=should_dump_logs)
    logger.info("\n%s", binance_archiver_logo)
    logger.info("Starting Binance Archiver...")
    logger.info("Configuration:\n%s", pprint.pformat(config, indent=1))

    dump_path = config.get("dump_path", "dump/")
    if dump_path.startswith("/"):
        logger.warning("Specified dump_path starts with '/': presumably dump_path is wrong.")

    os.makedirs(dump_path, exist_ok=True)

    backblaze_s3_parameters = None
    if all([backblaze_access_key_id, backblaze_secret_access_key, backblaze_endpoint_url]):
        backblaze_s3_parameters = {
            'access_key_id': backblaze_access_key_id,
            'secret_access_key': backblaze_secret_access_key,
            'endpoint_url': backblaze_endpoint_url
        }

    archiver_facade = DataSinkFacade(
        config=config,
        logger=logger,
        azure_blob_parameters_with_key=azure_blob_parameters_with_key,
        azure_container_name=azure_container_name,
        backblaze_s3_parameters=backblaze_s3_parameters,
        backblaze_bucket_name=backblaze_bucket_name
    )

    archiver_facade.run()

    return archiver_facade


class DataSinkFacade:

    __slots__ = [
        'config',
        'logger',
        'azure_blob_parameters_with_key',
        'azure_container_name',
        'backblaze_s3_parameters',
        'backblaze_bucket_name',
        'instruments',
        'global_shutdown_flag',
        'queue_pool',
        'stream_service',
        'command_line_interface',
        'fast_api_manager',
        'stream_data_saver_and_sender',
        'snapshot_manager'
    ]

    def __init__(
            self,
            config: dict,
            logger: logging.Logger,
            azure_blob_parameters_with_key: str | None = None,
            azure_container_name: str | None = None,
            backblaze_s3_parameters: dict[str, str] | None = None,
            backblaze_bucket_name: str | None = None
    ) -> None:
        self.config = config
        self.logger = logger
        self.azure_blob_parameters_with_key = azure_blob_parameters_with_key
        self.azure_container_name = azure_container_name
        self.backblaze_s3_parameters = backblaze_s3_parameters
        self.backblaze_bucket_name = backblaze_bucket_name
        self.instruments = config["instruments"]
        self.global_shutdown_flag = threading.Event()
        self.queue_pool = QueuePoolDataSink()

        self.stream_service = StreamService(
            config=self.config,
            logger=self.logger,
            queue_pool=self.queue_pool,
            global_shutdown_flag=self.global_shutdown_flag
        )

        self.stream_data_saver_and_sender = StreamDataSaverAndSender(
            logger=self.logger,
            config=self.config,
            azure_blob_parameters_with_key=self.azure_blob_parameters_with_key,
            azure_container_name=self.azure_container_name,
            backblaze_s3_parameters=self.backblaze_s3_parameters,
            backblaze_bucket_name=self.backblaze_bucket_name,
            global_shutdown_flag=self.global_shutdown_flag
        )

        self.command_line_interface = CommandLineInterface(
            config=self.config,
            logger=self.logger,
            stream_service=self.stream_service
        )

        self.fast_api_manager = FastAPIManager()
        self.fast_api_manager.set_callback(self.command_line_interface.handle_command)

        snapshot_strategy = DataSinkSnapshotStrategy(
            data_saver=self.stream_data_saver_and_sender,
            save_to_json=self.config["save_to_json"],
            save_to_zip=self.config["save_to_zip"],
            send_zip_to_blob=self.config["send_zip_to_blob"]
        )

        self.snapshot_manager = SnapshotManager(
            config=self.config,
            logger=self.logger,
            snapshot_strategy=snapshot_strategy,
            global_shutdown_flag=self.global_shutdown_flag
        )

    def run(self) -> None:
        dump_path = self.config.get("dump_path", "dump/")

        self.stream_service.run_streams()

        self.fast_api_manager.run()

        self.stream_data_saver_and_sender.run_data_saver(
            queue_pool=self.queue_pool,
            dump_path=dump_path,
            file_duration_seconds=self.config["file_duration_seconds"],
            save_to_json=self.config["save_to_json"],
            save_to_zip=self.config["save_to_zip"],
            send_zip_to_blob=self.config["send_zip_to_blob"]
        )

        self.snapshot_manager.run_snapshots(dump_path=dump_path)

    def shutdown(self):
        self.logger.info("Shutting down archiver")
        self.global_shutdown_flag.set()

        self.fast_api_manager.shutdown()

        remaining_threads = [
            thread for thread in threading.enumerate()
            if thread is not threading.current_thread() and thread.is_alive()
        ]

        if remaining_threads:
            self.logger.warning(f"Some threads are still alive:")
            for thread in remaining_threads:
                self.logger.warning(f"Thread {thread.name} is still alive {thread.is_alive()}")
        else:
            self.logger.info("All threads have been successfully stopped.")
