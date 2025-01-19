from __future__ import annotations

import logging
import pprint

from binance_archiver.logo import binance_archiver_logo
from binance_archiver.setup_logger import setup_logger
from binance_archiver.stream_service import StreamService

__all__ = [
    'launch_data_sink'
]

def launch_data_sink(
        config,
        should_dump_logs: bool = False
) -> DataSinkFacade:

    logger = setup_logger(should_dump_logs=should_dump_logs)
    logger.info("\n%s", binance_archiver_logo)
    logger.info("Starting Binance Archiver...")
    logger.info("Configuration:\n%s", pprint.pformat(config, indent=1))

    archiver_facade = DataSinkFacade(
        config=config,
        logger=logger,
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
    ) -> None:
        self.config = config
        self.logger = logger

        self.stream_service = StreamService(
            config=self.config,
            logger=self.logger,
        )

    def run(self) -> None:

        self.stream_service.run_streams()
