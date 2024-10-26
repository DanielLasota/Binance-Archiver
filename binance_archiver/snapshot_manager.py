from __future__ import annotations

import json
import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from queue import Queue

import requests

from binance_archiver.data_saver_sender import DataWriterSender
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.timestamps_generator import TimestampsGenerator
from binance_archiver.url_factory import URLFactory


class SnapshotStrategy(ABC):

    __slots__ = ()

    @abstractmethod
    def handle_snapshot(
        self,
        snapshot: dict,
        pair: str,
        market: Market,
        dump_path: str,
        file_name: str
    ):
        ...


class DataSinkSnapshotStrategy(SnapshotStrategy):

    __slots__ = [
        'data_saver',
        'save_to_json',
        'save_to_zip',
        'send_zip_to_blob'
    ]

    def __init__(
        self,
        data_saver: DataWriterSender,
        save_to_json: bool,
        save_to_zip: bool,
        send_zip_to_blob: bool
    ):
        self.data_saver = data_saver
        self.save_to_json = save_to_json
        self.save_to_zip = save_to_zip
        self.send_zip_to_blob = send_zip_to_blob

    def handle_snapshot(
        self,
        snapshot: dict,
        pair: str,
        market: Market,
        dump_path: str,
        file_name: str
    ):
        file_path = os.path.join(dump_path, file_name)
        if self.save_to_json:
            self.data_saver.save_to_json(snapshot, file_path)
        if self.save_to_zip:
            self.data_saver.save_to_zip(snapshot, file_name, file_path)
        if self.send_zip_to_blob:
            self.data_saver.send_zipped_json_to_blob(snapshot, file_name)


class ListenerSnapshotStrategy(SnapshotStrategy):
    __slots__ = ['global_queue']

    def __init__(self, global_queue: Queue):
        self.global_queue = global_queue

    def handle_snapshot(
        self,
        snapshot: dict,
        pair: str,
        market: Market,
        dump_path: str,
        file_name: str
    ):
        self.global_queue.put(json.dumps(snapshot))


class SnapshotManager:

    __slots__ = [
        'config',
        'instruments',
        'logger',
        'snapshot_strategy',
        'global_shutdown_flag'
    ]

    def __init__(
        self,
        config: dict,
        logger: logging.Logger,
        snapshot_strategy: SnapshotStrategy,
        global_shutdown_flag: threading.Event
    ):
        self.config = config
        self.instruments = config['instruments']
        self.logger = logger
        self.snapshot_strategy = snapshot_strategy
        self.global_shutdown_flag = global_shutdown_flag

    def run_snapshots(
        self,
        dump_path: str
    ):
        for market_str, pairs in self.instruments.items():
            market = Market[market_str.upper()]
            self.start_snapshot_daemon(
                market=market,
                pairs=pairs,
                dump_path=dump_path
            )

    def start_snapshot_daemon(
        self,
        market: Market,
        pairs: list[str],
        dump_path: str
    ):
        thread = threading.Thread(
            target=self._snapshot_daemon,
            args=(
                pairs,
                market,
                dump_path
            ),
            name=f'snapshot_daemon: market: {market}'
        )
        thread.start()

    def _snapshot_daemon(
        self,
        pairs: list[str],
        market: Market,
        dump_path: str
    ) -> None:
        while not self.global_shutdown_flag.is_set():
            for pair in pairs:
                try:
                    snapshot, request_timestamp, receive_timestamp = self._get_snapshot(pair, market)

                    if snapshot is None:
                        continue

                    snapshot["_rq"] = request_timestamp
                    snapshot["_rc"] = receive_timestamp

                    file_name = DataWriterSender.get_file_name(
                        pair=pair,
                        market=market,
                        stream_type=StreamType.DEPTH_SNAPSHOT
                    )

                    self.snapshot_strategy.handle_snapshot(
                        snapshot=snapshot,
                        pair=pair,
                        market=market,
                        dump_path=dump_path,
                        file_name=file_name
                    )

                except Exception as e:
                    self.logger.error(
                        f"Error whilst fetching snapshot: {pair} {market}: {e}"
                    )

            self._sleep_with_flag_check(self.config['snapshot_fetcher_interval_seconds'])

        self.logger.info(f"{market}: snapshot daemon has ended")

    def _sleep_with_flag_check(self, duration: int) -> None:
        interval = 1
        for _ in range(0, duration, interval):
            if self.global_shutdown_flag.is_set():
                break
            time.sleep(interval)

    def _get_snapshot(self, pair: str, market: Market) -> tuple[dict[str, any] | None, int | None, int | None]:
        url = URLFactory.get_snapshot_url(market=market, pair=pair)

        try:
            request_timestamp = TimestampsGenerator.get_utc_timestamp_epoch_milliseconds()
            response = requests.get(url, timeout=5)
            receive_timestamp = TimestampsGenerator.get_utc_timestamp_epoch_milliseconds()
            response.raise_for_status()
            data = response.json()

            return data, request_timestamp, receive_timestamp

        except Exception as e:
            self.logger.error(f"Error whilst fetching snapshot: {e}")

            return None, None, None