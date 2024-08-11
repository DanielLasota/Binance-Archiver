import json
import logging
import os
import pprint
import time
import zipfile
from datetime import datetime, timezone
from typing import List, Any, Dict, Tuple
from collections import defaultdict
from azure.storage.blob import BlobServiceClient
import io
import threading

from .setup_logger import setup_logger
from .stream_listener import StreamListener
from .difference_depth_queue import DifferenceDepthQueue
from .market_enum import Market
import requests
from binance_archiver.logo import logo

from .stream_type_enum import StreamType
from .trade_queue import TradeQueue
from .url_factory import URLFactory


class ArchiverDaemon:
    def __init__(
        self,
        logger: logging.Logger,
        azure_blob_parameters_with_key: str,
        container_name: str,
    ) -> None:
        self.logger = logger
        self.global_shutdown_flag: bool = False
        self.blob_service_client = BlobServiceClient.from_connection_string(
            azure_blob_parameters_with_key
        )
        self.container_name = container_name

        self.spot_orderbook_stream_message_queue = DifferenceDepthQueue()
        self.spot_transaction_stream_message_queue = TradeQueue()

        self.usd_m_futures_orderbook_stream_message_queue = DifferenceDepthQueue()
        self.usd_m_futures_transaction_stream_message_queue = TradeQueue()

        self.coin_m_orderbook_stream_message_queue = DifferenceDepthQueue()
        self.coin_m_transaction_stream_message_queue = TradeQueue()

        self.queue_lookup = {
            (Market.SPOT, StreamType.DIFFERENCE_DEPTH): self.spot_orderbook_stream_message_queue,
            (Market.SPOT, StreamType.TRADE): self.spot_transaction_stream_message_queue,
            (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH): self.usd_m_futures_orderbook_stream_message_queue,
            (Market.USD_M_FUTURES, StreamType.TRADE): self.usd_m_futures_transaction_stream_message_queue,
            (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH): self.coin_m_orderbook_stream_message_queue,
            (Market.COIN_M_FUTURES, StreamType.TRADE): self.coin_m_transaction_stream_message_queue,
        }

    def _get_queue(self, market: Market, stream_type: StreamType) -> DifferenceDepthQueue | TradeQueue:
        return self.queue_lookup.get((market, stream_type))

    def run(
        self,
        instruments: Dict,
        file_duration_seconds: int,
        dump_path: str | None,
        websockets_lifetime_seconds: int,
        save_to_json: bool = False,
        save_to_zip: bool = False,
        send_zip_to_blob: bool = True,
        snapshot_fetcher_interval_seconds: int = 60,
    ) -> None:

        for market, pairs in instruments.items():
            market_enum = Market[market.upper()]

            self.start_stream_supervisor_service(
                    pairs,
                    StreamType.DIFFERENCE_DEPTH,
                    market_enum,
                    websockets_lifetime_seconds
            )

            self.start_stream_supervisor_service(
                    pairs,
                    StreamType.TRADE,
                    market_enum,
                    websockets_lifetime_seconds
            )

            self.start_stream_writer(
                    market_enum,
                    file_duration_seconds,
                    dump_path,
                    StreamType.DIFFERENCE_DEPTH,
                    save_to_json,
                    save_to_zip,
                    send_zip_to_blob
            )

            self.start_stream_writer(
                    market_enum,
                    file_duration_seconds,
                    dump_path,
                    StreamType.TRADE,
                    save_to_json,
                    save_to_zip,
                    send_zip_to_blob
            )

            self.start_snapshot_daemon(
                    pairs,
                    market_enum,
                    dump_path,
                    snapshot_fetcher_interval_seconds,
                    save_to_json,
                    save_to_zip,
                    send_zip_to_blob
            )

    def start_stream_supervisor_service(
        self,
        pairs: List[str],
        stream_type: StreamType,
        market: Market,
        websockets_lifetime_seconds: int
    ) -> None:
        queue = self._get_queue(market, stream_type)
        thread = threading.Thread(
            target=self._stream_service_supervisor,
            args=(
                queue,
                pairs,
                stream_type,
                market,
                websockets_lifetime_seconds
            )
        )
        thread.start()

    def start_stream_writer(
        self,
        market: Market,
        file_duration_seconds: int,
        dump_path: str,
        stream_type: StreamType,
        save_to_json: bool,
        save_to_zip: bool,
        send_zip_to_blob: bool,
    ) -> None:

        queue = self._get_queue(market, stream_type)
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
                send_zip_to_blob,
            ),
        )
        thread.start()

    @staticmethod
    def _stream_service_supervisor(
        queue: DifferenceDepthQueue | TradeQueue,
        pairs: List[str],
        stream_type: StreamType,
        market: Market,
        websockets_lifetime_seconds: int,
    ) -> None:

        queue.set_pairs_amount = len(pairs)

        stream_listener = StreamListener(
            queue=queue, pairs=pairs, stream_type=stream_type, market=market
        )

        if stream_type is StreamType.DIFFERENCE_DEPTH:
            queue.currently_accepted_stream_id = stream_listener.id.id

        old_stream_listener_thread = threading.Thread(
            target=stream_listener.websocket_app.run_forever
        )
        old_stream_listener_thread.daemon = True
        old_stream_listener_thread.start()

        while True:
            time.sleep(websockets_lifetime_seconds)

            new_stream_listener = StreamListener(
                queue=queue, pairs=pairs, stream_type=stream_type, market=market
            )

            new_stream_listener_thread = threading.Thread(
                target=new_stream_listener.websocket_app.run_forever
            )
            new_stream_listener_thread.daemon = True
            new_stream_listener_thread.start()

            while queue.did_websockets_switch_successfully is False:
                time.sleep(1)

            print("switched successfully")

            stream_listener.websocket_app.close()
            queue.did_websockets_switch_successfully = False

            stream_listener = new_stream_listener
            old_stream_listener_thread = new_stream_listener_thread

            del new_stream_listener
            del new_stream_listener_thread

    def start_snapshot_daemon(
        self,
        pairs,
        market,
        dump_path,
        interval,
        save_to_json,
        save_to_zip,
        send_zip_to_blob,
    ):
        thread = threading.Thread(
                target=self._snapshot_daemon,
                args=(
                    pairs,
                    market,
                    dump_path,
                    interval,
                    save_to_json,
                    save_to_zip,
                    send_zip_to_blob,
                ),
        )
        thread.start()

    def _snapshot_daemon(
        self,
        pairs: List[str],
        market: Market,
        dump_path: str,
        interval: int,
        save_to_json: bool,
        save_to_zip: bool,
        send_zip_to_blob: bool,
    ) -> None:

        while self.global_shutdown_flag is False:
            for pair in pairs:
                try:
                    snapshot, request_timestamp, receive_timestamp = self._get_snapshot(
                        pair, market
                    )
                    snapshot["_rq"] = request_timestamp
                    snapshot["_rc"] = receive_timestamp

                    file_name = self.get_file_name(
                        pair=pair, market=market, stream_type=StreamType.DEPTH_SNAPSHOT
                    )
                    file_path = os.path.join(dump_path, file_name)

                    if save_to_json is True:
                        self._save_to_json(snapshot, file_path)

                    if save_to_zip is True:
                        self._save_to_zip(snapshot, file_name, file_path)

                    if send_zip_to_blob is True:
                        self._send_zipped_json_to_blob(snapshot, file_name)

                except Exception as e:
                    self.logger.info(
                        f"error whilst fetching snapshot: {market} {StreamType.DEPTH_SNAPSHOT}: {e}"
                    )

            time.sleep(interval)

        self.logger.info(f"{market}: snapshot daemon has ended")

    def _get_snapshot(
        self, pair: str, market: Market
    ) -> Tuple[Dict[str, Any], int, int] | None:

        url = URLFactory.get_snapshot_url(market=market, pair=pair)

        try:
            request_timestamp = self._get_utc_timestamp_epoch_milliseconds()
            response = requests.get(url, timeout=5)
            receive_timestamp = self._get_utc_timestamp_epoch_milliseconds()
            response.raise_for_status()
            data = response.json()

            return data, request_timestamp, receive_timestamp

        except Exception as e:
            self.logger.info(f"error whilst fetching snapshot: {e}")

    def _stream_writer(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        market: Market,
        file_duration_seconds: int,
        dump_path: str,
        stream_type: StreamType,
        save_to_json: bool,
        save_to_zip: bool,
        send_zip_to_blob: bool,
    ):

        while self.global_shutdown_flag is False:
            self._process_stream_data(
                queue,
                market,
                dump_path,
                stream_type,
                save_to_json,
                save_to_zip,
                send_zip_to_blob,
            )

            self._sleep_with_flag_check(file_duration_seconds)

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

        interval = 2
        for _ in range(0, duration, interval):
            if self.global_shutdown_flag is True:
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
        send_zip_to_blob: bool,
    ) -> None:

        if not queue.empty():

            stream_data = defaultdict(list)

            while not queue.empty():
                message, timestamp_of_receive = queue.get_nowait()
                message = json.loads(message)

                stream = message["stream"]
                message["_E"] = timestamp_of_receive
                stream_data[stream].append(message)

            for stream, data in stream_data.items():
                _pair = stream.split("@")[0]
                file_name = self.get_file_name(_pair, market, stream_type)
                file_path = os.path.join(dump_path, file_name)

                if save_to_json is True:
                    self._save_to_json(data, file_path)

                if save_to_zip is True:
                    self._save_to_zip(data, file_name, file_path)

                if send_zip_to_blob is True:
                    self._send_zipped_json_to_blob(data, file_name)

    def _save_to_json(self, data, file_path) -> None:
        try:
            with open(file_path, "w") as f:
                json.dump(data, f)
        except IOError as e:
            self.logger.info(f"IO error when writing to file {file_path}: {e}")

    def _save_to_zip(self, data, file_name, file_path):
        zip_file_path = f"{file_path}.zip"
        try:
            with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
                json_data = json.dumps(data)
                json_filename = f"{file_name}.json"
                zipf.writestr(json_filename, json_data)
        except IOError as e:
            self.logger.info(f"IO error when writing to zip file {zip_file_path}: {e}")

    def _send_zipped_json_to_blob(self, data, file_name):
        try:
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
                json_data = json.dumps(data)
                json_filename = f"{file_name}.json"
                zipf.writestr(json_filename, json_data)

            zip_buffer.seek(0)

            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=f"{file_name}.zip"
            )

            blob_client.upload_blob(zip_buffer, overwrite=True)
            self.logger.info(f"Successfully uploaded {file_name}.zip to blob storage.")
        except Exception as e:
            self.logger.info(f"Error uploading zip to blob: {e}")

    def get_file_name(
        self,
        pair: str,
        market: Market,
        stream_type: StreamType,
        extension: str = "json",
    ) -> str:

        pair_lower = pair.lower()
        formatted_now_timestamp = self._get_utc_formatted_timestamp()

        market_mapping = {
            Market.SPOT: "spot",
            Market.USD_M_FUTURES: "futures_usd_m",
            Market.COIN_M_FUTURES: "futures_coin_m",
        }

        data_type_mapping = {
            StreamType.DIFFERENCE_DEPTH: "binance_difference_depth",
            StreamType.DEPTH_SNAPSHOT: "binance_snapshot",
            StreamType.TRADE: "binance_transaction_broadcast",
        }

        market_short_name = market_mapping.get(market, "unknown_market")
        prefix = data_type_mapping.get(stream_type, "unknown_data_type")

        return f"{prefix}_{market_short_name}_{pair_lower}_{formatted_now_timestamp}.{extension}"

    @staticmethod
    def _get_utc_formatted_timestamp() -> str:
        return datetime.utcnow().strftime("%d-%m-%YT%H-%M-%SZ")

    @staticmethod
    def _get_utc_timestamp_epoch_milliseconds() -> int:
        return round(datetime.now(timezone.utc).timestamp() * 1000)

    @staticmethod
    def _get_utc_timestamp_epoch_seconds() -> int:
        return round(datetime.now(timezone.utc).timestamp())


class BadConfigException(Exception):
    ...


class BadAzureParameters(Exception):
    ...


def launch_data_sink(
    config,
    dump_path: str = "dump/",
    azure_blob_parameters_with_key: str | None = None,
    container_name: str | None = None,
    dump_path_to_log_file: str | None = None,
    dump_logs_to_files: bool = False
):
    valid_markets = {"spot", "usd_m_futures", "coin_m_futures"}

    instruments = config.get("instruments")

    if not instruments or not isinstance(instruments, dict):
        raise BadConfigException("Instruments config is missing or not a dictionary.")

    if not (0 < len(instruments) <= 3):
        raise BadConfigException("Config must contain 1 to 3 markets.")

    for market, pairs in instruments.items():
        if market not in valid_markets:
            raise BadConfigException(f"Invalid or not handled market: {market}")
        if not pairs or not isinstance(pairs, list):
            raise BadConfigException(f"Pairs for market {market} are missing or invalid.")

    if config['send_zip_to_blob'] and (not azure_blob_parameters_with_key or not container_name):
        raise BadAzureParameters('Azure blob parameters with key or container name is missing or empty')

    logger = setup_logger(dump_path_to_log_file, dump_logs_to_files=dump_logs_to_files)

    logger.info("\n%s", logo)
    logger.info("Starting Binance Archiver...")
    logger.info("Configuration:\n%s", pprint.pformat(config, indent=1))

    archiver_daemon = ArchiverDaemon(
        logger=logger,
        azure_blob_parameters_with_key=azure_blob_parameters_with_key,
        container_name=container_name
    )

    if dump_path[0] == "/":
        logger.warning(
            "specified dump_path starts with '/': presumably dump_path is wrong"
        )

    if not os.path.exists(dump_path):
        os.makedirs(dump_path)

    archiver_daemon.run(
        instruments=config["instruments"],
        file_duration_seconds=config["file_duration_seconds"],
        dump_path=dump_path,
        websockets_lifetime_seconds=config["websocket_life_time_seconds"],
        save_to_json=config["save_to_json"],
        save_to_zip=config["save_to_zip"],
        send_zip_to_blob=config["send_zip_to_blob"],
        snapshot_fetcher_interval_seconds=config["snapshot_fetcher_interval_seconds"],
    )
