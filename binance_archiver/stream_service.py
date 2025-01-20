from __future__ import annotations

import logging
import threading
import time
import traceback

from binance_archiver.queue_pool import QueuePoolListener, QueuePoolDataSink
from binance_archiver.difference_depth_queue import DifferenceDepthQueue
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.stream_listener import StreamListener
from binance_archiver.trade_queue import TradeQueue


class StreamService:


    __slots__ = [
        'config',
        'instruments',
        'logger',
        'queue_pool',
        'global_shutdown_flag',
        'is_someone_overlapping_right_now_flag',
        'stream_listeners',
        'overlap_lock'
    ]

    def __init__(
        self,
        config: dict,
        logger: logging.Logger,
        queue_pool: QueuePoolDataSink | QueuePoolListener,
        global_shutdown_flag: threading.Event
    ):
        self.config = config
        self.instruments = config['instruments']
        self.logger = logger
        self.queue_pool = queue_pool
        self.global_shutdown_flag = global_shutdown_flag
        self.is_someone_overlapping_right_now_flag = threading.Event()
        self.stream_listeners = {}
        self.overlap_lock: threading.Lock = threading.Lock()

    def run_streams(self):
        for market_str, pairs in self.instruments.items():
            market = Market[market_str.upper()]
            for stream_type in [StreamType.DIFFERENCE_DEPTH_STREAM, StreamType.TRADE_STREAM]:
                self.start_stream_service(
                    stream_type=stream_type,
                    market=market
                )

    def start_stream_service(self,stream_type: StreamType, market: Market) -> None:
        queue = self.queue_pool.get_queue(market, stream_type)
        pairs = self.instruments[market.name.lower()]

        thread = threading.Thread(
            target=self._stream_service,
            args=(
                queue,
                pairs,
                stream_type,
                market
            ),
            name=f'stream_service: market: {market}, stream_type: {stream_type}'
        )
        thread.start()

    def _stream_service(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        pairs: list[str],
        stream_type: StreamType,
        market: Market
    ) -> None:

        def sleep_with_flag_check(duration) -> None:
            interval = 1
            for _ in range(0, duration, interval):
                if self.global_shutdown_flag.is_set():
                    break
                time.sleep(interval)

        while not self.global_shutdown_flag.is_set():
            new_stream_listener = None
            old_stream_listener = None

            try:
                old_stream_listener = StreamListener(
                    logger=self.logger,
                    queue=queue,
                    pairs=pairs,
                    stream_type=stream_type,
                    market=market
                )
                self.stream_listeners[(market, stream_type, 'old')] = old_stream_listener

                if stream_type is StreamType.DIFFERENCE_DEPTH_STREAM:
                    queue.currently_accepted_stream_id = old_stream_listener.id.id
                elif stream_type is StreamType.TRADE_STREAM:
                    queue.currently_accepted_stream_id = old_stream_listener.id

                old_stream_listener.start_websocket_app()
                new_stream_listener = None

                while not self.global_shutdown_flag.is_set():
                    sleep_with_flag_check(self.config['websocket_life_time_seconds'])

                    while self.is_someone_overlapping_right_now_flag.is_set():
                        time.sleep(1)

                    with self.overlap_lock:
                        self.is_someone_overlapping_right_now_flag.set()
                        self.logger.info(f'Started changing procedure {market} {stream_type}')

                        new_stream_listener = StreamListener(
                            logger=self.logger,
                            queue=queue,
                            pairs=pairs,
                            stream_type=stream_type,
                            market=market
                        )

                        new_stream_listener.start_websocket_app()
                        self.stream_listeners[(market, stream_type, 'new')] = new_stream_listener

                    while not queue.did_websockets_switch_successfully and not self.global_shutdown_flag.is_set():
                        time.sleep(1)

                    with self.overlap_lock:
                        self.is_someone_overlapping_right_now_flag.clear()
                    self.logger.info(f"{market} {stream_type} switched successfully")

                    if not self.global_shutdown_flag.is_set():
                        queue.did_websockets_switch_successfully = False

                        old_stream_listener.websocket_app.close()
                        old_stream_listener.thread.join()

                        old_stream_listener = new_stream_listener
                        old_stream_listener.thread = new_stream_listener.thread

                        self.stream_listeners[(market, stream_type, 'new')] = None
                        self.stream_listeners[(market, stream_type, 'old')] = old_stream_listener

            except Exception as e:
                self.logger.error(f'{e}, something bad happened')
                self.logger.error("Traceback (most recent call last):")
                self.logger.error(traceback.format_exc())

            finally:
                if new_stream_listener is not None:
                    for _ in range(10):
                        if new_stream_listener.websocket_app.sock.connected is False:
                            time.sleep(1)
                        else:
                            new_stream_listener.websocket_app.close()
                            break
                if old_stream_listener is not None:
                    for _ in range(10):
                        if old_stream_listener.websocket_app.sock.connected is False:
                            time.sleep(1)
                        else:
                            old_stream_listener.websocket_app.close()
                            break

                if (new_stream_listener is not None and new_stream_listener.websocket_app.sock
                        and new_stream_listener.websocket_app.sock.connected is False):
                    new_stream_listener = None

                if (old_stream_listener is not None and old_stream_listener.websocket_app.sock
                        and old_stream_listener.websocket_app.sock.connected is False):
                    old_stream_listener = None

                time.sleep(6)

    def update_subscriptions(self, market: Market, asset_upper: str, action: str):
        for stream_type in [StreamType.DIFFERENCE_DEPTH_STREAM, StreamType.TRADE_STREAM]:
            for status in ['old', 'new']:
                stream_listener: StreamListener = self.stream_listeners.get((market, stream_type, status))
                if stream_listener:
                    stream_listener.change_subscription(action=action, pair=asset_upper)
