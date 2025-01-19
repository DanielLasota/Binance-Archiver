from __future__ import annotations

import logging
import threading
import time
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.stream_listener import StreamListener

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
    ):
        self.config = config
        self.instruments = config['instruments']
        self.logger = logger

    def run_streams(self):
        for market_str, pairs in self.instruments.items():
            market = Market[market_str.upper()]
            for stream_type in [StreamType.DIFFERENCE_DEPTH_STREAM, StreamType.TRADE_STREAM]:
                self.start_stream_service(
                    stream_type=stream_type,
                    market=market
                )

    def start_stream_service(self, stream_type: StreamType, market: Market) -> None:
        pairs = self.instruments[market.name.lower()]

        thread = threading.Thread(
            target=self._stream_service,
            args=(
                pairs,
                stream_type,
                market
            ),
            name=f'stream_service: market: {market}, stream_type: {stream_type}'
        )
        thread.start()

    def _stream_service(
        self,
        pairs: list[str],
        stream_type: StreamType,
        market: Market
    ) -> None:

        while True:

            try:
                old_stream_listener = StreamListener(
                    logger=self.logger,
                    pairs=pairs,
                    stream_type=stream_type,
                    market=market
                )

                old_stream_listener.start_websocket_app()

                while True:
                    time.sleep(999999999)

            except Exception as e:
                print(e)
