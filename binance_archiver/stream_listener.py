import logging
import threading
import traceback

from websocket import WebSocketApp

from binance_archiver.exceptions import WrongListInstanceException, PairsLengthException
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.url_factory import URLFactory


class StreamListener:
    __slots__ = [
        'logger',
        'queue',
        'pairs',
        'stream_type',
        'market',
        'id',
        'websocket_app',
        'thread',
        '_blackout_supervisor'
    ]

    def __init__(
        self,
        logger: logging.Logger,
        pairs: list[str],
        stream_type: StreamType,
        market: Market
    ):

        if not isinstance(pairs, list):
            raise WrongListInstanceException('pairs argument is not a list')
        if len(pairs) == 0:
            raise PairsLengthException('pairs len is zero')

        self.logger = logger
        self.pairs = pairs
        self.stream_type = stream_type
        self.market = market

        self.websocket_app: WebSocketApp = self._construct_websocket_app(
            self.pairs,
            self.stream_type,
            self.market
        )
        self.thread: threading.Thread | None = None

    def start_websocket_app(self):
        self.thread = threading.Thread(
            target=self.websocket_app.run_forever,
            name=f'websocket app thread {self.stream_type} {self.market}'
        )
        self.thread.start()


    def _construct_websocket_app(
        self,
        pairs: list[str],
        stream_type: StreamType,
        market: Market
    ) -> WebSocketApp:

        stream_url_methods = {
            StreamType.DIFFERENCE_DEPTH_STREAM: URLFactory.get_difference_depth_stream_url,
            StreamType.TRADE_STREAM: URLFactory.get_trade_stream_url
        }

        url_method = stream_url_methods.get(stream_type, None)
        url = url_method(market, pairs)

        def _on_difference_depth_message(ws, message):
            ...

        def _on_trade_message(ws, message):
            ...

        def _on_error(ws, error):
            self.logger.error(f"_error: {market} {stream_type} : {error} "
                              f"_error: Traceback (most recent call last): {traceback.format_exc()}")

        def _on_close(ws, close_status_code, close_msg):
            self.logger.info(
                f"_on_close: {market} {stream_type} "
                f": WebSocket connection closed, {close_msg} (code: {close_status_code})"
            )

        def _on_open(ws):
            self.logger.info(f"_on_open: {market} {stream_type} "
                             f": WebSocket connection opened")

        websocket_app = WebSocketApp(
            url=url,
            on_message=(
                _on_trade_message
                if stream_type == StreamType.TRADE_STREAM
                else _on_difference_depth_message
            ),
            on_error=_on_error,
            on_close=_on_close,
            on_open=_on_open,
        )

        return websocket_app
