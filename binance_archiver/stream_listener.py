import json
import logging
import threading
import time
import traceback

from websocket import WebSocketApp, ABNF

from binance_archiver.difference_depth_queue import DifferenceDepthQueue
from binance_archiver.exceptions import WrongListInstanceException, PairsLengthException
from binance_archiver.enum_.market_enum import Market
from binance_archiver.stream_id import StreamId
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.blackout_supervisor import BlackoutSupervisor
from binance_archiver.trade_queue import TradeQueue
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
        queue: TradeQueue | DifferenceDepthQueue,
        pairs: list[str],
        stream_type: StreamType,
        market: Market
    ):

        if not isinstance(pairs, list):
            raise WrongListInstanceException('pairs argument is not a list')
        if len(pairs) == 0:
            raise PairsLengthException('pairs len is zero')

        self.logger = logger
        self.queue = queue
        self.pairs = pairs
        self.stream_type = stream_type
        self.market = market

        self.id: StreamId = StreamId(pairs=pairs)
        self.websocket_app: WebSocketApp = self._construct_websocket_app(self.queue, self.pairs, self.stream_type, self.market)
        self.thread: threading.Thread | None = None
        self._blackout_supervisor: BlackoutSupervisor

    def start_websocket_app(self):
        self.thread = threading.Thread(
            target=self.websocket_app.run_forever,
            kwargs={'reconnect': 2},
            name=f'websocket app thread {self.stream_type} {self.market} {self.id.start_timestamp}'
        )
        self.thread.start()
        self._blackout_supervisor.run()

    def restart_websocket_app(self):
        self.websocket_app.close()

        while self.websocket_app.sock:
            if self.websocket_app.sock.connected is False:
                break
            time.sleep(1)

        if self.thread is not None:
            self.thread.join()

        self.websocket_app = None
        self.websocket_app = self._construct_websocket_app(self.queue, self.pairs, self.stream_type, self.market)

        self.start_websocket_app()

    def change_subscription(self, pair, action):
        max_retries = 5
        retry_delay = 2

        for attempt in range(max_retries):
            if self.websocket_app.sock and self.websocket_app.sock.connected:
                break
            else:
                self.logger.error(
                    f"Cannot {action}, WebSocket is not connected. Attempt {attempt + 1} of {max_retries}")
                time.sleep(retry_delay)
        else:
            self.logger.error(f"Failed to {action}, WebSocket did not connect after {max_retries} attempts.")
            return

        pair = pair.lower()

        method = None
        if action.lower() == "subscribe":
            method = "SUBSCRIBE"
        elif action.lower() == "unsubscribe":
            method = "UNSUBSCRIBE"

        message = None

        if self.stream_type == StreamType.TRADE_STREAM:
            message = {
                "method": method,
                "params": [f"{pair}@trade"],
                "id": 1
            }

        elif self.stream_type == StreamType.DIFFERENCE_DEPTH_STREAM:
            message = {
                "method": method,
                "params": [f"{pair}@depth@100ms"],
                "id": 1
            }

        self.websocket_app.send(json.dumps(message))
        self.logger.info(f"{method} message sent for pair: {pair}")
        if isinstance(self.queue, DifferenceDepthQueue):
            self.queue.update_deque_max_len(self.id.pairs_amount)

    def _construct_websocket_app(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        pairs: list[str],
        stream_type: StreamType,
        market: Market
    ) -> WebSocketApp:

        self._blackout_supervisor = BlackoutSupervisor(
            stream_type=stream_type,
            market=market,
            check_interval_in_seconds=5,
            max_interval_without_messages_in_seconds=20 if market is Market.COIN_M_FUTURES else 15,
            on_error_callback=lambda: self.restart_websocket_app(),
            logger=self.logger
        )

        stream_url_methods = {
            StreamType.DIFFERENCE_DEPTH_STREAM: URLFactory.get_difference_depth_stream_url,
            StreamType.TRADE_STREAM: URLFactory.get_trade_stream_url
        }

        url_method = stream_url_methods.get(stream_type, None)
        url = url_method(market, pairs)

        def _on_difference_depth_message(ws, message):
            # self.logger.info(f"self.id.start_timestamp: {self.id.start_timestamp} {market} {stream_type}: {message}")

            timestamp_of_receive = int(time.time() * 1000 + 0.5)

            if 'stream' in message:
                queue.put_queue_message(
                    stream_listener_id=self.id,
                    message=message,
                    timestamp_of_receive=timestamp_of_receive
                )
            self._blackout_supervisor.notify()

        def _on_trade_message(ws, message):
            # self.logger.info(f"self.id.start_timestamp: {self.id.start_timestamp} {market} {stream_type}: {message}")

            timestamp_of_receive = int(time.time() * 1000 + 0.5)

            if 'stream' in message:
                queue.put_trade_message(
                    stream_listener_id=self.id,
                    message=message,
                    timestamp_of_receive=timestamp_of_receive
                )
            self._blackout_supervisor.notify()

        def _on_error(ws, error):
            self.logger.error(f"_error: {market} {stream_type} {self.id.start_timestamp}: {error} "
                              f"_error: Traceback (most recent call last): {traceback.format_exc()}")

        def _on_close(ws, close_status_code, close_msg):
            self.logger.info(
                f"_on_close: {market} {stream_type} {self.id.start_timestamp}"
                f": WebSocket connection closed, {close_msg} (code: {close_status_code})"
            )
            self._blackout_supervisor.shutdown_supervisor()

        def _on_ping(ws, message: str, *args, **kwargs):
            self.logger.debug(f'_on_ping: {market} {stream_type} ping has been received'
                              f': {message}, args: {args}, kwargs: {kwargs}')

            ws.send(message, ABNF.OPCODE_PONG)

        def _on_open(ws):
            self.logger.info(f"_on_open: {market} {stream_type} {self.id.start_timestamp}"
                             f": WebSocket connection opened")

        def _on_reconnect(ws):
            self.logger.info(f'_on_reconnect: {market} {stream_type} {self.id.start_timestamp}')

        websocket_app = WebSocketApp(
            url=url,
            on_message=(
                _on_trade_message
                if stream_type == StreamType.TRADE_STREAM
                else _on_difference_depth_message
            ),
            on_error=_on_error,
            on_close=_on_close,
            on_ping=_on_ping,
            on_open=_on_open,
            on_reconnect=_on_reconnect
        )

        return websocket_app
