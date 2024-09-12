import json
import logging
import threading
import time
from typing import List
from websocket import WebSocketApp, ABNF

from binance_archiver.orderbook_level_2_listener.difference_depth_queue import DifferenceDepthQueue
from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.stream_id import StreamId
from binance_archiver.orderbook_level_2_listener.stream_type_enum import StreamType
from binance_archiver.orderbook_level_2_listener.blackoutsupervisor import BlackoutSupervisor
from binance_archiver.orderbook_level_2_listener.trade_queue import TradeQueue
from binance_archiver.orderbook_level_2_listener.url_factory import URLFactory


class PairsLengthException(Exception):
    ...


class WrongListInstanceException(Exception):
    ...


class StreamListener:
    def __init__(
        self,
        logger: logging.Logger,
        queue: TradeQueue | DifferenceDepthQueue,
        pairs: List[str],
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
            daemon=True,
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
        if not self.websocket_app.sock or not self.websocket_app.sock.connected:
            self.logger.info(f"Cannot {action}, WebSocket is not connected")
            return

        pair = pair.lower()

        _internal_dict = {
            StreamType.DIFFERENCE_DEPTH: 'depth',
            StreamType.TRADE: 'trade'
        }

        _stream_type = _internal_dict.get(self.stream_type)

        if action.lower() == "subscribe":
            method = "SUBSCRIBE"
        elif action.lower() == "unsubscribe":
            method = "UNSUBSCRIBE"

        message = {
            "method": method,
            "params": [f"{pair}@{_stream_type}"],
            "id": 1
        }

        self.websocket_app.send(json.dumps(message))
        self.logger.info(f"{method} message sent for pair: {pair}")

    def _construct_websocket_app(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        pairs: List[str],
        stream_type: StreamType,
        market: Market
    ) -> WebSocketApp:

        self._blackout_supervisor = BlackoutSupervisor(
            stream_type=stream_type,
            market=market,
            check_interval_in_seconds=5,
            max_interval_without_messages_in_seconds=10,
            on_error_callback=lambda: self.restart_websocket_app(),
            logger=self.logger
        )

        stream_url_methods = {
            StreamType.DIFFERENCE_DEPTH: URLFactory.get_orderbook_stream_url,
            StreamType.TRADE: URLFactory.get_trade_stream_url
        }

        url_method = stream_url_methods.get(stream_type, None)
        url = url_method(market, pairs)

        def _on_difference_depth_message(ws, message):
            # self.logger.info(f"{self.id.start_timestamp} {market} {stream_type}: {message}")

            timestamp_of_receive = int(time.time() * 1000 + 0.5)

            if 'stream' in message:
                queue.put_queue_message(
                    stream_listener_id=self.id,
                    message=message,
                    timestamp_of_receive=timestamp_of_receive
                )
            self._blackout_supervisor.notify()

        def _on_trade_message(ws, message):
            # self.logger.info(f"{self.id.start_timestamp} {market} {stream_type}: {message}")

            timestamp_of_receive = int(time.time() * 1000 + 0.5)

            if 'stream' in message:
                queue.put_trade_message(message=message, timestamp_of_receive=timestamp_of_receive)
            self._blackout_supervisor.notify()

        def _on_error(ws, error):
            self.logger.error(f"_on_error: {market} {stream_type} {self.id.start_timestamp}: {error}")

        def _on_close(ws, close_status_code, close_msg):
            self.logger.info(
                f"_on_close: {market} {stream_type} {self.id.start_timestamp}: WebSocket connection closed, "
                f"{close_msg} (code: {close_status_code})"
            )
            self._blackout_supervisor.shutdown_supervisor()

        def _on_ping(ws, message):
            ws.send("", ABNF.OPCODE_PONG)

        def _on_open(ws):
            self.logger.info(f"_on_open : {market} {stream_type} {self.id.start_timestamp}: WebSocket connection opened")

        def _on_reconnect(ws):
            self.logger.info(f'_on_reconnect: {market} {stream_type} {self.id.start_timestamp}')

        websocket_app = WebSocketApp(
            url=url,
            on_message=(
                _on_trade_message
                if stream_type == StreamType.TRADE
                else _on_difference_depth_message
            ),
            on_error=_on_error,
            on_close=_on_close,
            on_ping=_on_ping,
            on_open=_on_open,
            on_reconnect=_on_reconnect
        )

        return websocket_app
