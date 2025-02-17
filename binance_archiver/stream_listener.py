import asyncio
import json
import logging
import threading
import time
import traceback
import websockets

from websockets.legacy.client import WebSocketClientProtocol

from binance_archiver.enum_.asset_parameters import AssetParameters
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.difference_depth_queue import DifferenceDepthQueue
from binance_archiver.trade_queue import TradeQueue
from binance_archiver.stream_id import StreamId
from binance_archiver.exceptions import WrongListInstanceException, PairsLengthException
from binance_archiver.blackout_supervisor import BlackoutSupervisor
from binance_archiver.url_factory import URLFactory


class StreamListener:
    __slots__ = [
        'logger',
        'queue',
        'asset_parameters',
        'id',
        'thread',
        '_stop_event',
        '_ws_lock',
        '_ws',
        '_url',
        '_loop',
        '_blackout_supervisor',
    ]

    def __init__(
        self,
        queue: TradeQueue | DifferenceDepthQueue,
        asset_parameters: AssetParameters
    ):

        if not isinstance(asset_parameters.pairs, list):
            raise WrongListInstanceException('pairs argument is not a list')
        if len(asset_parameters.pairs) == 0:
            raise PairsLengthException('pairs len is zero')

        self.logger = logging.getLogger('binance_data_sink')
        self.queue = queue
        self.asset_parameters = asset_parameters

        self.id: StreamId = StreamId(pairs=self.asset_parameters.pairs)
        self.thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._ws_lock = threading.Lock()
        self._ws: WebSocketClientProtocol | None = None

        self._url = URLFactory.get_stream_url(asset_parameters)

        self._blackout_supervisor = BlackoutSupervisor(
            asset_parameters=asset_parameters,
            on_error_callback=lambda: self.restart_websocket_app(),
            max_interval_without_messages_in_seconds=60 if self.asset_parameters.market is Market.COIN_M_FUTURES else 30,
        )

    def start_websocket_app(self):
        self.logger.info(f"{self.asset_parameters.market} {self.asset_parameters.stream_type} {self.id.start_timestamp} Starting streamListener")
        self.thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.thread.start()
        self._blackout_supervisor.run()

    def restart_websocket_app(self):
        self.logger.warning(f"{self.asset_parameters} {self.asset_parameters} {self.asset_parameters} Restarting streamListener")
        self.close_websocket()
        self.start_websocket_app()

    def close_websocket_app(self):
        self.logger.info(f"{self.asset_parameters.market} {self.asset_parameters.stream_type} {self.id.start_timestamp} Closing StreamListener")
        self.close_websocket()
        self._blackout_supervisor.shutdown_supervisor()

    def close_websocket(self):
        self._blackout_supervisor.shutdown_supervisor()
        self._stop_event.set()
        with self._ws_lock:
            if self._ws:
                try:
                    asyncio.run_coroutine_threadsafe(self._ws.close(), self._loop)
                except Exception as e:
                    self.logger.exception(f"Error while closing the websocket: {e}")

        if self.thread and self.thread.is_alive():
            self.thread.join()
        self.thread = None

    def change_subscription(self, pair: str, action: str):
        pair = pair.lower()
        method = None
        if action.lower() == "subscribe":
            method = "SUBSCRIBE"
        elif action.lower() == "unsubscribe":
            method = "UNSUBSCRIBE"

        if not method:
            self.logger.error(f"Unknown action: {action}, skipping subscription change.")
            return

        message = {}
        if self.asset_parameters.stream_type == StreamType.TRADE_STREAM:
            message = {
                "method": method,
                "params": [f"{pair}@trade"],
                "id": 1
            }
        elif self.asset_parameters.stream_type == StreamType.DIFFERENCE_DEPTH_STREAM:
            message = {
                "method": method,
                "params": [f"{pair}@depth@100ms"],
                "id": 1
            }
            self.queue.update_deque_max_len(len(self.asset_parameters.pairs))

        loop = getattr(self, '_loop', None)
        if loop is not None and loop.is_running():
            def _do_send():
                asyncio.create_task(self._send_message(json.dumps(message)))
            loop.call_soon_threadsafe(_do_send)
        else:
            self.logger.error(f"Loop is not running, cannot {action} for pair={pair}.")

    def _run_event_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop

        try:
            loop.run_until_complete(self._main_coroutine())
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    async def _main_coroutine(self):
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(self._url) as ws:
                    # self.logger.info(f"WebSocket connected: {self._url}")
                    with self._ws_lock:
                        self._ws = ws

                    await self._listen_messages(ws)

            except (OSError, websockets.exceptions.ConnectionClosed) as e:
                self.logger.error(f"Connection error/reconnect for {self.asset_parameters}: {e}")
                await asyncio.sleep(2)
            except Exception as e:
                self.logger.error(f"Unexpected error in _main_coroutine: {self.asset_parameters} {e}")
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(2)
            finally:
                with self._ws_lock:
                    self._ws = None

    async def _listen_messages(self, ws: WebSocketClientProtocol):
        while not self._stop_event.is_set():
            try:
                message = await ws.recv()

                raw_timestamp_of_receive_ns = time.time_ns()
                timestamp_of_receive_rounded = (
                    (raw_timestamp_of_receive_ns + 500) // 1_000
                    if self.asset_parameters.market is Market.SPOT
                    else (raw_timestamp_of_receive_ns + 500_000) // 1_000_000
                )

            except websockets.exceptions.ConnectionClosed as e:
                if not self._stop_event.is_set():
                    self.logger.warning(
                        f"{e} \n"
                        f"stream_listener_id: {self.id.id} "
                        f"{self.asset_parameters.market} {self.asset_parameters.stream_type} "
                        f"self._stop_event.is_set(): {self._stop_event.is_set()}"
                    )


            self._handle_incoming_message(
                raw_message=message,
                timestamp_of_receive=timestamp_of_receive_rounded
            )
            self._blackout_supervisor.notify()

    def _handle_incoming_message(self, raw_message: str, timestamp_of_receive: int):
        # self.logger.info(f"self.id.start_timestamp: {self.id.start_timestamp} {raw_message}")

        if 'stream' in raw_message:
            if self.asset_parameters.stream_type == StreamType.DIFFERENCE_DEPTH_STREAM:
                self.queue.put_difference_depth_message(
                    stream_listener_id=self.id,
                    message=raw_message,
                    timestamp_of_receive=timestamp_of_receive
                )
            elif self.asset_parameters.stream_type == StreamType.TRADE_STREAM:
                self.queue.put_trade_message(
                    stream_listener_id=self.id,
                    message=raw_message,
                    timestamp_of_receive=timestamp_of_receive
                )

    async def _send_message(self, message: str):
        with self._ws_lock:
            if not self._ws:
                self.logger.error("Cannot send message – websocket is None.")
                return
            ws = self._ws

        try:
            await ws.send(message)
        except Exception as e:
            self.logger.error(f"Error while sending message: {e}")
