from __future__ import annotations

import logging
import pprint
import time
import threading

from binance_archiver.logo import binance_archiver_logo
from .listener_observer_updater import ListenerObserverUpdater
from .queue_pool import QueuePoolListener
from .setup_logger import setup_logger
from .exceptions import WebSocketLifeTimeException, BadConfigException
from .abstract_base_classes import Subject, Observer
from .snapshot_manager import ListenerSnapshotStrategy, SnapshotManager
from .stream_service import StreamService

__all__ = [
    'launch_data_listener'
]


def launch_data_listener(
        config,
        should_dump_logs: bool = False,
        init_observers: list[object] = None
) -> ListenerFacade:

    valid_markets = {"spot", "usd_m_futures", "coin_m_futures"}
    instruments = config.get("instruments")

    if not isinstance(instruments, dict) or not (0 < len(instruments) <= 3):
        raise BadConfigException("Config must contain 1 to 3 markets and must be a dictionary.")

    for market, pairs in instruments.items():
        if market not in valid_markets or not isinstance(pairs, list):
            raise BadConfigException(f"Invalid pairs for market {market}.")

    websocket_lifetime = config.get("websocket_life_time_seconds", 0)
    if not (30 <= websocket_lifetime <= 60 * 60 * 23):
        raise WebSocketLifeTimeException('Bad websocket_life_time_seconds')

    logger = setup_logger(should_dump_logs=should_dump_logs)
    logger.info("\n%s", binance_archiver_logo)
    logger.info("Starting Binance Listener...")
    logger.info("Configuration:\n%s", pprint.pformat(config, indent=1))

    listener_facade = ListenerFacade(
        config=config,
        logger=logger,
        init_observers=init_observers
    )

    listener_facade.run()
    return listener_facade

class ListenerFacade(Subject):

    __slots__ = [
        'config',
        'logger',
        'instruments',
        'global_shutdown_flag',
        'queue_pool',
        'stream_service',
        '_observers',
        'whistleblower',
        'snapshot_manager'
    ]

    def __init__(
            self,
            config: dict,
            logger: logging.Logger,
            init_observers: list[Observer] | None = None
    ) -> None:
        self.config = config
        self.logger = logger
        self.instruments = config["instruments"]
        self.global_shutdown_flag = threading.Event()

        self.queue_pool = QueuePoolListener()
        self.stream_service = StreamService(
            config=config,
            logger=self.logger,
            queue_pool=self.queue_pool,
            global_shutdown_flag=self.global_shutdown_flag
        )

        self._observers = init_observers if init_observers is not None else []

        self.whistleblower = ListenerObserverUpdater(
            logger=self.logger,
            observers=self._observers,
            global_queue=self.queue_pool.global_queue,
            global_shutdown_flag=self.global_shutdown_flag
        )

        snapshot_strategy = ListenerSnapshotStrategy(global_queue=self.queue_pool.global_queue)

        self.snapshot_manager = SnapshotManager(
            config=self.config,
            logger=self.logger,
            snapshot_strategy=snapshot_strategy,
            global_shutdown_flag=self.global_shutdown_flag
        )

    def attach(self, observer: Observer) -> None:
        self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        self._observers.remove(observer)

    def notify(self, message) -> None:
        for observer in self._observers:
            observer.update(message)

    def run(self) -> None:
        dump_path = self.config.get("dump_path", "dump/")

        snapshot_fetcher_interval_seconds = self.config["snapshot_fetcher_interval_seconds"]

        self.stream_service.run_streams()

        self.whistleblower.run_whistleblower()

        while not any(queue_.qsize() != 0 for queue_ in self.queue_pool.queue_lookup.values()):
            time.sleep(0.001)

        time.sleep(5)

        self.snapshot_manager.run_snapshots(
            dump_path=dump_path
        )

    def shutdown(self):
        self.logger.info("Shutting down archiver")
        self.global_shutdown_flag.set()

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



