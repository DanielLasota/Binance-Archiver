from __future__ import annotations

import logging
import queue
import threading
from queue import Queue

from binance_archiver.abstract_base_classes import Observer


class ListenerObserverUpdater:

    __slots__ = [
        'logger',
        'observers',
        'global_queue',
        'global_shutdown_flag'
    ]

    def __init__(
            self,
            logger: logging.Logger,
            observers: list[Observer],
            global_queue: Queue,
            global_shutdown_flag: threading.Event
    ) -> None:
        self.logger = logger
        self.observers = observers
        self.global_queue = global_queue
        self.global_shutdown_flag = global_shutdown_flag

    def process_global_queue(self):
        while not self.global_shutdown_flag.is_set():

            if self.global_queue.qsize() > 200:
                self.logger.warning(f'qsize: {self.global_queue.qsize()}')

            try:
                message = self.global_queue.get(timeout=1)
                for observer in self.observers:
                    observer.update(message)

            except queue.Empty:
                continue

    def run_whistleblower(self):
        whistleblower_thread = threading.Thread(target=self.process_global_queue)
        whistleblower_thread.start()
