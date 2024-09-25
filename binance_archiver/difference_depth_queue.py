import json
import re
import threading
import uuid
from queue import Queue
from typing import Any, Dict, final
from collections import deque

from binance_archiver.exceptions import ClassInstancesAmountLimitException
from binance_archiver.binance_archiver_enums.market_enum import Market
from binance_archiver.binance_archiver_enums.run_mode_enum import RunMode
from binance_archiver.stream_id import StreamId


class DifferenceDepthQueue:
    _instances = []
    _lock = threading.Lock()
    _instances_amount_limit = 4
    _event_timestamp_pattern = re.compile(r'"E":\d+,')

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if len(cls._instances) >= cls._instances_amount_limit:
                raise ClassInstancesAmountLimitException(f"Cannot create more than {cls._instances_amount_limit} "
                                                         f"instances of DifferenceDepthQueue")
            instance = super(DifferenceDepthQueue, cls).__new__(cls)
            cls._instances.append(instance)
            return instance

    @classmethod
    def get_instance_count(cls):
        return len(cls._instances)

    @classmethod
    def clear_instances(cls):
        with cls._lock:
            cls._instances.clear()

    def __init__(self, market: Market, run_mode: RunMode, global_queue: Queue | None = None):
        self._market = market
        self.lock = threading.Lock()
        self.currently_accepted_stream_id = None
        self.no_longer_accepted_stream_id = None
        self.did_websockets_switch_successfully = False
        self._two_last_throws = {}

        self.run_mode = run_mode

        if run_mode is RunMode.LISTENER and global_queue is None:
            raise ValueError('run_mode is RunMode.Listener and global_queue is None')

        self.queue = Queue() if run_mode is RunMode.DATA_SINK else global_queue

    @property
    @final
    def market(self):
        return self._market

    def put_queue_message(self, message: str, stream_listener_id: StreamId, timestamp_of_receive: int) -> None:
        with self.lock:
            if stream_listener_id.id == self.no_longer_accepted_stream_id:
                return

            if stream_listener_id.id == self.currently_accepted_stream_id:
                self.queue.put((message, timestamp_of_receive))

            self._append_message_to_compare_structure(stream_listener_id, message)

            do_throws_match = self._do_last_two_throws_match(stream_listener_id.pairs_amount, self._two_last_throws)

            if do_throws_match is True:
                self.set_new_stream_id_as_currently_accepted()

    def _append_message_to_compare_structure(self, stream_listener_id: StreamId, message: str) -> None:
        id_index = stream_listener_id.id

        message_str = self._remove_event_timestamp(message)

        message_list = self._two_last_throws.setdefault(id_index, deque(maxlen=stream_listener_id.pairs_amount))
        message_list.append(message_str)

    def _update_deque_max_len_if_needed(self, id_index: tuple[int, uuid.UUID], new_max_len: int) -> None:
        if id_index in self._two_last_throws:
            existing_deque = self._two_last_throws[id_index]
            if existing_deque.maxlen != new_max_len:
                updated_deque = deque(existing_deque, maxlen=new_max_len)
                self._two_last_throws[id_index] = updated_deque

    def update_deque_max_len(self, new_max_len: int) -> None:
        for id_index in self._two_last_throws:
            existing_deque = self._two_last_throws[id_index]
            updated_deque = deque(existing_deque, maxlen=new_max_len)
            self._two_last_throws[id_index] = updated_deque

    @staticmethod
    def _remove_event_timestamp(message: str) -> str:
        return DifferenceDepthQueue._event_timestamp_pattern.sub('', message)

    @staticmethod
    def _do_last_two_throws_match(amount_of_listened_pairs: int, two_last_throws: Dict) -> bool:
        keys = list(two_last_throws.keys())

        if len(keys) < 2:
            return False

        if len(two_last_throws[keys[0]]) == len(two_last_throws[keys[1]]) == amount_of_listened_pairs:

            last_throw_streams_set: set[str] = {json.loads(entry)['stream'] for entry in two_last_throws[keys[0]]}
            second_last_throw_streams_set: set[str] = {json.loads(entry)['stream'] for entry in two_last_throws[keys[1]]}

            if len(last_throw_streams_set) != amount_of_listened_pairs:
                return False
            if len(second_last_throw_streams_set) != amount_of_listened_pairs:
                return False

            if two_last_throws[keys[0]] == two_last_throws[keys[1]]:
                return True

        return False

    def set_new_stream_id_as_currently_accepted(self):
        self.currently_accepted_stream_id = max(self._two_last_throws.keys(), key=lambda x: x[0])
        self.no_longer_accepted_stream_id = min(self._two_last_throws.keys(), key=lambda x: x[0])

        self._two_last_throws = {}
        self.did_websockets_switch_successfully = True

    def get(self) -> Any:
        entry = self.queue.get()
        return entry

    def get_nowait(self) -> Any:
        entry = self.queue.get_nowait()
        return entry

    def clear(self) -> None:
        self.queue.queue.clear()

    def empty(self) -> bool:
        return self.queue.empty()

    def qsize(self) -> int:
        return self.queue.qsize()