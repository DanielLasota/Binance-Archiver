import re
import orjson
import threading
from queue import Queue
from collections import deque
from typing import final

from binance_archiver.exceptions import ClassInstancesAmountLimitException
from binance_archiver.enum_.market_enum import Market
from binance_archiver.stream_id import StreamId


class DifferenceDepthQueue:

    __slots__ = [
        '_market',
        'lock',
        'currently_accepted_stream_id',
        'no_longer_accepted_stream_id',
        'did_websockets_switch_successfully',
        '_two_last_throws',
        'queue'
    ]

    _instances = []
    _lock = threading.Lock()
    _instances_amount_limit = 3
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

    def __init__(self, market: Market, global_queue: Queue | None = None):
        self._market = market
        self.lock = threading.Lock()
        self.currently_accepted_stream_id = None
        self.no_longer_accepted_stream_id = None
        self.did_websockets_switch_successfully = False
        self._two_last_throws = {}

        self.queue = Queue() if global_queue is None else global_queue

    @property
    @final
    def market(self):
        return self._market

    def put_queue_message(self, message: str, stream_listener_id: StreamId, timestamp_of_receive: int) -> None:
        with self.lock:
            if stream_listener_id.id == self.no_longer_accepted_stream_id:
                return

            if stream_listener_id.id == self.currently_accepted_stream_id:
                message_with_timestamp_of_receive = message[:-1] + f',"_E":{timestamp_of_receive}}}'
                self.queue.put(message_with_timestamp_of_receive)

            self._append_message_to_compare_structure(stream_listener_id, message)

            do_throws_match = self._do_last_two_throws_match(stream_listener_id.pairs_amount, self._two_last_throws)

            if do_throws_match is True:
                self.set_new_stream_id_as_currently_accepted()

    def _append_message_to_compare_structure(self, stream_listener_id: StreamId, message: str) -> None:
        id_index = stream_listener_id.id

        message_str = self._remove_event_timestamp(message)

        message_list = self._two_last_throws.setdefault(id_index, deque(maxlen=stream_listener_id.pairs_amount))
        message_list.append(message_str)

    def update_deque_max_len(self, new_max_len: int) -> None:
        for id_index in self._two_last_throws:
            existing_deque = self._two_last_throws[id_index]
            updated_deque = deque(existing_deque, maxlen=new_max_len)
            self._two_last_throws[id_index] = updated_deque

    @staticmethod
    def _remove_event_timestamp(message: str) -> str:
        return DifferenceDepthQueue._event_timestamp_pattern.sub('', message)

    @staticmethod
    def _do_last_two_throws_match(amount_of_listened_pairs: int, two_last_throws: dict) -> bool:
        if len(two_last_throws) < 2:
            return False

        keys = list(two_last_throws.keys())
        last_throw = two_last_throws[keys[0]]
        second_last_throw = two_last_throws[keys[1]]

        if len(last_throw) != amount_of_listened_pairs or len(second_last_throw) != amount_of_listened_pairs:
            return False

        last_throw_streams_set = {orjson.loads(entry)['stream'] for entry in last_throw}
        second_last_throw_streams_set = {orjson.loads(entry)['stream'] for entry in second_last_throw}

        if len(last_throw_streams_set) != amount_of_listened_pairs or len(
                second_last_throw_streams_set) != amount_of_listened_pairs:
            return False

        return last_throw == second_last_throw

    def set_new_stream_id_as_currently_accepted(self):
        self.currently_accepted_stream_id = max(self._two_last_throws.keys(), key=lambda x: x[0])
        self.no_longer_accepted_stream_id = min(self._two_last_throws.keys(), key=lambda x: x[0])

        self._two_last_throws = {}
        self.did_websockets_switch_successfully = True

    def get(self) -> any:
        entry = self.queue.get()
        return entry

    def get_nowait(self) -> any:
        entry = self.queue.get_nowait()
        return entry

    def clear(self) -> None:
        self.queue.queue.clear()

    def empty(self) -> bool:
        return self.queue.empty()

    def qsize(self) -> int:
        return self.queue.qsize()
