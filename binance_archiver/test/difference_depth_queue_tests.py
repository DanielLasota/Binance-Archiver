import queue
import time
from collections import deque
import json
from queue import Queue
import pytest

from binance_archiver.difference_depth_queue import DifferenceDepthQueue, \
    ClassInstancesAmountLimitException
from binance_archiver.enum_.market_enum import Market
from binance_archiver.stream_id import StreamId


def format_message_string_that_is_pretty_to_binance_string_format(message: str) -> str:
    message = message.strip()
    data = json.loads(message)
    compact_message = json.dumps(data, separators=(',', ':'))

    return compact_message


class TestDifferenceDepthQueue:


    # format_message_string_that_is_pretty_to_binance_string_format method from above
    #
    def test_given_pretty_printed_message_from_test_when_reformatting_then_message_is_in_binance_format(self):

        pretty_message_from_sample_test = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [
                    ]
                }
            }
        '''

        binance_format_message = format_message_string_that_is_pretty_to_binance_string_format(
            pretty_message_from_sample_test)
        assert binance_format_message == ('{"stream":"trxusdt@depth@100ms","data":{"e":"depthUpdate","E":1720337869317,'
                                          '"s":"TRXUSDT","U":4609985365,"u":4609985365,'
                                          '"b":[["0.12984000","123840.00000000"]],"a":[]}}')

    # DifferenceDepthQueue singleton init test
    #
    def test_given_too_many_difference_depth_queue_instances_exists_when_creating_new_then_exception_is_thrown(self):
        for _ in range(3):
            DifferenceDepthQueue(Market.SPOT)

        with pytest.raises(ClassInstancesAmountLimitException):
            DifferenceDepthQueue(Market.SPOT)

        DifferenceDepthQueue.clear_instances()

    def test_given_checking_amount_of_instances_when_get_instance_count_invocation_then_amount_is_correct(self):
        instance_count = DifferenceDepthQueue.get_instance_count()
        assert instance_count == 0

        for _ in range(3):
            DifferenceDepthQueue(Market.SPOT)

        assert DifferenceDepthQueue.get_instance_count() == 3

        DifferenceDepthQueue.clear_instances()

    def test_given_instances_amount_counter_reset_when_clear_instances_method_invocation_then_amount_is_zero(self):
        for _ in range(3):
            DifferenceDepthQueue(Market.SPOT)

        DifferenceDepthQueue.clear_instances()

        assert DifferenceDepthQueue.get_instance_count() == 0
        DifferenceDepthQueue.clear_instances()

    # put_queue_message test
    #
    def test_given_putting_message_when_putting_message_of_currently_accepted_stream_id_then_message_is_being_added_to_the_queue(self):

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        pairs = config['instruments']['spot']

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id
        mocked_timestamp_of_receive = 2115

        _first_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''
        _first_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_1)

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get_nowait())
        assert (_first_listener_message_1, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert len(difference_depth_queue_content_list) == 1

        DifferenceDepthQueue.clear_instances()

    def test_given_putting_message_from_no_longer_accepted_stream_listener_id_when_try_to_put_then_message_is_not_added_to_the_queue(self):

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        pairs = config['instruments']['spot']

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        assert old_stream_listener_id.pairs_amount == 3
        assert new_stream_listener_id.pairs_amount == 3

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = old_stream_listener_id.id

        _old_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _new_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _new_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869318,
                    "s": "DOTUSDT",
                    "U": 7871863948,
                    "u": 7871863950,
                    "b": [
                        [
                            "7.19800000",
                            "1817.61000000"
                        ],
                        [
                            "7.19300000",
                            "1593.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "7.20800000",
                            "1911.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_4 = '''
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869319,
                    "s": "DOTUSDT",
                    "U": 7871863948,
                    "u": 7871863950,
                    "b": [
                        [
                            "7.19800000",
                            "1817.61000000"
                        ],
                        [
                            "7.19300000",
                            "1593.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "7.20800000",
                            "1911.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_2)
        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_3)
        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_1)
        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_2)
        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_3)
        _new_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_4)
        _old_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_4)

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_4,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_4,
            timestamp_of_receive=2115
        )

        assert difference_depth_queue.currently_accepted_stream_id == new_stream_listener_id.id
        assert difference_depth_queue.qsize() == 4

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get_nowait())

        assert (_old_listener_message_1, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert (_old_listener_message_2, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert (_old_listener_message_3, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert (_new_listener_message_4, mocked_timestamp_of_receive) in difference_depth_queue_content_list

        assert (_new_listener_message_1, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        assert (_new_listener_message_2, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        assert (_new_listener_message_3, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        assert (_old_listener_message_4, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        DifferenceDepthQueue.clear_instances()

    def test_given_putting_stream_message_and_two_last_throws_are_not_equal_when_two_listeners_messages_are_being_compared_then_currently_accepted_stream_id_is_not_changed_and_only_old_stream_listener_messages_are_put_in(self):
        """
        difference lays in a _old_listener_message_1 / _new_listener_message_1, new stream listener is + 1 ms
        """

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = old_stream_listener_id.id

        _old_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _new_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_2)
        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_3)
        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_1)
        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_2)
        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_3)

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=2115
        )

        assert difference_depth_queue.currently_accepted_stream_id == old_stream_listener_id.id
        assert difference_depth_queue.qsize() == 3

        difference_depth_queue_content_list = [difference_depth_queue.get_nowait() for _ in
                                               range(difference_depth_queue.qsize())]

        assert (_old_listener_message_1, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert (_old_listener_message_2, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert (_old_listener_message_3, mocked_timestamp_of_receive) in difference_depth_queue_content_list

        assert (_new_listener_message_1, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        assert (_new_listener_message_2, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        assert (_new_listener_message_3, mocked_timestamp_of_receive) not in difference_depth_queue_content_list

        DifferenceDepthQueue.clear_instances()

    def test_given_putting_stream_message_and_two_last_throws_are_equal_when_two_listeners_messages_are_being_compared_then_currently_accepted_stream_id_is_changed_and_only_old_stream_listener_messages_are_put_in(self):

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        pairs = config['instruments']['spot']

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = old_stream_listener_id.id

        _old_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _new_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_2)
        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_3)
        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_1)
        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_2)
        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_3)

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )
        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )
        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )
        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )
        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )
        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue_content_list = [difference_depth_queue.get_nowait() for _ in
                                               range(difference_depth_queue.qsize())]

        assert difference_depth_queue.currently_accepted_stream_id == new_stream_listener_id.id

        expected_list = [
            (_old_listener_message_1, mocked_timestamp_of_receive),
            (_old_listener_message_2, mocked_timestamp_of_receive),
            (_old_listener_message_3, mocked_timestamp_of_receive)
        ]

        assert difference_depth_queue_content_list == expected_list

        DifferenceDepthQueue.clear_instances()

    # run_mode tests
    #
    def test_given_data_listener_mode_and_global_queue_when_initializing_difference_depth_queue_then_queue_is_set_to_global_queue(self):
        global_queue = Queue()
        ddq = DifferenceDepthQueue(market=Market.SPOT, global_queue=global_queue)
        assert ddq.queue is global_queue
        DifferenceDepthQueue.clear_instances()

    def test_given_difference_depth_message_in_data_listener_mode_when_putting_message_then_message_is_added_to_global_queue(self):
        global_queue = Queue()
        ddq = DifferenceDepthQueue(market=Market.SPOT, global_queue=global_queue)
        stream_listener_id = StreamId(pairs=['BTCUSDT'])
        ddq.currently_accepted_stream_id = stream_listener_id.id
        message = '''
        {
            "stream": "btcusdt@depth@100ms",
            "data": {
                "E": 123456789,
                "b": [
                    ["50000.00", "1.0"]
                ],
                "a": [
                    ["51000.00", "2.0"]
                ]
            }
        }
        '''
        formatted_message = format_message_string_that_is_pretty_to_binance_string_format(message)
        timestamp_of_receive = 1234567890
        ddq.put_queue_message(formatted_message, stream_listener_id, timestamp_of_receive)
        assert not global_queue.empty()
        queued_message, received_timestamp = global_queue.get_nowait()
        assert queued_message == formatted_message
        assert received_timestamp == timestamp_of_receive
        DifferenceDepthQueue.clear_instances()

    def test_given_messages_in_data_listener_mode_when_using_queue_operations_then_operations_reflect_global_queue_state(self):
        global_queue = Queue()
        ddq = DifferenceDepthQueue(market=Market.SPOT, global_queue=global_queue)
        stream_listener_id = StreamId(pairs=['ETHUSDT'])
        ddq.currently_accepted_stream_id = stream_listener_id.id
        message = '''
        {
            "stream": "ethusdt@depth@100ms",
            "data": {
                "E": 123456789,
                "b": [
                    ["4000.00", "1.0"]
                ],
                "a": [
                    ["4100.00", "2.0"]
                ]
            }
        }
        '''
        formatted_message = format_message_string_that_is_pretty_to_binance_string_format(message)
        timestamp_of_receive = 1234567890
        ddq.put_queue_message(formatted_message, stream_listener_id, timestamp_of_receive)
        assert ddq.qsize() == 1
        assert not ddq.empty()
        queued_message, received_timestamp = ddq.get_nowait()
        assert queued_message == formatted_message
        assert received_timestamp == timestamp_of_receive
        assert ddq.empty()
        DifferenceDepthQueue.clear_instances()

    def test_given_empty_global_queue_in_data_listener_mode_when_checking_queue_then_empty_and_get_nowait_raises_exception(self):
        global_queue = Queue()
        ddq = DifferenceDepthQueue(market=Market.SPOT, global_queue=global_queue)
        assert ddq.empty()
        with pytest.raises(queue.Empty):
            ddq.get_nowait()
        DifferenceDepthQueue.clear_instances()


    # _append_message_to_compare_structure
    #
    def test_given_putting_message_when_adding_two_different_stream_listeners_message_throws_to_compare_structure_then_structure_is_ok(self):
        """difference lays in a _old_listener_message_1 / _new_listener_message_1, new stream listener is + 1 ms"""

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = old_stream_listener_id.id

        _old_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _new_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_2)
        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_3)
        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_1)
        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_2)

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        expected_comparison_structure = {
            old_stream_listener_id.id: deque([
                DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_1),
                DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_2),
                DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_3)
            ]),
            new_stream_listener_id.id: deque([
                DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_1),
                DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_2)
            ])
        }

        assert difference_depth_queue._two_last_throws == expected_comparison_structure
        assert old_stream_listener_id.id in difference_depth_queue._two_last_throws
        assert new_stream_listener_id.id in difference_depth_queue._two_last_throws
        assert len(difference_depth_queue._two_last_throws[old_stream_listener_id.id]) == 3
        assert len(difference_depth_queue._two_last_throws[new_stream_listener_id.id]) == 2

        DifferenceDepthQueue.clear_instances()

    def test_given_putting_message_when_adding_messages_to_the_full_queues_then_is_last_message_being_removed(self):
        """difference lays in a _old_listener_message_1 / _new_listener_message_1, new stream listener is + 1 ms"""

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = old_stream_listener_id.id

        _old_listener_message_1 = '''
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_2 = '''
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _old_listener_message_4 = '''
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869316,
                    "s": "DOTUSDT",
                    "U": 7871863948,
                    "u": 7871863950,
                    "b": [
                        [
                            "7.19800000",
                            "1817.61000000"
                        ],
                        [
                            "7.19300000",
                            "1593.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "7.20800000",
                            "1911.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_1 = '''
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_2 = '''
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [

                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _new_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869315,
                    "s": "DOTUSDT",
                    "U": 7871863948,
                    "u": 7871863950,
                    "b": [
                        [
                            "7.19800000",
                            "1817.61000000"
                        ],
                        [
                            "7.19300000",
                            "1593.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "7.20800000",
                            "1911.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        expected_comparison_structure = {
            old_stream_listener_id.id: deque([
                # _old_expected_message_1,
                DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_2),
                DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_3),
                DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_4)
            ], maxlen=old_stream_listener_id.pairs_amount),
            new_stream_listener_id.id: deque([
                # _new_expected_message_1,
                DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_2),
                DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_3),
                DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_4)
            ], maxlen=new_stream_listener_id.pairs_amount)
        }

        assert difference_depth_queue._two_last_throws == expected_comparison_structure
        assert old_stream_listener_id.id in difference_depth_queue._two_last_throws
        assert new_stream_listener_id.id in difference_depth_queue._two_last_throws
        assert len(difference_depth_queue._two_last_throws[old_stream_listener_id.id]) == 3
        assert len(difference_depth_queue._two_last_throws[new_stream_listener_id.id]) == 3

        DifferenceDepthQueue.clear_instances()

    #_remove_event_timestamp
    #
    def test_given_processing_message_to_compare_structure_when_removing_event_timestamp_then_output_is_ok(self):

        sample_message_queue = [
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961396,"s":"SOLUSDT","U":13653945429,"u":13653945433,"b":[["130.30000000","271.76800000"],["129.68000000","77.56800000"]],"a":[["130.31000000","759.04600000"],["130.45000000","321.85800000"],["139.45000000","7.59500000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961406,"s":"BNBUSDT","U":10326331602,"u":10326331602,"b":[],"a":[["522.90000000","77.21500000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961414,"s":"BTCUSDT","U":51157289092,"u":51157289107,"b":[["57710.99000000","1.53088000"],["57708.99000000","0.00009000"],["57656.43000000","0.00000000"],["57437.60000000","0.01051000"],["57436.89000000","0.00000000"],["33000.00000000","16.17838000"]],"a":[["57711.00000000","6.41608000"],["57713.68000000","0.12203000"],["57713.74000000","0.00000000"],["57714.99000000","0.25998000"],["57718.01000000","0.12203000"],["57718.48000000","0.00040000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961415,"s":"ETHUSDT","U":36258111898,"u":36258111919,"b":[["2441.81000000","8.31240000"],["2441.80000000","0.00220000"],["2441.77000000","0.01130000"],["2441.58000000","0.00000000"],["2441.55000000","4.09670000"],["2441.44000000","8.63980000"],["2441.43000000","0.00000000"],["2440.98000000","9.60590000"],["2440.20000000","18.03310000"],["2200.03000000","0.08540000"]],"a":[["2441.98000000","12.34010000"],["2442.47000000","0.70300000"],["2447.99000000","0.12580000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961495,"s":"SOLUSDT","U":13653945434,"u":13653945439,"b":[["130.29000000","49.16500000"],["130.24000000","641.42400000"],["130.23000000","542.42600000"],["130.22000000","590.08100000"],["130.07000000","114.75100000"]],"a":[["130.36000000","338.69500000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961506,"s":"BNBUSDT","U":10326331603,"u":10326331605,"b":[["522.80000000","82.14100000"],["522.70000000","143.63200000"]],"a":[["522.90000000","77.17700000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961514,"s":"BTCUSDT","U":51157289108,"u":51157289113,"b":[["57689.19000000","0.25344000"],["57610.99000000","0.02000000"]],"a":[["57711.00000000","6.41529000"],["57718.48000000","0.00000000"],["57811.00000000","0.02770000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961515,"s":"ETHUSDT","U":36258111920,"u":36258111935,"b":[["2441.45000000","10.53290000"],["2441.44000000","0.00000000"],["2441.20000000","6.74900000"],["2440.87000000","0.70440000"],["2439.83000000","1.88270000"],["2439.76000000","29.95070000"],["2439.04000000","10.44470000"],["2197.63000000","0.00500000"]],"a":[["2441.82000000","83.52030000"],["2442.84000000","19.47480000"],["2442.92000000","1.83300000"],["2444.54000000","0.00000000"],["2444.59000000","4.98630000"],["2497.08000000","0.41240000"]]}}',
            '{"stream":"dotusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961573,"s":"DOTUSDT","U":8119446138,"u":8119446139,"b":[],"a":[["4.12100000","4833.56000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961595,"s":"SOLUSDT","U":13653945440,"u":13653945456,"b":[["130.30000000","218.52800000"],["130.29000000","29.22300000"],["130.28000000","194.17200000"]],"a":[["130.32000000","452.44700000"],["130.37000000","403.88000000"],["130.40000000","376.19700000"],["130.46000000","277.08200000"],["130.47000000","392.48200000"],["130.64000000","15.30900000"],["130.80000000","126.04800000"],["130.81000000","27.44700000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961606,"s":"BNBUSDT","U":10326331606,"u":10326331612,"b":[["522.70000000","143.62100000"],["188.90000000","0.89600000"],["188.40000000","4.48500000"],["188.30000000","4.02500000"],["188.20000000","3.30000000"],["188.10000000","1.69600000"]],"a":[]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961614,"s":"BTCUSDT","U":51157289114,"u":51157289119,"b":[["57586.94000000","0.14193000"],["57437.60000000","0.00000000"],["57437.09000000","0.09996000"]],"a":[["57778.93000000","3.97290000"],["58025.42000000","0.08636000"],["59023.60000000","0.08474000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961615,"s":"ETHUSDT","U":36258111936,"u":36258111944,"b":[["2441.81000000","8.30320000"],["2441.46000000","6.80100000"],["2441.45000000","0.91080000"],["2440.09000000","0.00000000"],["2438.73000000","11.95230000"],["2200.03000000","0.08040000"]],"a":[]}}',
            '{"stream":"xrpusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961617,"s":"XRPUSDT","U":12011423549,"u":12011423549,"b":[],"a":[["0.56060000","41105.00000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961695,"s":"SOLUSDT","U":13653945457,"u":13653945466,"b":[["130.28000000","203.37200000"],["130.27000000","580.51500000"],["130.26000000","427.13600000"]],"a":[["130.31000000","842.43000000"],["130.32000000","456.08200000"],["130.44000000","221.78500000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961706,"s":"BNBUSDT","U":10326331613,"u":10326331615,"b":[["522.70000000","136.60300000"],["522.60000000","190.77000000"]],"a":[["522.90000000","77.16400000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961714,"s":"BTCUSDT","U":51157289120,"u":51157289154,"b":[["57710.99000000","1.53071000"],["57709.09000000","0.02773000"],["57707.95000000","0.00000000"],["57706.29000000","0.34666000"],["57703.37000000","0.01393000"],["57701.13000000","0.00000000"],["57699.74000000","0.00000000"],["57688.31000000","0.00677000"],["57688.09000000","0.00000000"],["57682.48000000","0.00000000"],["57680.01000000","1.18729000"],["57679.08000000","0.00000000"],["57677.37000000","0.34684000"],["57677.27000000","0.01388000"],["57675.60000000","0.08599000"],["57671.91000000","0.00000000"],["57660.03000000","0.75206000"],["57610.04000000","0.16291000"]],"a":[["57711.00000000","7.45500000"],["57721.54000000","0.00000000"],["57724.61000000","0.43321000"],["57728.20000000","0.25344000"],["57730.00000000","1.05971000"],["57730.59000000","0.05718000"],["57734.77000000","0.11782000"],["57737.25000000","0.00000000"],["57744.96000000","0.05181000"],["57756.82000000","0.03392000"],["57776.99000000","0.00000000"],["57777.81000000","2.38380000"],["57806.53000000","0.00000000"],["57818.90000000","0.00000000"],["57819.06000000","0.00000000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961715,"s":"ETHUSDT","U":36258111945,"u":36258111977,"b":[["2441.81000000","8.30870000"],["2441.75000000","2.83650000"],["2441.47000000","4.80570000"],["2441.46000000","0.00000000"],["2438.55000000","9.82750000"],["2436.59000000","2.87880000"],["2197.63000000","0.01000000"],["881.11000000","0.00690000"],["880.96000000","0.02760000"],["880.89000000","0.00690000"],["880.85000000","0.02760000"],["880.56000000","0.11730000"],["880.21000000","0.08280000"],["880.17000000","0.01380000"],["879.79000000","0.01380000"],["879.58000000","0.00000000"],["879.26000000","0.00000000"]],"a":[["2441.82000000","83.62830000"],["2442.87000000","2.86680000"],["2443.09000000","4.95060000"],["2445.10000000","2.37620000"],["2446.00000000","7.61730000"],["2446.10000000","3.72830000"],["2446.16000000","0.00000000"],["2446.20000000","10.31660000"],["2446.33000000","4.82960000"],["2446.41000000","3.87430000"],["2446.46000000","1.23700000"]]}}',
            '{"stream":"xrpusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961717,"s":"XRPUSDT","U":12011423550,"u":12011423551,"b":[],"a":[["0.56070000","159883.00000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961795,"s":"SOLUSDT","U":13653945467,"u":13653945469,"b":[["130.30000000","262.52800000"]],"a":[["130.31000000","842.81700000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961806,"s":"BNBUSDT","U":10326331616,"u":10326331620,"b":[["522.70000000","136.61400000"],["522.40000000","179.74200000"],["522.20000000","130.09400000"],["521.30000000","92.24400000"]],"a":[["524.50000000","84.61200000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961814,"s":"BTCUSDT","U":51157289155,"u":51157289163,"b":[["57709.09000000","0.04159000"],["57691.97000000","0.09543000"],["57682.87000000","0.00000000"],["57677.27000000","0.00000000"],["57437.09000000","0.00000000"],["57434.90000000","0.01051000"]],"a":[["57719.20000000","0.16124000"],["57720.70000000","0.00000000"],["57778.93000000","0.00000000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961815,"s":"ETHUSDT","U":36258111978,"u":36258111986,"b":[["2441.81000000","8.31920000"],["2441.47000000","0.01020000"],["2441.42000000","4.27060000"],["2440.90000000","3.03670000"],["2440.87000000","0.00000000"]],"a":[["2441.82000000","84.07620000"]]}}',
            '{"stream":"xrpusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961817,"s":"XRPUSDT","U":12011423552,"u":12011423554,"b":[["0.56050000","115803.00000000"]],"a":[["0.56070000","162557.00000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961895,"s":"SOLUSDT","U":13653945470,"u":13653945471,"b":[["129.12000000","116.71700000"],["129.02000000","7.92700000"]],"a":[]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961906,"s":"BNBUSDT","U":10326331621,"u":10326331621,"b":[["522.70000000","136.60300000"]],"a":[]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961914,"s":"BTCUSDT","U":51157289164,"u":51157289176,"b":[["57710.99000000","1.78710000"],["57692.07000000","0.50884000"],["57690.14000000","0.71245000"],["57636.53000000","0.07546000"]],"a":[["57711.00000000","7.11421000"],["57722.34000000","0.12203000"],["57724.99000000","0.60000000"],["57726.67000000","0.12203000"],["57734.14000000","0.43321000"],["57777.81000000","0.00000000"],["58960.00000000","0.03633000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961915,"s":"ETHUSDT","U":36258111987,"u":36258112005,"b":[["2441.81000000","13.85070000"],["2441.60000000","14.13380000"],["2441.58000000","4.09670000"],["2441.55000000","0.00000000"],["2441.43000000","6.83330000"],["2441.42000000","0.00000000"],["2438.59000000","1.22890000"],["2437.06000000","1.88610000"],["2429.00000000","205.94080000"]],"a":[["2441.97000000","0.40950000"],["2441.99000000","0.79930000"],["2442.64000000","0.22000000"],["2443.50000000","0.81980000"],["2443.85000000","1.83230000"],["2446.87000000","2.67010000"],["2451.79000000","0.00000000"]]}}',
            '{"stream":"xrpusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961917,"s":"XRPUSDT","U":12011423555,"u":12011423555,"b":[["0.56050000","115813.00000000"]],"a":[]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382961995,"s":"SOLUSDT","U":13653945472,"u":13653945486,"b":[["130.30000000","271.76800000"],["130.29000000","49.20900000"],["130.26000000","388.71800000"],["130.25000000","622.46300000"],["130.24000000","698.99700000"],["130.23000000","484.85000000"],["130.21000000","303.84600000"],["130.20000000","393.89600000"],["130.15000000","475.53300000"],["120.69000000","2.84300000"]],"a":[["130.35000000","650.78700000"],["130.41000000","665.14400000"],["130.44000000","164.22500000"],["132.48000000","22.71400000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962006,"s":"BNBUSDT","U":10326331622,"u":10326331622,"b":[["515.00000000","221.93300000"]],"a":[]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962014,"s":"BTCUSDT","U":51157289177,"u":51157289235,"b":[["57710.99000000","2.21967000"],["57708.09000000","0.34665000"],["57706.30000000","0.95454000"],["57706.29000000","0.00000000"],["57706.02000000","0.03392000"],["57706.01000000","0.00000000"],["57704.27000000","0.13286000"],["57703.38000000","0.00382000"],["57703.25000000","0.00000000"],["57700.31000000","0.00000000"],["57698.01000000","0.00000000"],["57693.25000000","0.05941000"],["57690.69000000","0.25344000"],["57689.94000000","0.00000000"],["57689.48000000","0.00000000"],["57689.29000000","0.00000000"],["57689.19000000","0.00000000"],["57686.28000000","0.06466000"],["57682.58000000","0.00432000"],["57682.13000000","0.00000000"],["57681.11000000","0.86665000"],["57670.53000000","0.00000000"],["57669.65000000","0.00000000"],["57668.29000000","0.00000000"],["57666.03000000","0.00382000"],["57653.26000000","0.18713000"],["57651.16000000","0.00000000"],["57632.34000000","0.00000000"],["57625.04000000","0.00173000"],["57616.15000000","0.00000000"],["57610.99000000","0.02000000"],["33000.00000000","16.17838000"]],"a":[["57711.00000000","6.36203000"],["57711.29000000","0.00000000"],["57713.99000000","0.00000000"],["57715.29000000","0.00000000"],["57718.16000000","0.00677000"],["57718.78000000","0.13286000"],["57718.97000000","0.32933000"],["57718.98000000","0.00000000"],["57720.69000000","0.00000000"],["57721.53000000","0.00000000"],["57722.99000000","0.00000000"],["57726.22000000","0.00000000"],["57728.20000000","0.00000000"],["57743.20000000","0.00000000"],["57750.45000000","0.06008000"],["57754.90000000","0.00000000"],["57755.54000000","0.00237000"],["57762.17000000","0.00000000"],["57782.46000000","0.00000000"],["57783.14000000","0.18713000"],["57784.40000000","0.00321000"],["57790.99000000","0.00000000"],["57792.91000000","0.00237000"],["57839.99000000","0.00000000"],["57842.29000000","0.00128000"],["59014.75000000","0.00000000"],["59023.60000000","0.00000000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962015,"s":"ETHUSDT","U":36258112006,"u":36258112029,"b":[["2441.81000000","14.85070000"],["2441.65000000","0.91080000"],["2441.58000000","0.00000000"],["2441.45000000","0.00000000"],["2441.43000000","4.27100000"],["2441.04000000","0.70470000"],["2440.90000000","2.33660000"],["2440.79000000","6.14950000"],["2440.70000000","0.81930000"],["2440.54000000","2.60470000"],["2440.21000000","7.98310000"],["2440.17000000","8.19880000"],["2439.64000000","0.03060000"]],"a":[["2441.82000000","80.91210000"],["2442.10000000","0.00000000"],["2442.40000000","6.33520000"],["2442.47000000","0.00000000"],["2442.48000000","0.00210000"],["2442.49000000","12.19120000"],["2442.66000000","0.76990000"],["2442.73000000","0.81930000"],["2442.77000000","15.41120000"],["2442.87000000","2.04750000"],["2497.08000000","0.00000000"]]}}',
            '{"stream":"xrpusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962017,"s":"XRPUSDT","U":12011423556,"u":12011423557,"b":[["0.56050000","116328.00000000"]],"a":[]}}',
            '{"stream":"dotusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962073,"s":"DOTUSDT","U":8119446140,"u":8119446151,"b":[["4.11700000","4013.69000000"]],"a":[["4.12100000","3680.50000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962095,"s":"SOLUSDT","U":13653945487,"u":13653945502,"b":[["130.30000000","282.51400000"],["130.29000000","73.81200000"],["130.28000000","383.75100000"],["130.27000000","556.64600000"],["130.20000000","364.71800000"],["130.17000000","406.75400000"],["130.05000000","686.45800000"],["130.02000000","792.86000000"],["126.00000000","9254.14700000"]],"a":[["130.31000000","758.88300000"],["130.32000000","456.36300000"],["130.33000000","207.73600000"],["130.44000000","164.02500000"],["130.46000000","276.05100000"],["130.64000000","2.55900000"],["130.65000000","1109.24500000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962114,"s":"BTCUSDT","U":51157289236,"u":51157289272,"b":[["57710.99000000","4.13944000"],["57710.95000000","0.00000000"],["57710.72000000","0.20384000"],["57710.46000000","0.17884000"],["57710.00000000","0.16559000"],["57709.12000000","1.37187000"],["57708.00000000","0.17876000"],["57707.28000000","0.00000000"],["57706.30000000","0.25999000"],["57706.24000000","0.00000000"],["57706.00000000","0.19039000"],["57704.28000000","0.00000000"],["57702.37000000","0.00000000"],["57702.35000000","0.00000000"],["57693.39000000","0.05973000"],["57693.25000000","0.00000000"],["57690.69000000","0.00000000"],["57688.93000000","0.00038000"],["57655.86000000","0.08666000"],["57635.46000000","0.00140000"],["57625.03000000","0.00000000"],["57620.57000000","0.00000000"],["57620.56000000","0.00260000"],["57587.62000000","0.00000000"],["57587.61000000","0.00174000"],["57583.53000000","0.00117000"]],"a":[["57711.00000000","6.12404000"],["57713.75000000","0.00000000"],["57716.19000000","0.00000000"],["57719.37000000","0.00040000"],["57722.99000000","0.03465000"],["57730.54000000","0.06037000"],["57774.90000000","0.00016000"],["57827.30000000","0.00013000"],["58025.42000000","0.00000000"],["58190.48000000","0.00174000"],["58190.50000000","0.00000000"],["58223.41000000","0.00260000"],["58223.42000000","0.00000000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962115,"s":"ETHUSDT","U":36258112030,"u":36258112038,"b":[["2441.75000000","2.84080000"],["2441.66000000","9.84100000"],["2441.64000000","13.77280000"],["2441.40000000","11.28220000"],["2441.37000000","1.99550000"],["2439.25000000","12.68000000"],["2435.64000000","10.83770000"]],"a":[["2442.00000000","6.36040000"],["2442.05000000","15.00000000"]]}}',
            '{"stream":"dotusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962173,"s":"DOTUSDT","U":8119446152,"u":8119446169,"b":[["4.11900000","366.89000000"],["4.11800000","1743.42000000"],["4.11600000","4185.54000000"],["4.11300000","6570.21000000"]],"a":[["4.11900000","0.00000000"],["4.12000000","1462.54000000"],["4.12100000","4514.97000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962195,"s":"SOLUSDT","U":13653945503,"u":13653945521,"b":[["130.30000000","399.19600000"],["130.29000000","73.82900000"]],"a":[["130.31000000","434.01300000"],["130.32000000","510.89100000"],["130.35000000","617.78600000"],["130.36000000","349.44100000"],["130.41000000","607.57100000"],["130.42000000","829.33500000"],["130.51000000","230.43900000"],["130.57000000","75.04400000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962206,"s":"BNBUSDT","U":10326331623,"u":10326331641,"b":[["522.80000000","82.14100000"]],"a":[["522.90000000","48.81500000"],["523.00000000","252.93400000"],["523.30000000","140.90200000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962214,"s":"BTCUSDT","U":51157289273,"u":51157289323,"b":[["57710.99000000","5.64353000"],["57709.12000000","0.34527000"],["57702.40000000","0.06360000"],["57701.58000000","0.07294000"],["57699.09000000","0.01733000"],["57693.81000000","0.05941000"],["57693.39000000","0.00000000"],["57692.47000000","0.00000000"],["57691.97000000","0.02249000"],["57683.18000000","1.05506000"],["55985.48000000","0.00178000"]],"a":[["57711.00000000","3.62259000"],["57711.03000000","0.00010000"],["57711.09000000","0.00010000"],["57711.38000000","0.00010000"],["57714.87000000","0.18335000"],["57716.19000000","0.00608000"],["57718.16000000","0.00000000"],["57718.20000000","0.00040000"],["57721.62000000","0.00000000"],["57721.95000000","0.00677000"],["57722.99000000","0.00000000"],["57730.00000000","0.02000000"],["57730.54000000","0.00000000"],["57741.44000000","0.00000000"],["57744.96000000","0.00000000"],["57753.29000000","0.05181000"],["57755.99000000","0.16492000"],["57798.48000000","0.00016000"],["57798.97000000","0.00000000"],["57804.69000000","0.02660000"],["57817.89000000","0.08496000"],["57825.81000000","0.08096000"],["58100.66000000","0.02710000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962215,"s":"ETHUSDT","U":36258112039,"u":36258112169,"b":[["2441.81000000","42.65500000"],["2441.80000000","0.00000000"],["2441.70000000","2.19720000"],["2441.67000000","9.28790000"],["2441.66000000","5.74450000"],["2441.65000000","1.99330000"],["2441.64000000","8.48080000"],["2441.63000000","0.00000000"],["2441.53000000","0.00000000"],["2441.51000000","0.00000000"],["2441.38000000","8.21980000"],["2441.37000000","0.00310000"],["2441.33000000","0.00000000"],["2441.28000000","2.04970000"],["2441.22000000","1.43370000"],["2441.14000000","0.51430000"],["2441.05000000","0.70640000"],["2441.04000000","0.00210000"],["2440.96000000","0.80630000"],["2440.95000000","14.62380000"],["2440.90000000","1.53860000"],["2440.85000000","5.84730000"],["2440.58000000","1.02420000"],["2440.25000000","12.20570000"],["2439.88000000","1.98840000"],["2439.32000000","1.99360000"],["2439.26000000","0.00000000"],["2439.21000000","1.61760000"],["2439.07000000","0.00000000"],["2403.48000000","0.00000000"]],"a":[["2441.82000000","21.77820000"],["2441.87000000","0.00000000"],["2441.88000000","0.00220000"],["2441.89000000","6.41860000"],["2441.91000000","8.38430000"],["2441.97000000","0.00000000"],["2441.98000000","3.81660000"],["2442.01000000","8.19410000"],["2442.02000000","1.65170000"],["2442.03000000","8.19410000"],["2442.05000000","0.00000000"],["2442.06000000","8.99720000"],["2442.07000000","1.07350000"],["2442.08000000","1.78290000"],["2442.10000000","8.19370000"],["2442.12000000","0.00210000"],["2442.13000000","0.00000000"],["2442.15000000","29.87010000"],["2442.19000000","0.00500000"],["2442.20000000","18.54040000"],["2442.22000000","2.03020000"],["2442.28000000","8.60230000"],["2442.39000000","6.79760000"],["2442.40000000","8.14720000"],["2442.61000000","0.00000000"],["2442.64000000","0.91860000"],["2442.73000000","0.00000000"],["2442.84000000","20.29410000"],["2442.86000000","4.77470000"],["2442.90000000","0.00000000"],["2443.10000000","3.55880000"],["2443.79000000","0.86960000"],["2444.05000000","3.86040000"],["2444.06000000","8.19570000"],["2444.35000000","0.00210000"],["2444.55000000","1.86920000"],["2446.62000000","7.35270000"],["2478.36000000","0.00000000"]]}}',
            '{"stream":"xrpusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962217,"s":"XRPUSDT","U":12011423558,"u":12011423558,"b":[],"a":[["0.56070000","160774.00000000"]]}}',
            '{"stream":"dotusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962273,"s":"DOTUSDT","U":8119446170,"u":8119446174,"b":[["4.11900000","690.08000000"],["4.11800000","1398.96000000"],["4.11700000","4621.06000000"],["4.11600000","3578.06000000"]],"a":[["4.12000000","1744.31000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962295,"s":"SOLUSDT","U":13653945522,"u":13653945585,"b":[["130.30000000","475.20100000"],["130.29000000","126.26200000"],["130.28000000","324.63900000"],["130.27000000","636.89700000"],["130.26000000","438.60600000"],["130.25000000","568.08800000"],["130.24000000","641.42400000"],["130.22000000","592.67900000"],["130.21000000","380.66000000"],["130.15000000","398.68000000"]],"a":[["130.31000000","238.68700000"],["130.32000000","431.13200000"],["130.33000000","170.39000000"],["130.34000000","259.17000000"],["130.35000000","628.11000000"],["130.36000000","316.70800000"],["130.37000000","414.62500000"],["130.38000000","510.48200000"],["130.42000000","771.76400000"],["130.43000000","298.42700000"],["130.44000000","240.70800000"],["130.45000000","318.85800000"],["130.51000000","252.32800000"],["130.55000000","203.12000000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962306,"s":"BNBUSDT","U":10326331642,"u":10326331661,"b":[["522.80000000","86.93500000"]],"a":[["522.90000000","38.83800000"],["523.00000000","243.54200000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962314,"s":"BTCUSDT","U":51157289324,"u":51157289439,"b":[["57710.99000000","6.06588000"],["57708.09000000","0.00000000"],["57706.89000000","0.07454000"],["57705.60000000","0.01733000"],["57705.52000000","0.05073000"],["57705.05000000","0.00677000"],["57703.94000000","0.01733000"],["57700.59000000","0.04801000"],["57699.09000000","0.00000000"],["57697.68000000","0.00000000"],["57695.23000000","0.00000000"],["57694.92000000","0.06070000"],["57693.94000000","0.00000000"],["57693.81000000","0.00000000"],["57692.79000000","0.00000000"],["57688.31000000","0.00000000"],["57666.73000000","1.27395000"],["57656.73000000","0.00000000"],["57631.01000000","10.82740000"],["57586.94000000","0.05718000"],["57561.07000000","0.00000000"],["57434.90000000","0.00000000"]],"a":[["57711.00000000","1.53823000"],["57711.09000000","0.00010000"],["57713.68000000","0.00000000"],["57713.85000000","0.00000000"],["57714.31000000","0.16898000"],["57714.99000000","0.00000000"],["57715.09000000","0.00000000"],["57715.72000000","0.00000000"],["57716.31000000","0.00000000"],["57718.01000000","0.00000000"],["57718.06000000","0.00040000"],["57718.26000000","0.00039000"],["57718.97000000","0.00000000"],["57718.99000000","0.00000000"],["57719.21000000","0.00000000"],["57721.05000000","0.00000000"],["57721.95000000","0.00000000"],["57722.89000000","0.00000000"],["57722.99000000","0.86659000"],["57723.27000000","0.01733000"],["57723.97000000","0.32930000"],["57723.98000000","0.00000000"],["57724.24000000","0.00000000"],["57724.61000000","0.00000000"],["57724.69000000","1.03971000"],["57725.22000000","0.00677000"],["57725.99000000","0.04000000"],["57733.27000000","0.00000000"],["57733.88000000","0.06018000"],["57738.99000000","0.16992000"],["57740.11000000","0.04331000"],["57740.64000000","0.00000000"],["57743.66000000","0.43724000"],["57746.76000000","0.04811000"],["57750.58000000","0.16292000"],["57751.10000000","1.10897000"],["57751.30000000","0.00000000"],["57752.36000000","0.16192000"],["57754.93000000","0.00000000"],["57778.30000000","0.00000000"],["57818.14000000","0.08496000"],["57818.54000000","0.08496000"],["57819.83000000","0.08496000"],["57825.55000000","0.07897000"],["57826.72000000","0.08097000"],["57827.83000000","0.08096000"],["57827.93000000","0.08097000"],["57827.94000000","0.08096000"],["58579.20000000","0.00000000"],["58590.72000000","0.08667000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962315,"s":"ETHUSDT","U":36258112170,"u":36258112284,"b":[["2441.81000000","42.65500000"],["2441.80000000","3.85000000"],["2441.79000000","10.09020000"],["2441.73000000","2.45710000"],["2441.68000000","5.57330000"],["2441.67000000","7.58040000"],["2441.60000000","15.18540000"],["2441.58000000","1.02380000"],["2441.50000000","0.41390000"],["2441.41000000","1.21050000"],["2441.36000000","7.88980000"],["2441.34000000","6.14280000"],["2441.33000000","4.09530000"],["2441.26000000","0.00000000"],["2441.22000000","4.79870000"],["2441.21000000","0.40960000"],["2441.19000000","0.70180000"],["2441.05000000","6.14640000"],["2441.04000000","8.19780000"],["2440.94000000","9.20490000"],["2440.83000000","0.00250000"],["2440.79000000","0.00250000"],["2440.73000000","7.98000000"],["2440.25000000","21.78170000"],["2439.49000000","0.00000000"],["2439.30000000","20.50270000"],["2439.26000000","2.99250000"],["2438.83000000","0.00000000"],["2438.22000000","16.35820000"],["2200.03000000","0.07540000"]],"a":[["2441.82000000","0.11050000"],["2441.88000000","0.00000000"],["2441.89000000","0.00000000"],["2441.90000000","0.00220000"],["2441.91000000","5.97960000"],["2441.95000000","0.01020000"],["2441.98000000","0.00000000"],["2442.00000000","6.32330000"],["2442.01000000","0.00000000"],["2442.02000000","5.56270000"],["2442.03000000","0.00000000"],["2442.06000000","8.19720000"],["2442.16000000","0.00000000"],["2442.19000000","0.41460000"],["2442.20000000","18.53440000"],["2442.28000000","0.40660000"],["2442.29000000","0.57480000"],["2442.38000000","1.12370000"],["2442.39000000","0.03040000"],["2442.40000000","8.14120000"],["2442.42000000","7.31480000"],["2442.52000000","18.03170000"],["2442.61000000","8.19570000"],["2442.63000000","10.33130000"],["2442.64000000","0.22000000"],["2442.66000000","0.00000000"],["2442.73000000","0.24960000"],["2442.74000000","0.00310000"],["2442.77000000","14.64240000"],["2442.82000000","0.07700000"],["2442.84000000","19.47480000"],["2442.86000000","1.10070000"],["2442.90000000","0.76990000"],["2442.99000000","8.79920000"],["2443.02000000","0.00310000"],["2443.06000000","0.76880000"],["2443.13000000","20.95140000"],["2443.22000000","2.54890000"],["2443.46000000","1.78190000"],["2444.60000000","0.48070000"],["2444.80000000","1.98080000"],["2445.00000000","2.60000000"],["2446.69000000","3.90360000"],["2446.82000000","2.66590000"],["2446.84000000","1.59940000"],["2447.18000000","1.83230000"],["2450.80000000","0.00000000"],["2454.10000000","0.79100000"],["2454.20000000","0.11300000"],["2454.40000000","0.12000000"],["2454.60000000","0.03510000"]]}}',
            '{"stream":"xrpusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962317,"s":"XRPUSDT","U":12011423559,"u":12011423562,"b":[["0.56050000","116338.00000000"],["0.56040000","142346.00000000"]],"a":[["0.56360000","115948.00000000"],["0.56370000","7581.00000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962395,"s":"SOLUSDT","U":13653945586,"u":13653945599,"b":[["130.30000000","476.62200000"],["130.26000000","468.11500000"],["130.18000000","210.22900000"],["130.17000000","429.74300000"],["130.07000000","152.33700000"],["130.01000000","262.85300000"],["129.99000000","213.15100000"]],"a":[["130.32000000","359.29400000"],["130.33000000","118.38500000"],["130.34000000","292.92000000"],["130.36000000","312.70800000"],["130.38000000","487.49600000"],["130.56000000","39.18100000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962406,"s":"BNBUSDT","U":10326331662,"u":10326331667,"b":[["522.70000000","143.62100000"],["522.60000000","183.75200000"]],"a":[["522.90000000","38.83800000"],["523.00000000","243.54200000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962414,"s":"BTCUSDT","U":51157289440,"u":51157289495,"b":[["57710.99000000","5.99017000"],["57710.00000000","0.17219000"],["57709.36000000","0.12203000"],["57708.00000000","0.18536000"],["57707.14000000","0.13286000"],["57706.00000000","0.19699000"],["57704.27000000","0.00000000"],["57696.90000000","0.05079000"],["57695.18000000","0.05940000"],["57694.92000000","0.00018000"],["57691.12000000","0.00000000"],["57668.29000000","0.25998000"],["57574.42000000","0.08666000"],["57440.00000000","0.11002000"],["33000.00000000","16.17838000"]],"a":[["57711.00000000","2.01780000"],["57711.38000000","0.00000000"],["57711.39000000","0.00000000"],["57712.87000000","0.00000000"],["57713.04000000","0.00000000"],["57713.99000000","0.00000000"],["57714.86000000","0.12113000"],["57717.99000000","0.20804000"],["57718.01000000","0.12203000"],["57718.54000000","0.00000000"],["57718.78000000","0.00000000"],["57719.20000000","0.00000000"],["57720.13000000","0.26861000"],["57720.90000000","0.18575000"],["57720.99000000","0.08258000"],["57721.40000000","0.20157000"],["57721.65000000","0.13286000"],["57722.98000000","0.03465000"],["57722.99000000","0.86659000"],["57724.61000000","0.43321000"],["57729.08000000","0.00000000"],["57737.97000000","0.05203000"],["57738.99000000","0.08496000"],["57743.74000000","0.00086000"],["57767.67000000","0.34630000"],["57819.81000000","0.08496000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962415,"s":"ETHUSDT","U":36258112285,"u":36258112430,"b":[["2441.90000000","61.89030000"],["2441.89000000","8.20300000"],["2441.84000000","0.01020000"],["2441.83000000","0.00000000"],["2441.82000000","0.00000000"],["2441.81000000","11.91970000"],["2441.80000000","7.81640000"],["2441.79000000","5.99390000"],["2441.78000000","0.50000000"],["2441.75000000","5.29790000"],["2441.69000000","0.00000000"],["2441.57000000","1.99270000"],["2441.41000000","9.19050000"],["2441.35000000","1.72780000"],["2441.19000000","0.00000000"],["2440.85000000","6.40410000"],["2440.65000000","0.09500000"],["2440.54000000","2.04790000"],["2440.38000000","12.19120000"],["2439.29000000","0.00000000"],["2438.85000000","5.74580000"],["2438.22000000","3.15810000"],["2437.24000000","1.00000000"],["2435.68000000","2.56790000"],["2197.71000000","0.02000000"],["2196.00000000","72.40440000"]],"a":[["2441.82000000","0.00000000"],["2441.90000000","0.00000000"],["2441.91000000","4.30910000"],["2441.92000000","0.00220000"],["2441.93000000","0.20480000"],["2441.94000000","0.00000000"],["2441.95000000","0.00000000"],["2441.98000000","0.00220000"],["2441.99000000","0.80150000"],["2442.02000000","0.00000000"],["2442.03000000","0.50000000"],["2442.06000000","0.00000000"],["2442.07000000","0.01020000"],["2442.08000000","0.00000000"],["2442.10000000","0.00000000"],["2442.12000000","0.00000000"],["2442.13000000","0.00000000"],["2442.14000000","0.00000000"],["2442.15000000","17.67890000"],["2442.16000000","0.00000000"],["2442.20000000","6.34320000"],["2442.22000000","0.04150000"],["2442.24000000","0.41180000"],["2442.31000000","9.07420000"],["2442.45000000","2.99250000"],["2442.54000000","16.39210000"],["2442.69000000","12.19120000"],["2442.70000000","12.19120000"],["2442.75000000","12.19550000"],["2442.93000000","10.28470000"],["2443.10000000","1.78900000"],["2443.22000000","1.30810000"],["2443.31000000","1.83300000"],["2443.38000000","1.98270000"],["2444.09000000","3.44000000"],["2445.55000000","9.24510000"],["2445.57000000","16.56040000"],["2446.76000000","1.83830000"],["2446.86000000","6.29640000"],["2446.87000000","3.91090000"],["2447.25000000","1.83230000"],["2447.98000000","1.82780000"],["2454.10000000","0.00000000"],["2454.20000000","0.90400000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962495,"s":"SOLUSDT","U":13653945600,"u":13653945616,"b":[["130.30000000","374.61300000"],["130.28000000","297.02600000"],["130.22000000","593.07500000"]],"a":[["130.31000000","352.81100000"],["130.32000000","297.29400000"],["130.37000000","448.33000000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962506,"s":"BNBUSDT","U":10326331668,"u":10326331679,"b":[["522.80000000","83.86200000"]],"a":[["522.90000000","55.13300000"],["523.00000000","243.53100000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962514,"s":"BTCUSDT","U":51157289496,"u":51157289524,"b":[["57710.99000000","5.99014000"],["57708.18000000","0.06360000"],["57706.02000000","0.00000000"],["57704.00000000","0.19260000"],["57695.33000000","0.05942000"],["57695.18000000","0.00000000"],["57685.20000000","0.82746000"],["57670.10000000","0.00000000"],["57610.99000000","0.02000000"]],"a":[["57711.00000000","2.03828000"],["57711.03000000","0.00000000"],["57711.04000000","0.00000000"],["57712.87000000","0.00000000"],["57717.99000000","0.00000000"],["57719.99000000","0.16122000"],["57720.89000000","0.03486000"],["57720.99000000","0.00000000"],["57724.60000000","0.50884000"],["57733.55000000","0.05958000"],["57733.88000000","0.00000000"],["57751.03000000","0.00038000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962515,"s":"ETHUSDT","U":36258112431,"u":36258112475,"b":[["2441.90000000","77.11210000"],["2441.81000000","11.89800000"],["2441.79000000","0.00000000"],["2441.75000000","2.46140000"],["2441.73000000","0.00000000"],["2441.61000000","0.00310000"],["2441.39000000","7.21250000"],["2441.37000000","0.70830000"],["2441.35000000","1.03410000"],["2441.13000000","0.58590000"],["2440.82000000","10.00000000"],["2440.26000000","24.21020000"],["2440.25000000","12.20570000"],["2439.31000000","12.30210000"],["2439.26000000","0.00000000"],["2436.69000000","2.13440000"],["2200.03000000","0.07040000"]],"a":[["2441.91000000","20.75240000"],["2441.98000000","0.00220000"],["2442.08000000","1.06590000"],["2442.14000000","4.18340000"],["2442.15000000","6.55660000"],["2442.25000000","1.46140000"],["2442.31000000","9.97860000"],["2442.39000000","0.43990000"],["2442.45000000","4.98120000"],["2442.77000000","14.57120000"],["2443.08000000","12.08200000"],["2443.72000000","4.72630000"],["2447.99000000","0.07700000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962595,"s":"SOLUSDT","U":13653945617,"u":13653945640,"b":[["130.30000000","298.94500000"],["130.29000000","108.09900000"],["130.28000000","269.40900000"],["130.27000000","741.02200000"],["130.26000000","462.41900000"],["130.25000000","554.26400000"]],"a":[["130.31000000","353.67000000"],["130.32000000","335.67200000"],["130.35000000","635.91100000"],["130.36000000","289.87100000"],["130.37000000","440.63300000"],["139.80000000","522.76900000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962606,"s":"BNBUSDT","U":10326331680,"u":10326331681,"b":[],"a":[["522.90000000","60.82800000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962614,"s":"BTCUSDT","U":51157289525,"u":51157289535,"b":[["57704.00000000","0.19510000"],["57701.21000000","0.03465000"],["57699.01000000","0.03465000"],["57695.34000000","0.05977000"],["57695.33000000","0.00000000"],["57693.89000000","0.25344000"]],"a":[["57711.00000000","2.01962000"],["57719.98000000","0.03465000"],["57721.39000000","0.03465000"],["57725.54000000","0.00000000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962616,"s":"ETHUSDT","U":36258112476,"u":36258112494,"b":[["2441.76000000","3.45220000"],["2441.75000000","0.00430000"],["2441.68000000","0.00000000"],["2441.42000000","0.40950000"],["2441.37000000","0.00310000"],["2441.11000000","0.01020000"],["2403.72000000","2.04710000"],["2197.71000000","0.02500000"]],"a":[["2441.98000000","4.27510000"],["2441.99000000","0.79930000"],["2442.07000000","0.65000000"],["2442.14000000","3.25600000"],["2442.30000000","0.41260000"],["2442.84000000","12.28750000"],["2442.88000000","12.91100000"],["2442.93000000","9.58620000"],["2451.24000000","2.04050000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962695,"s":"SOLUSDT","U":13653945641,"u":13653945653,"b":[["130.30000000","370.41300000"],["130.29000000","86.70700000"],["130.28000000","274.39100000"]],"a":[["130.31000000","296.81900000"],["130.35000000","625.16500000"],["130.36000000","300.61700000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962714,"s":"BTCUSDT","U":51157289536,"u":51157289544,"b":[["57710.99000000","6.46410000"],["51939.00000000","0.00010000"]],"a":[["57714.86000000","0.25514000"],["57724.99000000","0.00000000"],["57735.80000000","1.02443000"],["60516.87000000","0.00000000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962716,"s":"ETHUSDT","U":36258112495,"u":36258112502,"b":[["2441.81000000","12.96700000"],["2441.64000000","8.48290000"],["2441.27000000","0.70050000"],["2439.31000000","15.29460000"]],"a":[["2441.97000000","0.00220000"],["2442.14000000","4.13450000"],["2442.31000000","9.10010000"],["2442.41000000","0.64050000"],["2442.84000000","0.00210000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962795,"s":"SOLUSDT","U":13653945654,"u":13653945657,"b":[["130.30000000","354.61300000"],["130.29000000","65.30900000"]],"a":[["130.31000000","354.96200000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962814,"s":"BTCUSDT","U":51157289545,"u":51157289557,"b":[["57693.89000000","0.00000000"],["57450.24000000","0.08740000"],["41100.36000000","0.67385000"]],"a":[["57713.99000000","0.46040000"],["57714.86000000","0.00000000"],["57734.60000000","0.25344000"],["57737.97000000","0.00000000"],["57762.72000000","0.43321000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962815,"s":"ETHUSDT","U":36258112503,"u":36258112525,"b":[["2441.90000000","77.17320000"],["2439.31000000","12.30210000"]],"a":[["2442.14000000","5.03890000"],["2442.21000000","8.19310000"],["2442.24000000","8.60490000"],["2442.26000000","14.31990000"],["2442.29000000","8.76790000"],["2442.31000000","8.19570000"],["2443.14000000","7.36280000"],["2450.90000000","0.40000000"],["2451.20000000","0.12000000"],["2451.24000000","0.00000000"],["2451.40000000","0.00600000"],["2451.60000000","0.00600000"],["2454.20000000","0.47400000"],["2454.40000000","0.00000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962895,"s":"SOLUSDT","U":13653945658,"u":13653945662,"b":[["130.30000000","354.61300000"],["130.29000000","52.43100000"]],"a":[]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962914,"s":"BTCUSDT","U":51157289558,"u":51157289561,"b":[["57684.03000000","0.63304000"]],"a":[["57713.68000000","0.12203000"],["57725.98000000","0.41602000"],["57776.99000000","0.71240000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382962915,"s":"ETHUSDT","U":36258112526,"u":36258112531,"b":[["2441.29000000","2.10790000"],["2440.80000000","6.33310000"]],"a":[["2442.14000000","1.78290000"],["2442.19000000","5.57810000"],["2443.12000000","7.03590000"],["2456.16000000","2.04370000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963006,"s":"BNBUSDT","U":10326331682,"u":10326331682,"b":[["521.20000000","37.26400000"]],"a":[]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963014,"s":"BTCUSDT","U":51157289562,"u":51157289569,"b":[["57710.99000000","6.46391000"],["57699.74000000","0.57182000"],["57694.99000000","0.25344000"],["57683.18000000","0.00000000"]],"a":[["57713.68000000","0.00000000"],["57718.01000000","0.12203000"],["57734.60000000","0.00000000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963015,"s":"ETHUSDT","U":36258112532,"u":36258112532,"b":[],"a":[["2478.72000000","2.05320000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963095,"s":"SOLUSDT","U":13653945663,"u":13653945673,"b":[["130.29000000","66.53800000"],["130.25000000","604.09300000"],["130.08000000","381.87600000"],["120.54000000","1.60000000"]],"a":[["130.31000000","368.60700000"],["130.37000000","469.79200000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963106,"s":"BNBUSDT","U":10326331683,"u":10326331683,"b":[["522.80000000","83.63500000"]],"a":[]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963114,"s":"BTCUSDT","U":51157289570,"u":51157289578,"b":[["57701.20000000","0.00000000"],["57700.76000000","0.34314000"],["57699.80000000","0.00050000"],["57684.03000000","0.00000000"],["51939.00000000","0.00010000"]],"a":[["57711.00000000","2.01628000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963115,"s":"ETHUSDT","U":36258112533,"u":36258112541,"b":[["2441.16000000","0.00430000"],["2440.65000000","0.09280000"],["881.00000000","0.43080000"],["880.94000000","0.01380000"],["880.71000000","0.02760000"],["880.54000000","0.05520000"],["880.12000000","0.03450000"],["879.91000000","0.05520000"]],"a":[["2443.75000000","0.25600000"],["2448.29000000","0.00000000"]]}}',
            '{"stream":"dotusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963173,"s":"DOTUSDT","U":8119446175,"u":8119446176,"b":[],"a":[["4.12100000","4272.21000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963195,"s":"SOLUSDT","U":13653945674,"u":13653945679,"b":[["130.29000000","106.09700000"],["130.18000000","568.84200000"],["130.12000000","400.15600000"],["130.11000000","813.30300000"],["130.03000000","661.02000000"],["128.93000000","1.77800000"]],"a":[]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963214,"s":"BTCUSDT","U":51157289579,"u":51157289586,"b":[["57710.99000000","6.46369000"],["57699.80000000","0.00000000"],["57693.62000000","1.05870000"],["57018.41000000","0.00061000"],["51939.00000000","0.00010000"],["41300.00000000","0.36913000"]],"a":[["57711.00000000","2.04447000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963215,"s":"ETHUSDT","U":36258112542,"u":36258112545,"b":[["2441.77000000","0.00000000"],["2199.99000000","0.09280000"]],"a":[["2444.86000000","0.35640000"],["2445.09000000","17.80860000"]]}}',
            '{"stream":"xrpusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963217,"s":"XRPUSDT","U":12011423563,"u":12011423563,"b":[["0.56050000","116159.00000000"]],"a":[]}}',
            '{"stream":"dotusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963273,"s":"DOTUSDT","U":8119446177,"u":8119446178,"b":[],"a":[["4.12000000","1742.80000000"],["4.12400000","13257.39000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963295,"s":"SOLUSDT","U":13653945680,"u":13653945692,"b":[["130.29000000","106.23100000"],["130.26000000","405.63200000"],["130.25000000","661.66300000"],["130.22000000","685.19800000"],["130.21000000","288.52900000"],["130.04000000","439.92300000"],["130.03000000","108.44600000"],["130.02000000","1121.43300000"]],"a":[["130.31000000","368.57800000"],["130.40000000","294.45900000"],["130.41000000","689.30900000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963314,"s":"BTCUSDT","U":51157289587,"u":51157289600,"b":[["57710.99000000","6.46407000"],["57700.01000000","0.03465000"],["57695.47000000","0.05943000"],["57695.34000000","0.00000000"],["57694.99000000","0.00000000"],["57693.89000000","0.25344000"]],"a":[["57711.00000000","2.04332000"],["57713.99000000","0.12167000"],["57740.33000000","0.02710000"],["58100.66000000","0.00000000"],["59020.65000000","0.03389000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963315,"s":"ETHUSDT","U":36258112546,"u":36258112556,"b":[["2441.90000000","77.18450000"],["2197.71000000","0.03000000"],["2196.00000000","72.40440000"]],"a":[["2441.91000000","20.73030000"],["2441.93000000","4.30090000"],["2441.97000000","0.00000000"],["2442.08000000","0.00000000"],["2442.18000000","0.40950000"],["2442.25000000","0.00430000"],["2445.05000000","9.96980000"]]}}',
            '{"stream":"xrpusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963317,"s":"XRPUSDT","U":12011423564,"u":12011423565,"b":[["0.56050000","116005.00000000"],["0.55240000","2565.00000000"]],"a":[]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963395,"s":"SOLUSDT","U":13653945693,"u":13653945701,"b":[["130.04000000","312.26200000"],["130.03000000","93.81000000"]],"a":[["130.32000000","335.72300000"],["130.39000000","225.46200000"],["130.40000000","120.18800000"],["130.43000000","551.36800000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963414,"s":"BTCUSDT","U":51157289601,"u":51157289605,"b":[["56445.63000000","0.00014000"]],"a":[["57711.00000000","2.01513000"],["57731.00000000","0.14973000"],["57740.33000000","0.00000000"],["58099.29000000","0.02710000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963415,"s":"ETHUSDT","U":36258112557,"u":36258112567,"b":[["2441.29000000","2.81170000"],["2441.27000000","0.00000000"],["2439.31000000","15.29460000"],["2199.99000000","0.08780000"]],"a":[["2442.07000000","0.01020000"],["2442.23000000","0.40950000"],["2447.60000000","0.73600000"],["2447.80000000","0.12000000"],["2448.00000000","0.65440000"],["2448.20000000","9.58200000"],["2451.20000000","0.09000000"],["2454.20000000","0.39600000"],["2454.60000000","0.02910000"]]}}',
            '{"stream":"xrpusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963417,"s":"XRPUSDT","U":12011423566,"u":12011423566,"b":[["0.56040000","142661.00000000"]],"a":[]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963495,"s":"SOLUSDT","U":13653945702,"u":13653945704,"b":[["130.05000000","578.33500000"]],"a":[["130.41000000","746.87900000"],["130.43000000","493.80200000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963514,"s":"BTCUSDT","U":51157289606,"u":51157289618,"b":[["57710.99000000","6.46388000"],["57710.95000000","0.00020000"],["57701.58000000","0.00000000"],["57695.61000000","0.06040000"],["57695.47000000","0.00000000"],["57694.85000000","0.07294000"],["33000.00000000","16.17838000"]],"a":[["57713.68000000","0.12203000"],["57713.99000000","0.36475000"],["57727.54000000","0.34368000"],["57805.41000000","0.01340000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963515,"s":"ETHUSDT","U":36258112568,"u":36258112571,"b":[["2436.36000000","3.86420000"],["2197.71000000","0.03500000"]],"a":[["2442.18000000","1.20950000"],["2448.99000000","0.00760000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963595,"s":"SOLUSDT","U":13653945705,"u":13653945707,"b":[],"a":[["130.39000000","375.70600000"],["130.43000000","240.86100000"],["130.54000000","1524.01800000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963614,"s":"BTCUSDT","U":51157289619,"u":51157289626,"b":[["57701.21000000","0.00000000"],["57699.71000000","0.00050000"],["57592.83000000","0.08474000"],["57515.98000000","0.00000000"]],"a":[["57732.99000000","0.06015000"],["57733.55000000","0.00000000"],["57800.00000000","4.83717000"],["59029.50000000","0.08474000"],["61950.00000000","0.50226000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963615,"s":"ETHUSDT","U":36258112572,"u":36258112580,"b":[["2440.85000000","5.84730000"],["2440.62000000","0.56700000"],["2439.31000000","12.30210000"],["2429.06000000","0.00370000"]],"a":[["2442.60000000","2.47110000"],["2442.68000000","7.32580000"],["2497.32000000","0.80130000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963695,"s":"SOLUSDT","U":13653945708,"u":13653945737,"b":[["130.30000000","109.24300000"],["130.29000000","74.63500000"],["130.28000000","268.73600000"],["130.27000000","733.55000000"],["130.25000000","604.09300000"],["130.24000000","698.99700000"],["130.22000000","684.80200000"],["130.16000000","297.12200000"],["117.09000000","4.08400000"]],"a":[["130.31000000","556.67400000"],["130.32000000","297.34500000"],["130.33000000","125.85700000"],["130.35000000","635.91100000"],["130.36000000","289.87100000"],["131.53000000","10.66200000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963706,"s":"BNBUSDT","U":10326331684,"u":10326331685,"b":[["522.80000000","82.87000000"]],"a":[["522.90000000","65.25300000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963714,"s":"BTCUSDT","U":51157289627,"u":51157289683,"b":[["57710.99000000","4.96113000"],["57708.18000000","0.00000000"],["57705.52000000","0.00000000"],["57700.99000000","0.07456000"],["57700.77000000","0.03508000"],["57695.61000000","0.00000000"],["57693.89000000","0.00000000"],["57692.07000000","0.00000000"],["57686.28000000","0.00000000"],["57681.11000000","0.00000000"],["57678.11000000","0.06466000"],["57675.02000000","0.86665000"],["57674.89000000","0.00000000"],["57647.27000000","0.00018000"],["57622.50000000","0.08097000"],["57620.78000000","0.08097000"],["57619.57000000","0.08097000"],["57617.74000000","0.00000000"],["57613.60000000","0.00000000"],["57610.99000000","0.01000000"],["57592.83000000","0.00000000"],["56449.81000000","0.00112000"],["20844.00000000","0.00000000"],["20798.18000000","0.00203000"],["20788.20000000","0.00174000"],["20787.01000000","0.00058000"],["20778.48000000","0.00145000"],["20766.24000000","0.00029000"]],"a":[["57711.00000000","4.17953000"],["57711.36000000","0.34663000"],["57715.09000000","0.43321000"],["57718.94000000","0.34659000"],["57719.97000000","0.50884000"],["57722.97000000","1.18729000"],["57732.85000000","0.06025000"],["57732.99000000","0.00000000"],["57743.72000000","0.05181000"],["57753.29000000","0.00000000"],["57767.67000000","0.00000000"],["57782.95000000","3.97290000"],["57808.58000000","0.00000000"],["57811.00000000","0.03770000"],["57827.54000000","0.08097000"],["58034.09000000","0.08635000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963715,"s":"ETHUSDT","U":36258112581,"u":36258112614,"b":[["2441.90000000","67.89070000"],["2441.86000000","4.09620000"],["2441.81000000","9.76700000"],["2441.80000000","3.85000000"],["2441.57000000","0.00000000"],["2441.41000000","1.21050000"],["2440.96000000","0.25070000"],["2440.72000000","9.97740000"],["2440.26000000","14.63420000"]],"a":[["2441.91000000","26.37520000"],["2442.15000000","3.27830000"],["2442.25000000","0.40330000"],["2442.31000000","9.64920000"],["2442.56000000","0.78010000"],["2442.63000000","10.33130000"],["2442.74000000","7.98310000"],["2442.83000000","0.70000000"],["2442.88000000","12.21160000"],["2442.90000000","0.00000000"],["2444.59000000","8.26460000"],["2456.16000000","0.00310000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963795,"s":"SOLUSDT","U":13653945738,"u":13653945778,"b":[["130.30000000","90.05200000"],["130.29000000","40.20300000"],["130.28000000","307.12300000"],["130.27000000","666.49900000"],["130.26000000","376.12300000"],["130.25000000","680.88400000"],["130.24000000","641.42400000"],["130.23000000","542.42600000"],["130.22000000","592.67900000"],["130.21000000","303.84100000"],["130.17000000","406.75200000"],["130.15000000","475.53200000"],["130.01000000","285.84100000"],["129.24000000","212.62800000"],["129.19000000","9.12600000"],["117.27000000","5.05600000"],["46.97000000","2.68800000"],["46.95000000","2.68800000"],["46.91000000","1.02400000"],["46.85000000","1.88900000"]],"a":[["130.32000000","301.34500000"],["130.33000000","202.60600000"],["130.34000000","222.92400000"],["130.35000000","625.16500000"],["130.36000000","293.87100000"],["130.38000000","533.47200000"],["130.45000000","322.05800000"],["130.50000000","297.01300000"],["130.55000000","180.13200000"],["130.56000000","16.19200000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963814,"s":"BTCUSDT","U":51157289684,"u":51157289724,"b":[["57710.99000000","4.26488000"],["57710.84000000","0.75178000"],["57710.00000000","0.17879000"],["57708.00000000","0.19196000"],["57705.05000000","0.00000000"],["57702.85000000","0.00677000"],["57699.71000000","0.00000000"],["57694.85000000","0.00000000"],["57693.47000000","0.06012000"],["57687.17000000","0.07294000"]],"a":[["57711.00000000","4.71994000"],["57713.68000000","0.00000000"],["57713.99000000","0.21489000"],["57715.09000000","0.43321000"],["57717.99000000","0.20798000"],["57718.01000000","0.00000000"],["57718.06000000","0.00000000"],["57718.26000000","0.00000000"],["57719.96000000","0.63467000"],["57719.98000000","0.00000000"],["57719.99000000","0.00000000"],["57722.34000000","0.00000000"],["57722.96000000","0.03474000"],["57723.97000000","0.00000000"],["57724.61000000","0.00000000"],["57731.80000000","0.06047000"],["57732.85000000","0.00000000"],["57751.03000000","0.00000000"],["57783.89000000","0.00000000"],["57838.99000000","0.01206000"],["58034.09000000","0.00000000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963815,"s":"ETHUSDT","U":36258112615,"u":36258112654,"b":[["2441.90000000","38.38370000"],["2441.86000000","0.00000000"],["2441.82000000","5.86020000"],["2441.81000000","8.69800000"],["2441.67000000","4.30210000"],["2441.29000000","2.06700000"],["2441.18000000","7.95660000"],["2441.01000000","0.04090000"],["2440.62000000","0.01020000"],["2440.36000000","0.55680000"]],"a":[["2441.91000000","45.53840000"],["2441.95000000","1.78290000"],["2441.98000000","3.34770000"],["2442.09000000","1.98870000"],["2442.12000000","0.00210000"],["2442.14000000","0.00000000"],["2442.17000000","1.04140000"],["2442.30000000","15.41260000"],["2442.45000000","2.99250000"],["2442.67000000","0.81930000"],["2442.68000000","4.34220000"],["2442.73000000","0.94590000"],["2442.74000000","8.75190000"],["2442.83000000","0.00000000"],["2442.99000000","7.98000000"],["2443.06000000","0.00000000"],["2497.32000000","0.00000000"]]}}',
            '{"stream":"xrpusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963817,"s":"XRPUSDT","U":12011423567,"u":12011423567,"b":[["0.54340000","3186.00000000"]],"a":[]}}',
            '{"stream":"dotusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963873,"s":"DOTUSDT","U":8119446179,"u":8119446184,"b":[["1.49200000","96.72000000"],["1.49100000","32.24000000"],["1.49000000","12.09000000"]],"a":[]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963895,"s":"SOLUSDT","U":13653945779,"u":13653945789,"b":[["130.30000000","90.05200000"],["130.29000000","40.16200000"],["130.28000000","307.12300000"]],"a":[["130.32000000","404.19000000"],["130.33000000","276.61600000"],["130.34000000","189.17400000"],["150.00000000","6838.34800000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963914,"s":"BTCUSDT","U":51157289725,"u":51157289743,"b":[["57710.99000000","2.91497000"],["57710.84000000","0.00000000"],["57709.12000000","1.13585000"],["57706.00000000","0.19590000"],["57705.60000000","0.00000000"],["57698.95000000","0.01733000"]],"a":[["57711.00000000","4.72034000"],["57713.68000000","0.12203000"],["57715.09000000","0.00000000"],["57719.95000000","0.41552000"],["57719.96000000","0.32933000"],["57719.97000000","0.00000000"],["57720.89000000","0.00021000"],["57730.92000000","0.06159000"],["57731.80000000","0.00000000"],["57745.63000000","0.00038000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963915,"s":"ETHUSDT","U":36258112655,"u":36258112673,"b":[["2441.90000000","38.36600000"],["2441.82000000","0.00000000"],["2441.67000000","1.02380000"],["2441.18000000","4.67830000"],["2441.15000000","0.69870000"],["2440.65000000","0.01100000"]],"a":[["2441.91000000","48.70250000"],["2442.08000000","1.19350000"],["2442.21000000","0.00000000"],["2442.24000000","0.41180000"],["2442.26000000","8.48210000"],["2442.29000000","0.57480000"],["2442.30000000","0.41260000"],["2444.20000000","0.34130000"],["2444.40000000","1.11550000"],["2444.60000000","0.48670000"],["2444.80000000","1.98680000"],["2447.80000000","0.09000000"],["2451.20000000","0.00000000"],["2451.40000000","0.00000000"],["2451.60000000","0.00000000"]]}}',
            '{"stream":"dotusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963973,"s":"DOTUSDT","U":8119446185,"u":8119446186,"b":[],"a":[["4.12200000","3675.41000000"],["4.12400000","13985.76000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382963995,"s":"SOLUSDT","U":13653945790,"u":13653945795,"b":[["130.30000000","90.05200000"],["130.29000000","40.16200000"],["130.28000000","307.12300000"]],"a":[]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964006,"s":"BNBUSDT","U":10326331686,"u":10326331686,"b":[["522.80000000","82.82300000"]],"a":[]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964014,"s":"BTCUSDT","U":51157289744,"u":51157289778,"b":[["57710.99000000","2.85904000"],["57703.70000000","0.00040000"],["57702.85000000","0.00000000"],["57700.06000000","0.00677000"],["57699.36000000","0.00050000"],["57694.22000000","0.63530000"],["57693.47000000","0.00000000"],["57666.73000000","1.18729000"],["57662.48000000","0.08666000"],["57450.24000000","0.00000000"],["53094.11000000","0.00019000"],["51939.00000000","0.00010000"],["41100.36000000","0.00000000"]],"a":[["57711.00000000","4.72044000"],["57713.99000000","0.12101000"],["57716.19000000","0.00000000"],["57718.01000000","0.12203000"],["57718.20000000","0.00000000"],["57719.94000000","0.28397000"],["57719.95000000","0.03465000"],["57722.98000000","0.00000000"],["57729.55000000","0.05945000"],["57730.92000000","0.00100000"],["57737.49000000","0.06008000"],["57737.95000000","0.15295000"],["57750.45000000","0.00000000"],["57782.94000000","2.38377000"],["57802.06000000","0.02600000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964015,"s":"ETHUSDT","U":36258112674,"u":36258112682,"b":[["2441.90000000","38.38350000"],["2440.40000000","7.42550000"]],"a":[["2442.16000000","3.80650000"],["2442.19000000","0.41460000"],["2442.26000000","6.12680000"],["2442.72000000","0.70020000"],["2442.73000000","0.24960000"],["2444.37000000","0.67000000"],["2448.08000000","0.05060000"]]}}',
            '{"stream":"dotusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964073,"s":"DOTUSDT","U":8119446187,"u":8119446187,"b":[["4.11200000","3748.30000000"]],"a":[]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964095,"s":"SOLUSDT","U":13653945796,"u":13653945797,"b":[["130.29000000","29.41500000"],["130.28000000","317.87000000"]],"a":[["140.14000000","3.11800000"]]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964106,"s":"BNBUSDT","U":10326331687,"u":10326331687,"b":[],"a":[["522.90000000","65.20600000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964114,"s":"BTCUSDT","U":51157289779,"u":51157289801,"b":[["57710.99000000","2.85883000"],["57709.36000000","0.00000000"],["57707.14000000","0.00000000"],["57705.04000000","0.00000000"],["57704.19000000","0.13286000"],["57702.92000000","0.00021000"],["57699.36000000","0.00000000"],["57691.00000000","0.08033000"],["57678.11000000","0.00000000"],["57450.24000000","0.08740000"],["41100.36000000","0.67385000"]],"a":[["57711.00000000","5.46720000"],["57713.30000000","0.00626000"],["57716.68000000","0.01733000"],["57718.17000000","0.13286000"],["57719.93000000","0.17042000"],["57719.94000000","0.03465000"],["57721.65000000","0.00000000"],["57723.27000000","0.00000000"],["57729.20000000","0.05965000"],["57729.55000000","0.00000000"],["57733.27000000","0.13286000"]]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964115,"s":"ETHUSDT","U":36258112683,"u":36258112688,"b":[["2441.76000000","3.45430000"]],"a":[["2442.18000000","0.80000000"],["2442.25000000","0.81280000"],["2442.70000000","12.89310000"],["2442.72000000","0.00210000"],["2444.36000000","0.00800000"]]}}',
            '{"stream":"xrpusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964117,"s":"XRPUSDT","U":12011423568,"u":12011423571,"b":[],"a":[["0.56070000","160774.00000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964195,"s":"SOLUSDT","U":13653945798,"u":13653945806,"b":[["130.30000000","90.05200000"],["130.29000000","29.41500000"],["130.28000000","317.87000000"],["130.27000000","666.45800000"],["130.07000000","189.98200000"]],"a":[["130.40000000","120.28000000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964214,"s":"BTCUSDT","U":51157289802,"u":51157289803,"b":[["57671.60000000","0.06466000"],["57500.00000000","55.32643000"]],"a":[]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964215,"s":"ETHUSDT","U":36258112689,"u":36258112693,"b":[["2441.18000000","7.95660000"],["2440.00000000","4.69590000"],["2437.76000000","6.09630000"],["2436.09000000","13.22140000"],["2431.47000000","0.00000000"]],"a":[["2442.15000000","6.55660000"],["2442.39000000","0.42990000"],["2447.27000000","0.01000000"]]}}',
            '{"stream":"dotusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964273,"s":"DOTUSDT","U":8119446188,"u":8119446188,"b":[],"a":[["4.12100000","4393.58000000"]]}}',
            '{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964295,"s":"SOLUSDT","U":13653945807,"u":13653945811,"b":[["130.30000000","90.05200000"],["130.29000000","29.41500000"],["117.09000000","3.99000000"]],"a":[]}}',
            '{"stream":"bnbusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964306,"s":"BNBUSDT","U":10326331688,"u":10326331688,"b":[["521.00000000","158.83600000"]],"a":[]}}',
            '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964315,"s":"ETHUSDT","U":36258112694,"u":36258112697,"b":[],"a":[["2442.14000000","3.93480000"],["2442.16000000","0.00000000"],["2442.92000000","1.87390000"],["2443.21000000","1.25720000"]]}}',
            '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1725382964315,"s":"BTCUSDT","U":51157289804,"u":51157289811,"b":[["57709.00000000","0.02926000"],["57689.69000000","0.25344000"]],"a":[["57711.00000000","5.56720000"],["57715.09000000","0.00000000"],["57735.80000000","0.00000000"],["57750.59000000","0.00000000"],["57798.31000000","0.00100000"]]}}',
        ]

        list_with_expected_messages = []

        for message in sample_message_queue:
            message_dict = json.loads(message)
            message_dict['data'].pop('E', None)
            corrected_message = json.dumps(message_dict, separators=(',', ':'))
            list_with_expected_messages.append(corrected_message)

        list_with_messages = []
        for message in sample_message_queue:
            corrected_message = DifferenceDepthQueue._remove_event_timestamp(message)
            list_with_messages.append(corrected_message)

        assert list_with_messages == list_with_expected_messages

    #_do_last_two_throws_match,
    #
    def test_given_comparing_two_throws_when_throws_are_equal_then_method_returns_true(self):

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "AVAXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']
        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        _old_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_3 = '''            
            {
                "stream": "avaxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _new_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_3 = '''            
            {
                "stream": "avaxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_2)
        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_3)
        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_1)
        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_2)
        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_3)

        difference_depth_queue._two_last_throws = {
            old_stream_listener_id.id:
                deque(
                    [
                        DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_1),
                        DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_2),
                        DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_3),
                    ],
                maxlen=old_stream_listener_id.pairs_amount),
            new_stream_listener_id.id:
                deque(
                    [
                        DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_1),
                        DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_2),
                        DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_3),
                    ],
                    maxlen=new_stream_listener_id.pairs_amount)
        }

        two_last_throws_comparison_structure = difference_depth_queue._two_last_throws

        do_they_match = DifferenceDepthQueue._do_last_two_throws_match(old_stream_listener_id.pairs_amount, two_last_throws_comparison_structure)
        assert do_they_match is True
        do_they_match = DifferenceDepthQueue._do_last_two_throws_match(new_stream_listener_id.pairs_amount, two_last_throws_comparison_structure)
        assert do_they_match is True
        DifferenceDepthQueue.clear_instances()

    def test_given_comparing_two_throws_when_throws_are_not_equal_then_method_returns_false(self):
        """difference lays in a _old_listener_message_1 / _new_listener_message_1"""

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "AVAXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        _old_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_3 = '''            
            {
                "stream": "avaxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _new_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_3 = '''            
            {
                "stream": "avaxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_2)
        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_3)
        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_1)
        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_2)
        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_3)

        difference_depth_queue._two_last_throws = {
            old_stream_listener_id.id:
                deque(
                    [
                        DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_1),
                        DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_2),
                        DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_3),
                    ],
                    maxlen=old_stream_listener_id.pairs_amount),
            new_stream_listener_id.id:
                deque(
                    [
                        DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_1),
                        DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_2),
                        DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_3),
                    ],
                    maxlen=new_stream_listener_id.pairs_amount)
        }

        two_last_throws_comparison_structure = difference_depth_queue._two_last_throws

        do_they_match = DifferenceDepthQueue._do_last_two_throws_match(old_stream_listener_id.pairs_amount,
                                                                       two_last_throws_comparison_structure)
        assert do_they_match is False
        do_they_match = DifferenceDepthQueue._do_last_two_throws_match(new_stream_listener_id.pairs_amount,
                                                                       two_last_throws_comparison_structure)
        assert do_they_match is False
        DifferenceDepthQueue.clear_instances()

    def test_given_comparing_two_throws_when_throws_are_not_equal_because_one_asset_is_duplicated_then_method_returns_false(self):
        """difference lays in a _old_listener_message_1 / _new_listener_message_1"""

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "AVAXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        _old_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_3 = '''            
            {
                "stream": "avaxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _new_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_3 = '''            
            {
                "stream": "avaxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "DOTUSDT",
                    "U": 7871863948,
                    "u": 7871863948,
                    "b": [
                        [
                            "2.32300000",
                            "1811.61000000"
                        ]
                    ],
                    "a": []
                }
            }
        '''

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_2)
        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_3)
        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_1)
        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_2)
        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_3)

        difference_depth_queue._two_last_throws = {
            old_stream_listener_id.id:
                deque(
                    [
                        DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_1),
                        DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_2),
                        DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_3),
                    ],
                    maxlen=old_stream_listener_id.pairs_amount),
            new_stream_listener_id.id:
                deque(
                    [
                        DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_1),
                        DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_2),
                        DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_3),
                    ],
                    maxlen=new_stream_listener_id.pairs_amount)
        }

        two_last_throws_comparison_structure = difference_depth_queue._two_last_throws

        do_they_match = DifferenceDepthQueue._do_last_two_throws_match(old_stream_listener_id.pairs_amount,
                                                                       two_last_throws_comparison_structure)
        assert do_they_match is False
        do_they_match = DifferenceDepthQueue._do_last_two_throws_match(new_stream_listener_id.pairs_amount,
                                                                       two_last_throws_comparison_structure)
        assert do_they_match is False
        DifferenceDepthQueue.clear_instances()

    def test_given_comparing_two_throws_when_throws_are_equal_but_one_asset_is_duplicated_then_method_returns_false(self):

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "AVAXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']
        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        _old_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_3 = '''
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869316,
                    "s": "DOTUSDT",
                    "U": 7871863948,
                    "u": 7871863948,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ]
                    ],
                    "a": []
                }
            }
        '''

        _new_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_3 = '''
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869316,
                    "s": "DOTUSDT",
                    "U": 7871863948,
                    "u": 7871863948,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ]
                    ],
                    "a": []
                }
            }
        '''

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_2)
        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_3)
        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_1)
        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_2)
        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_3)

        difference_depth_queue._two_last_throws = {
            old_stream_listener_id.id:
                deque(
                    [
                        DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_1),
                        DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_2),
                        DifferenceDepthQueue._remove_event_timestamp(_old_listener_message_3),
                    ],
                maxlen=old_stream_listener_id.pairs_amount),
            new_stream_listener_id.id:
                deque(
                    [
                        DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_1),
                        DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_2),
                        DifferenceDepthQueue._remove_event_timestamp(_new_listener_message_3),
                    ],
                    maxlen=new_stream_listener_id.pairs_amount)
        }

        two_last_throws_comparison_structure = difference_depth_queue._two_last_throws

        do_they_match = DifferenceDepthQueue._do_last_two_throws_match(old_stream_listener_id.pairs_amount, two_last_throws_comparison_structure)
        assert do_they_match is False
        do_they_match = DifferenceDepthQueue._do_last_two_throws_match(new_stream_listener_id.pairs_amount, two_last_throws_comparison_structure)
        assert do_they_match is False
        DifferenceDepthQueue.clear_instances()


    # set_new_stream_id_as_currently_accepted
    #

    ...

    # get, get_nowait, clear, empty, qsize
    #
    def test_given_putting_messages_whilst_changing_and_whilst_usual_working_mode_when_get_nowait_from_queue_then_order_and_amount_of_queue_elements_is_ok(self):
        """throws are set to be equal each after other"""
        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']
        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        third_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id

        _first_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _first_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _first_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _second_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _first_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_1)
        _first_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_2)
        _first_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_3)
        _second_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_1)
        _second_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_2)
        _second_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_3)

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        _second_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _third_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _third_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _third_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _second_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_4)
        _second_listener_message_5 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_5)
        _second_listener_message_6 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_6)
        _third_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_4)
        _third_listener_message_5 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_5)
        _third_listener_message_6 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_6)

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get_nowait())

        expected_list = [
            (_first_listener_message_1, mocked_timestamp_of_receive),
            (_first_listener_message_2, mocked_timestamp_of_receive),
            (_first_listener_message_3, mocked_timestamp_of_receive),
            (_second_listener_message_4, mocked_timestamp_of_receive),
            (_second_listener_message_5, mocked_timestamp_of_receive),
            (_second_listener_message_6, mocked_timestamp_of_receive)
        ]

        assert difference_depth_queue_content_list == expected_list

        DifferenceDepthQueue.clear_instances()

    def test_getting_from_queue_when_method_invocation_then_last_element_is_returned(self):
        """throws are set to be equal to cause change each after other"""

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        third_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id

        _first_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _first_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _first_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _second_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _first_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_1)
        _first_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_2)
        _first_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_3)
        _second_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_1)
        _second_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_2)
        _second_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_3)

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == second_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        _second_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _third_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _third_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _third_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _second_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_4)
        _second_listener_message_5 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_5)
        _second_listener_message_6 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_6)
        _third_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_4)
        _third_listener_message_5 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_5)
        _third_listener_message_6 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_6)

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == third_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get())

        assert difference_depth_queue_content_list[0] == (_first_listener_message_1, mocked_timestamp_of_receive)
        DifferenceDepthQueue.clear_instances()

    def test_getting_with_no_wait_from_queue_when_method_invocation_then_last_element_is_returned(self):

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)
        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        third_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id

        _first_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _first_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _first_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _second_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _first_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_1)
        _first_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_2)
        _first_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_3)
        _second_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_1)
        _second_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_2)
        _second_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_3)


        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == second_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        _second_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _third_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _third_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _third_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _second_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_4)
        _second_listener_message_5 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_5)
        _second_listener_message_6 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_6)
        _third_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_4)
        _third_listener_message_5 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_5)
        _third_listener_message_6 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_6)

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == third_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get_nowait())

        assert difference_depth_queue_content_list[0] == (_first_listener_message_1, mocked_timestamp_of_receive)
        DifferenceDepthQueue.clear_instances()

    def test_given_clearing_difference_depth_queue_when_invocation_then_qsize_equals_zero(self):

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)
        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        third_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id

        _first_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _first_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _first_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _second_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _first_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_1)
        _first_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_2)
        _first_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_3)
        _second_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_1)
        _second_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_2)
        _second_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_3)

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == second_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        _second_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _third_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _third_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _third_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _second_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_4)
        _second_listener_message_5 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_5)
        _second_listener_message_6 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_6)
        _third_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_4)
        _third_listener_message_5 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_5)
        _third_listener_message_6 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_6)

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.clear()

        assert difference_depth_queue.qsize() == 0
        DifferenceDepthQueue.clear_instances()

    def test_given_checking_empty_when_method_invocation_then_result_is_ok(self):

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)
        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        third_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id

        _first_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _first_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _first_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _second_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _first_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_1)
        _first_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_2)
        _first_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_3)
        _second_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_1)
        _second_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_2)
        _second_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_3)

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == second_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        _second_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _third_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _third_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _third_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _second_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_4)
        _second_listener_message_5 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_5)
        _second_listener_message_6 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_6)
        _third_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_4)
        _third_listener_message_5 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_5)
        _third_listener_message_6 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_6)

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.empty() is False

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get())

        difference_depth_queue.clear()

        assert difference_depth_queue.qsize() == 0
        assert difference_depth_queue.empty() is True

        DifferenceDepthQueue.clear_instances()

    def test_checking_size_when_method_invocation_then_result_is_ok(self):
        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)
        pairs = config['instruments']['spot']

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        third_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id

        _first_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _first_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _first_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _second_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _first_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_1)
        _first_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_2)
        _first_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_3)
        _second_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_1)
        _second_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_2)
        _second_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_3)

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == second_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        _second_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _second_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _third_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _third_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _third_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _second_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_4)
        _second_listener_message_5 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_5)
        _second_listener_message_6 = format_message_string_that_is_pretty_to_binance_string_format(_second_listener_message_6)
        _third_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_4)
        _third_listener_message_5 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_5)
        _third_listener_message_6 = format_message_string_that_is_pretty_to_binance_string_format(_third_listener_message_6)

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.qsize() == 6
        difference_depth_queue.clear_instances()

    # benchmark
    #
    @pytest.mark.skip
    def test_comparison_algorithm_benchmark(self):
        total_execution_time = 0
        number_of_runs = 1000

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']

        _old_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [
    
                    ]
                }
            }
        '''

        _new_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [
    
                    ]
                }
            }
        '''

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_2)
        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_3)
        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_1)
        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_2)
        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_3)

        for _ in range(number_of_runs):
            difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

            old_stream_listener_id = StreamId(pairs=pairs)
            time.sleep(0.01)
            new_stream_listener_id = StreamId(pairs=pairs)

            mocked_timestamp_of_receive = 2115

            difference_depth_queue.currently_accepted_stream_id = old_stream_listener_id.id

            start_time = time.perf_counter()

            difference_depth_queue.put_queue_message(
                stream_listener_id=old_stream_listener_id,
                message=_old_listener_message_1,
                timestamp_of_receive=mocked_timestamp_of_receive
            )
            difference_depth_queue.put_queue_message(
                stream_listener_id=old_stream_listener_id,
                message=_old_listener_message_2,
                timestamp_of_receive=mocked_timestamp_of_receive
            )
            difference_depth_queue.put_queue_message(
                stream_listener_id=old_stream_listener_id,
                message=_old_listener_message_3,
                timestamp_of_receive=mocked_timestamp_of_receive
            )
            difference_depth_queue.put_queue_message(
                stream_listener_id=new_stream_listener_id,
                message=_new_listener_message_1,
                timestamp_of_receive=mocked_timestamp_of_receive
            )
            difference_depth_queue.put_queue_message(
                stream_listener_id=new_stream_listener_id,
                message=_new_listener_message_2,
                timestamp_of_receive=mocked_timestamp_of_receive
            )
            difference_depth_queue.put_queue_message(
                stream_listener_id=new_stream_listener_id,
                message=_new_listener_message_3,
                timestamp_of_receive=mocked_timestamp_of_receive
            )

            end_time = time.perf_counter()

            execution_time = end_time - start_time
            total_execution_time += execution_time

            while not difference_depth_queue.empty():
                difference_depth_queue.get_nowait()

            DifferenceDepthQueue.clear_instances()

            del difference_depth_queue
            del old_stream_listener_id
            del new_stream_listener_id

        average_execution_time = total_execution_time / number_of_runs

        print(f"mean of {number_of_runs} runs: {average_execution_time} seconds")
