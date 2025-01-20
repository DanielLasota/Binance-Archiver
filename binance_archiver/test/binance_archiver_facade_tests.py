import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from queue import Queue
from unittest.mock import patch, MagicMock
import pytest

from ..abstract_base_classes import Observer
from ..exceptions import (
    BadStorageProviderParameters,
    BadConfigException,
    ClassInstancesAmountLimitException,
    WebSocketLifeTimeException
)

from ..listener_facade import (
    launch_data_listener,
    ListenerFacade
)
from ..data_sink_facade import DataSinkFacade
from .. import launch_data_sink
from ..snapshot_manager import SnapshotStrategy, DataSinkSnapshotStrategy, ListenerSnapshotStrategy, SnapshotManager
from ..listener_observer_updater import ListenerObserverUpdater
from ..stream_data_save_and_sender import StreamDataSaverAndSender
from ..queue_pool import QueuePoolListener, QueuePoolDataSink
from ..stream_service import StreamService
from ..commandline_interface import CommandLineInterface
from ..timestamps_generator import TimestampsGenerator
from ..fastapi_manager import FastAPIManager

from ..setup_logger import setup_logger
from ..difference_depth_queue import DifferenceDepthQueue
from ..stream_id import StreamId
from ..trade_queue import TradeQueue
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType


class TestArchiverFacade:

    def test_init(self):
        assert True


    class TestDataSinkFacade:

        def test_given_send_zip_to_blob_is_true_and_container_name_is_bad_then_exception_is_thrown(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": True
            }

            azure_blob_parameters_with_key = 'DefaultEndpointsProtocol=...'

            with pytest.raises(BadStorageProviderParameters) as excinfo:
                data_sink_facade = launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                )

            assert str(excinfo.value) == "At least one of the Azure or Backblaze parameter sets must be fully specified."

        def test_given_archiver_facade_when_init_then_global_shutdown_flag_is_false(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 60,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            logger = setup_logger()
            archiver_facade = DataSinkFacade(config=config, logger=logger)

            assert not archiver_facade.global_shutdown_flag.is_set()

            del archiver_facade

            TradeQueue.clear_instances()
            DifferenceDepthQueue.clear_instances()

        def test_given_archiver_facade_when_init_then_queues_are_set_properly(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 60,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            logger = setup_logger()
            archiver_facade = DataSinkFacade(config=config, logger=logger)

            queue_pool = archiver_facade.queue_pool

            assert isinstance(queue_pool.spot_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(queue_pool.usd_m_futures_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(queue_pool.coin_m_orderbook_stream_message_queue, DifferenceDepthQueue)

            assert isinstance(queue_pool.spot_trade_stream_message_queue, TradeQueue)
            assert isinstance(queue_pool.usd_m_futures_trade_stream_message_queue, TradeQueue)
            assert isinstance(queue_pool.coin_m_trade_stream_message_queue, TradeQueue)

            assert len(TradeQueue._instances) == 3
            assert len(DifferenceDepthQueue._instances) == 3

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_archiver_facade_run_call_when_threads_invoked_then_correct_threads_are_started(self):

            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            data_sink_facade = launch_data_sink(config)

            time.sleep(3)

            num_markets = len(config["instruments"])

            expected_stream_service_threads = num_markets * 2
            expected_stream_writer_threads = num_markets * 2
            expected_snapshot_daemon_threads = num_markets

            total_expected_threads = (expected_stream_service_threads + expected_stream_writer_threads
                                      + expected_snapshot_daemon_threads)

            active_threads = threading.enumerate()
            daemon_threads = [thread for thread in active_threads if 'stream_service' in thread.name or
                              'stream_writer' in thread.name or 'snapshot_daemon'
                              in thread.name]

            thread_names = [thread.name for thread in daemon_threads]

            for market in ["SPOT", "USD_M_FUTURES", "COIN_M_FUTURES"]:
                assert f'stream_service: market: {Market[market]}, stream_type: {StreamType.DIFFERENCE_DEPTH_STREAM}' in thread_names
                assert f'stream_service: market: {Market[market]}, stream_type: {StreamType.TRADE_STREAM}' in thread_names
                assert f'stream_writer: market: {Market[market]}, stream_type: {StreamType.DIFFERENCE_DEPTH_STREAM}' in thread_names
                assert f'stream_writer: market: {Market[market]}, stream_type: {StreamType.TRADE_STREAM}' in thread_names
                assert f'snapshot_daemon: market: {Market[market]}' in thread_names

            assert len(daemon_threads) == total_expected_threads

            data_sink_facade.shutdown()

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_archiver_facade_initialization_in_listener_mode(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT"]
                },
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }
            logger = setup_logger()

            data_sink_facade = DataSinkFacade(
                config=config,
                logger=logger
            )

            assert isinstance(data_sink_facade.queue_pool, QueuePoolDataSink)
            assert isinstance(data_sink_facade.stream_service, StreamService)
            assert isinstance(data_sink_facade.command_line_interface, CommandLineInterface)
            assert isinstance(data_sink_facade.fast_api_manager, FastAPIManager)
            assert isinstance(data_sink_facade.stream_data_saver_and_sender, StreamDataSaverAndSender)
            assert isinstance(data_sink_facade.snapshot_manager, SnapshotManager)

            data_sink_facade.shutdown()
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        @pytest.mark.skip
        def test_given_archiver_facade_when_shutdown_called_then_no_threads_are_left(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 60,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            archiver_facade = launch_data_sink(config)

            time.sleep(15)

            archiver_facade.shutdown()

            active_threads = []

            for _ in range(20):
                active_threads = [
                    thread for thread in threading.enumerate()
                    if thread is not threading.current_thread()
                ]
                if not active_threads:
                    break
                time.sleep(1)

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

            assert len(active_threads) == 0, f"Still active threads after shutdown: {[thread.name for thread in active_threads]}"

            del archiver_facade

        @pytest.mark.parametrize('execution_number', range(1))
        def test_given_archiver_daemon_when_shutdown_method_during_no_stream_switch_is_called_then_no_threads_are_left(self,execution_number):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "SHIBUSDT",
                             "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"],

                    "usd_m_futures": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT",
                                      "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"],

                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP", "BNBUSD_PERP", "SOLUSD_PERP", "XRPUSD_PERP",
                                       "DOGEUSD_PERP", "ADAUSD_PERP", "LTCUSD_PERP", "AVAXUSD_PERP", "TRXUSD_PERP",
                                       "DOTUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 60,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            archiver_daemon = launch_data_sink(config)

            time.sleep(5)

            archiver_daemon.shutdown()

            active_threads = []

            for _ in range(40):
                active_threads = [
                    thread for thread in threading.enumerate()
                    if thread is not threading.current_thread()
                ]
                if not active_threads:
                    break
                time.sleep(1)

            for _ in active_threads: print(_)

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

            assert len(active_threads) == 0, (f"Still active threads after run {execution_number + 1}"
                                              f": {[thread.name for thread in active_threads]}")

            del archiver_daemon


    class TestListenerFacade:

        def test_given_archiver_facade_when_init_then_global_shutdown_flag_is_false(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 60,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            logger = setup_logger()
            listener_facade = ListenerFacade(config=config, logger=logger)

            assert not listener_facade.global_shutdown_flag.is_set()

            del listener_facade

            TradeQueue.clear_instances()
            DifferenceDepthQueue.clear_instances()

        def test_given_archiver_facade_when_init_then_queues_are_set_properly(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 60,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            logger = setup_logger()
            listener_facade = ListenerFacade(config=config, logger=logger)

            queue_pool = listener_facade.queue_pool

            assert isinstance(queue_pool.spot_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(queue_pool.usd_m_futures_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(queue_pool.coin_m_orderbook_stream_message_queue, DifferenceDepthQueue)

            assert isinstance(queue_pool.spot_trade_stream_message_queue, TradeQueue)
            assert isinstance(queue_pool.usd_m_futures_trade_stream_message_queue, TradeQueue)
            assert isinstance(queue_pool.coin_m_trade_stream_message_queue, TradeQueue)

            assert len(TradeQueue._instances) == 3
            assert len(DifferenceDepthQueue._instances) == 3

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_archiver_facade_run_call_when_threads_invoked_then_correct_threads_are_started(self):

            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            data_listener_facade = launch_data_listener(config)

            time.sleep(5)

            num_markets = len(config["instruments"])

            expected_stream_service_threads = num_markets * 2
            expected_snapshot_daemon_threads = num_markets

            total_expected_threads = (expected_stream_service_threads + expected_snapshot_daemon_threads)

            active_threads = threading.enumerate()
            daemon_threads = [thread for thread in active_threads if 'stream_service' in thread.name or
                              'stream_writer' in thread.name or 'snapshot_daemon'
                              in thread.name]

            thread_names = [thread.name for thread in daemon_threads]

            for market in ["SPOT", "USD_M_FUTURES", "COIN_M_FUTURES"]:
                assert f'stream_service: market: {Market[market]}, stream_type: {StreamType.DIFFERENCE_DEPTH_STREAM}' in thread_names
                assert f'stream_service: market: {Market[market]}, stream_type: {StreamType.TRADE_STREAM}' in thread_names
                assert f'snapshot_daemon: market: {Market[market]}' in thread_names

            assert len(daemon_threads) == total_expected_threads

            data_listener_facade.shutdown()

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_archiver_facade_initialization_in_listener_mode(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT"]
                },
                "websocket_life_time_seconds": 70
            }
            logger = setup_logger()
            observers = [MagicMock(spec=Observer)]
            archiver_facade = ListenerFacade(
                config=config,
                logger=logger,
                init_observers=observers
            )

            assert archiver_facade._observers == observers
            assert isinstance(archiver_facade.whistleblower, ListenerObserverUpdater)
            assert archiver_facade.whistleblower.observers == observers

            archiver_facade.shutdown()
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_attach_and_detach_observers(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT"]
                },
                "websocket_life_time_seconds": 70
            }
            logger = setup_logger()
            observer1 = MagicMock(spec=Observer)
            observer2 = MagicMock(spec=Observer)
            listener_facade = ListenerFacade(
                config=config,
                logger=logger,
                init_observers=[observer1]
            )

            listener_facade.attach(observer2)
            assert listener_facade._observers == [observer1, observer2], "Observer2 should be attached"

            listener_facade.detach(observer1)
            assert listener_facade._observers == [observer2], "Observer1 should be detached"

            listener_facade.shutdown()
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        @pytest.mark.skip
        def test_given_archiver_facade_when_shutdown_called_then_no_threads_are_left(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 60,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            archiver_facade = launch_data_sink(config)

            time.sleep(15)

            archiver_facade.shutdown()

            active_threads = []

            for _ in range(40):
                active_threads = [
                    thread for thread in threading.enumerate()
                    if thread is not threading.current_thread()
                ]
                if not active_threads:
                    break
                time.sleep(1)

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

            assert len(active_threads) == 0, f"Still active threads after shutdown: {[thread.name for thread in active_threads]}"

            del archiver_facade

        @pytest.mark.parametrize('execution_number', range(1))
        def test_given_archiver_daemon_when_shutdown_method_during_no_stream_switch_is_called_then_no_threads_are_left(self,execution_number):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "SHIBUSDT",
                             "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"],

                    "usd_m_futures": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT",
                                      "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"],

                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP", "BNBUSD_PERP", "SOLUSD_PERP", "XRPUSD_PERP",
                                       "DOGEUSD_PERP", "ADAUSD_PERP", "LTCUSD_PERP", "AVAXUSD_PERP", "TRXUSD_PERP",
                                       "DOTUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 60,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            archiver_daemon = launch_data_listener(config)

            time.sleep(5)

            archiver_daemon.shutdown()

            active_threads = []

            for _ in range(40):
                active_threads = [
                    thread for thread in threading.enumerate()
                    if thread is not threading.current_thread()
                ]
                if not active_threads:
                    break
                time.sleep(1)

            for _ in active_threads: print(_)

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

            assert len(active_threads) == 0, (f"Still active threads after run {execution_number + 1}"
                                              f": {[thread.name for thread in active_threads]}")

            del archiver_daemon


    class TestObserverPattern:

        class MockObserver(Observer):
            def __init__(self):
                self.messages = []

            def update(self, message):
                self.messages.append(message)

        def test_observer_receives_notifications(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT"]
                },
                "websocket_life_time_seconds": 70
            }
            logger = setup_logger()
            observer = self.MockObserver()
            archiver_facade = ListenerFacade(
                config=config,
                logger=logger,
                init_observers=[observer]
            )

            test_message = "Test Message"
            archiver_facade.queue_pool.global_queue.put(test_message)

            whistleblower_thread = threading.Thread(target=archiver_facade.whistleblower.process_global_queue)
            whistleblower_thread.start()

            time.sleep(1)

            archiver_facade.shutdown()
            whistleblower_thread.join()

            assert observer.messages == [test_message], "Observer should have received the test message"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()


    class TestWhistleblower:

        def test_whistleblower_processes_messages_and_notifies_observers(self):
            observers = [MagicMock(spec=Observer)]
            global_queue = Queue()
            global_shutdown_flag = threading.Event()
            whistleblower = ListenerObserverUpdater(
                logger=setup_logger(),
                observers=observers,
                global_queue=global_queue,
                global_shutdown_flag=global_shutdown_flag
            )

            test_message = "Test Message"
            global_queue.put(test_message)

            thread = threading.Thread(target=whistleblower.process_global_queue)
            thread.start()

            time.sleep(1)

            global_shutdown_flag.set()
            thread.join()

            for observer in observers:
                observer.update.assert_called_with(test_message)

        def test_whistleblower_exits_on_shutdown(self):
            observers = [MagicMock(spec=Observer)]
            global_queue = Queue()
            global_shutdown_flag = threading.Event()
            whistleblower = ListenerObserverUpdater(
                logger=setup_logger(),
                observers=observers,
                global_queue=global_queue,
                global_shutdown_flag=global_shutdown_flag
            )

            thread = threading.Thread(target=whistleblower.process_global_queue)
            thread.start()

            global_shutdown_flag.set()
            thread.join()

            assert not thread.is_alive(), "Whistleblower thread should have exited"


    class TestQueuePoolDataSink:

        def test_given_queue_pool_when_initialized_then_queues_are_set_properly(self):
            queue_pool = QueuePoolDataSink()

            expected_keys = [
                (Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM),
                (Market.SPOT, StreamType.TRADE_STREAM),
                (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM),
                (Market.USD_M_FUTURES, StreamType.TRADE_STREAM),
                (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM),
                (Market.COIN_M_FUTURES, StreamType.TRADE_STREAM)
            ]

            assert set(queue_pool.queue_lookup.keys()) == set(
                expected_keys
            ), "queue_lookup keys do not match expected keys"

            # Check if queues are instances of the correct classes
            assert isinstance(queue_pool.spot_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(queue_pool.usd_m_futures_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(queue_pool.coin_m_orderbook_stream_message_queue, DifferenceDepthQueue)

            assert isinstance(queue_pool.spot_trade_stream_message_queue, TradeQueue)
            assert isinstance(queue_pool.usd_m_futures_trade_stream_message_queue, TradeQueue)
            assert isinstance(queue_pool.coin_m_trade_stream_message_queue, TradeQueue)

            # Verify that the correct number of instances have been created
            assert len(TradeQueue._instances) == 3, "There should be 3 instances of TradeQueue"
            assert len(DifferenceDepthQueue._instances) == 3, "There should be 3 instances of DifferenceDepthQueue"

            # Clear instances after test
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_get_queue_called_then_returns_correct_queue(self):
            queue_pool = QueuePoolDataSink()

            expected_queues = {
                (Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM): DifferenceDepthQueue,
                (Market.SPOT, StreamType.TRADE_STREAM): TradeQueue,
                (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM): DifferenceDepthQueue,
                (Market.USD_M_FUTURES, StreamType.TRADE_STREAM): TradeQueue,
                (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM): DifferenceDepthQueue,
                (Market.COIN_M_FUTURES, StreamType.TRADE_STREAM): TradeQueue
            }

            for (market, stream_type), expected_queue_type in expected_queues.items():
                queue = queue_pool.get_queue(market, stream_type)
                assert isinstance(queue,
                                  expected_queue_type), f"Queue for {market}, {stream_type} should be {expected_queue_type}"
                assert queue.market == market, f"Queue market should be {market}"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_more_than_allowed_trade_queues_created_then_exception_is_thrown(self):
            queue_pool = QueuePoolDataSink()

            with pytest.raises(ClassInstancesAmountLimitException) as excinfo:
                queue_pool.fourth_trade_queue = TradeQueue(market=Market.SPOT)

            assert str(excinfo.value) == "Cannot create more than 3 instances of TradeQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_more_than_allowed_difference_depth_queues_created_then_exception_is_thrown(self):
            queue_pool = QueuePoolDataSink()

            with pytest.raises(ClassInstancesAmountLimitException) as excinfo:
                queue_pool.fourth_difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

            assert str(excinfo.value) == "Cannot create more than 3 instances of DifferenceDepthQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_queue_pool_initialization_in_data_sink_mode(self):
            queue_pool = QueuePoolListener()

            assert queue_pool.global_queue is not None, "Global queue should be initialized in LISTENER mode"

            # Check if queues are initialized with the global_queue
            assert queue_pool.spot_orderbook_stream_message_queue.queue == queue_pool.global_queue
            assert queue_pool.spot_trade_stream_message_queue.queue == queue_pool.global_queue
            assert queue_pool.usd_m_futures_orderbook_stream_message_queue.queue == queue_pool.global_queue
            assert queue_pool.usd_m_futures_trade_stream_message_queue.queue == queue_pool.global_queue
            assert queue_pool.coin_m_orderbook_stream_message_queue.queue == queue_pool.global_queue
            assert queue_pool.coin_m_trade_stream_message_queue.queue == queue_pool.global_queue

            # Clear instances after test
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()


    class TestQueuePoolListener:

        def test_given_queue_pool_when_initialized_then_queues_are_set_properly(self):
            queue_pool = QueuePoolListener()

            expected_keys = [
                (Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM),
                (Market.SPOT, StreamType.TRADE_STREAM),
                (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM),
                (Market.USD_M_FUTURES, StreamType.TRADE_STREAM),
                (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM),
                (Market.COIN_M_FUTURES, StreamType.TRADE_STREAM)
            ]

            assert set(queue_pool.queue_lookup.keys()) == set(
                expected_keys
            ), "queue_lookup keys do not match expected keys"

            # Check if queues are instances of the correct classes
            assert isinstance(queue_pool.spot_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(queue_pool.usd_m_futures_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(queue_pool.coin_m_orderbook_stream_message_queue, DifferenceDepthQueue)

            assert isinstance(queue_pool.spot_trade_stream_message_queue, TradeQueue)
            assert isinstance(queue_pool.usd_m_futures_trade_stream_message_queue, TradeQueue)
            assert isinstance(queue_pool.coin_m_trade_stream_message_queue, TradeQueue)

            # Verify that the correct number of instances have been created
            assert len(TradeQueue._instances) == 3, "There should be 3 instances of TradeQueue"
            assert len(DifferenceDepthQueue._instances) == 3, "There should be 3 instances of DifferenceDepthQueue"

            # Clear instances after test
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_get_queue_called_then_returns_correct_queue(self):
            queue_pool = QueuePoolListener()

            expected_queues = {
                (Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM): DifferenceDepthQueue,
                (Market.SPOT, StreamType.TRADE_STREAM): TradeQueue,
                (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM): DifferenceDepthQueue,
                (Market.USD_M_FUTURES, StreamType.TRADE_STREAM): TradeQueue,
                (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM): DifferenceDepthQueue,
                (Market.COIN_M_FUTURES, StreamType.TRADE_STREAM): TradeQueue
            }

            for (market, stream_type), expected_queue_type in expected_queues.items():
                queue = queue_pool.get_queue(market, stream_type)
                assert isinstance(queue,
                                  expected_queue_type), f"Queue for {market}, {stream_type} should be {expected_queue_type}"
                assert queue.market == market, f"Queue market should be {market}"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_more_than_allowed_trade_queues_created_then_exception_is_thrown(self):
            queue_pool = QueuePoolListener()

            with pytest.raises(ClassInstancesAmountLimitException) as excinfo:
                queue_pool.fourth_trade_queue = TradeQueue(market=Market.SPOT)

            assert str(excinfo.value) == "Cannot create more than 3 instances of TradeQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_more_than_allowed_difference_depth_queues_created_then_exception_is_thrown(self):
            queue_pool = QueuePoolListener()

            with pytest.raises(ClassInstancesAmountLimitException) as excinfo:
                queue_pool.fourth_difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

            assert str(excinfo.value) == "Cannot create more than 3 instances of DifferenceDepthQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_queue_pool_initialization_in_listener_mode(self):
            queue_pool = QueuePoolListener()

            assert queue_pool.global_queue is not None, "Global queue should be initialized in LISTENER mode"

            assert queue_pool.spot_orderbook_stream_message_queue.queue == queue_pool.global_queue
            assert queue_pool.spot_trade_stream_message_queue.queue == queue_pool.global_queue
            assert queue_pool.usd_m_futures_orderbook_stream_message_queue.queue == queue_pool.global_queue
            assert queue_pool.usd_m_futures_trade_stream_message_queue.queue == queue_pool.global_queue
            assert queue_pool.coin_m_orderbook_stream_message_queue.queue == queue_pool.global_queue
            assert queue_pool.coin_m_trade_stream_message_queue.queue == queue_pool.global_queue

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()


    class TestStreamService:

        def test_stream_service_initialization_with_global_queue(self):
            logger = setup_logger()
            global_shutdown_flag = threading.Event()
            queue_pool = QueuePoolListener()
            stream_service = StreamService(
                config={
                    'instruments':{
                        "spot": ["BTCUSDT"]
                    },
                    'websocket_lifetime_seconds':60
                },
                logger=logger,
                queue_pool=queue_pool,
                global_shutdown_flag=global_shutdown_flag
            )

            assert queue_pool.global_queue is not None, "Global queue should be initialized in LISTENER mode"
            TradeQueue.clear_instances()
            DifferenceDepthQueue.clear_instances()

        def test_stream_service_runs_streams_in_listener_mode(self):
            logger = setup_logger()
            global_shutdown_flag = threading.Event()
            queue_pool = QueuePoolListener()
            stream_service = StreamService(
                config={
                    'instruments':{
                        "spot": ["BTCUSDT"]
                    },
                    'websocket_lifetime_seconds':60
                },
                logger=logger,
                queue_pool=queue_pool,
                global_shutdown_flag=global_shutdown_flag
            )

            with patch.object(StreamService, 'start_stream_service') as mock_start_stream_service:
                stream_service.run_streams()
                assert mock_start_stream_service.call_count == 2, "Should start two stream services in LISTENER mode"
            TradeQueue.clear_instances()
            DifferenceDepthQueue.clear_instances()


    class TestSnapshotManager:

        def test_init(self):
            assert True

        class TestSnapshotManager:

            def test_given_listener_strategy_when_snapshot_received_then_snapshot_put_into_global_queue(self):
                config={
                    'instruments':{
                        "spot": ["BTCUSDT"]
                    },
                    'websocket_lifetime_seconds':60
                }
                logger = setup_logger()
                global_shutdown_flag = threading.Event()

                global_queue = Queue()
                snapshot_strategy = ListenerSnapshotStrategy(global_queue=global_queue)

                snapshot_manager = SnapshotManager(
                    config=config,
                    logger=logger,
                    snapshot_strategy=snapshot_strategy,
                    global_shutdown_flag=global_shutdown_flag
                )

                with patch.object(SnapshotManager, '_request_snapshot_with_timestamps',
                                  return_value=({"snapshot": "data"}, 1234567890, 1234567891)):
                    daemon_thread = threading.Thread(
                        target=snapshot_manager._snapshot_daemon,
                        args=(
                            ["BTCUSDT"],
                            Market.SPOT,
                            ""
                        ),
                        name='snapshot_daemon_thread'
                    )
                    daemon_thread.start()

                    time.sleep(2)

                    global_shutdown_flag.set()

                    daemon_thread.join(timeout=2)

                    assert not global_queue.empty(), "Global queue powinna zawierać dane snapshotu"

                    message = global_queue.get()
                    assert isinstance(message, str), "Dane snapshotu powinny być zserializowane jako string JSON"

                    expected_snapshot = {"snapshot": "data", "_rq": 1234567890, "_rc": 1234567891}
                    assert message == json.dumps(expected_snapshot), "Dane snapshotu powinny zgadzać się z oczekiwanymi"

            def test_given_data_sink_strategy_when_snapshot_received_then_data_saver_methods_called(self):
                config={
                    'instruments':{
                        "spot": ["BTCUSDT"]
                    },
                    'websocket_lifetime_seconds':60
                }
                logger = setup_logger()
                global_shutdown_flag = threading.Event()

                data_saver = MagicMock(spec=StreamDataSaverAndSender)
                snapshot_strategy = DataSinkSnapshotStrategy(
                    data_saver=data_saver,
                    save_to_json=True,
                    save_to_zip=True,
                    send_zip_to_blob=True
                )

                snapshot_manager = SnapshotManager(
                    config=config,
                    logger=logger,
                    snapshot_strategy=snapshot_strategy,
                    global_shutdown_flag=global_shutdown_flag
                )

                with patch.object(SnapshotManager, '_request_snapshot_with_timestamps',
                                  return_value=({"snapshot": "data"}, 1234567890, 1234567891)):
                    with patch.object(StreamDataSaverAndSender, 'get_file_name', return_value='file_name.json'):
                        daemon_thread = threading.Thread(
                            target=snapshot_manager._snapshot_daemon,
                            args=(
                                ["BTCUSDT"],
                                Market.SPOT,
                                "dump_path"
                            ),
                            name='snapshot_daemon_thread'
                        )
                        daemon_thread.start()

                        time.sleep(0.5)

                        global_shutdown_flag.set()

                        daemon_thread.join(timeout=2)

                        file_name = 'file_name.json'
                        file_path = os.path.join("dump_path", file_name)

                        expected_snapshot = {"snapshot": "data", "_rq": 1234567890, "_rc": 1234567891}

                        data_saver.save_to_json.assert_called_with(expected_snapshot, file_path)
                        data_saver.save_to_zip.assert_called_with(expected_snapshot, file_name, file_path)
                        data_saver.send_zipped_json_to_cloud_storage.assert_called_with(expected_snapshot, file_name)

            def test_given_exception_in_get_snapshot_when_snapshot_fetched_then_error_logged_and_no_snapshot_processed(self):
                config={
                    'instruments':{
                        "spot": ["BTCUSDT"]
                    },
                    'websocket_lifetime_seconds':60
                }
                logger = setup_logger()
                global_shutdown_flag = threading.Event()

                global_queue = Queue()
                snapshot_strategy = ListenerSnapshotStrategy(global_queue=global_queue)

                snapshot_manager = SnapshotManager(
                    config=config,
                    logger=logger,
                    snapshot_strategy=snapshot_strategy,
                    global_shutdown_flag=global_shutdown_flag
                )

                with patch.object(SnapshotManager, '_request_snapshot_with_timestamps', side_effect=Exception("Test exception")):
                    with patch.object(logger, 'error') as mock_logger_error:
                        daemon_thread = threading.Thread(
                            target=snapshot_manager._snapshot_daemon,
                            args=(
                                ["BTCUSDT"],
                                Market.SPOT,
                                ""
                            ),
                            name='snapshot_daemon_thread'
                        )
                        daemon_thread.start()

                        time.sleep(0.5)

                        global_shutdown_flag.set()

                        daemon_thread.join(timeout=2)

                        mock_logger_error.assert_called()
                        assert global_queue.empty(), "Global queue powinna być pusta po wyjątku"

            def test_given_shutdown_flag_set_when_daemon_running_then_thread_exits(self):
                config={
                    'instruments':{
                        "spot": ["BTCUSDT"]
                    },
                    'websocket_lifetime_seconds':60
                }
                logger = setup_logger()
                global_shutdown_flag = threading.Event()

                global_queue = Queue()
                snapshot_strategy = ListenerSnapshotStrategy(global_queue=global_queue)

                snapshot_manager = SnapshotManager(
                    config=config,
                    logger=logger,
                    snapshot_strategy=snapshot_strategy,
                    global_shutdown_flag=global_shutdown_flag
                )

                with patch.object(SnapshotManager, '_request_snapshot_with_timestamps') as mock_get_snapshot:
                    daemon_thread = threading.Thread(
                        target=snapshot_manager._snapshot_daemon,
                        args=(
                            ["BTCUSDT"],
                            Market.SPOT,
                            ""
                        ),
                        name='snapshot_daemon_thread'
                    )
                    daemon_thread.start()

                    time.sleep(0.5)

                    global_shutdown_flag.set()

                    daemon_thread.join(timeout=2)

                    assert not daemon_thread.is_alive(), "Wątek snapshot_daemon powinien zakończyć działanie po ustawieniu flagi global_shutdown_flag"

            def test_given_successful_response_when_get_snapshot_called_then_data_and_timestamps_returned(self):
                config={
                    'instruments':{
                        "spot": ["BTCUSDT"]
                    },
                    'websocket_lifetime_seconds':60
                }
                logger = setup_logger()
                global_shutdown_flag = threading.Event()

                snapshot_strategy = MagicMock(spec=SnapshotStrategy)

                snapshot_manager = SnapshotManager(
                    config=config,
                    logger=logger,
                    snapshot_strategy=snapshot_strategy,
                    global_shutdown_flag=global_shutdown_flag
                )

                response_data = {"bids": [], "asks": []}
                with patch('requests.get') as mock_get:
                    mock_response = MagicMock()
                    mock_response.status_code = 200
                    mock_response.json.return_value = response_data
                    mock_get.return_value = mock_response

                    with patch.object(TimestampsGenerator, 'get_utc_timestamp_epoch_milliseconds', side_effect=[1000, 2000]):
                        data, request_timestamp, receive_timestamp = snapshot_manager._request_snapshot_with_timestamps("BTCUSDT",
                                                                                                                        Market.SPOT)

                        assert data == response_data
                        assert request_timestamp == 1000
                        assert receive_timestamp == 2000

            def test_given_exception_when_get_snapshot_called_then_none_returned_and_error_logged(self):
                config={
                    'instruments':{
                        "spot": ["BTCUSDT"]
                    },
                    'websocket_lifetime_seconds':60
                }
                logger = setup_logger()
                global_shutdown_flag = threading.Event()

                snapshot_strategy = MagicMock(spec=SnapshotStrategy)

                snapshot_manager = SnapshotManager(
                    config=config,
                    logger=logger,
                    snapshot_strategy=snapshot_strategy,
                    global_shutdown_flag=global_shutdown_flag
                )

                with patch('requests.get', side_effect=Exception("Network error")):
                    with patch.object(logger, 'error') as mock_logger_error:
                        data, request_timestamp, receive_timestamp = snapshot_manager._request_snapshot_with_timestamps("BTCUSDT",
                                                                                                                        Market.SPOT)

                        assert data is None
                        assert request_timestamp is None
                        assert receive_timestamp is None
                        mock_logger_error.assert_called()

            def test_given_shutdown_flag_set_when_sleep_with_flag_check_called_then_method_exits_early(self):
                logger = setup_logger()
                global_shutdown_flag = threading.Event()

                snapshot_manager = SnapshotManager(
                    config={
                        'instruments': {
                            "spot": ["BTCUSDT"]
                        },
                        'websocket_lifetime_seconds': 60
                    },
                    logger=logger,
                    snapshot_strategy=MagicMock(spec=SnapshotStrategy),
                    global_shutdown_flag=global_shutdown_flag
                )

                def set_flag():
                    time.sleep(0.5)
                    global_shutdown_flag.set()

                threading.Thread(target=set_flag).start()

                start_time = time.time()
                snapshot_manager._sleep_with_flag_check(duration=5)
                elapsed_time = time.time() - start_time

                assert elapsed_time < 1.5, "Metoda powinna zakończyć działanie szybko po ustawieniu flagi global_shutdown_flag"

        class TestListenerSnapshotStrategy:

            def test_given_snapshot_when_handle_snapshot_called_then_snapshot_put_into_global_queue(self):
                global_queue = Queue()
                snapshot_strategy = ListenerSnapshotStrategy(global_queue=global_queue)

                snapshot = {"snapshot": "data", "_rq": 1234567890, "_rc": 1234567891}
                snapshot_strategy.handle_snapshot(
                    snapshot=snapshot,
                    pair="BTCUSDT",
                    market=Market.SPOT,
                    dump_path="",
                    file_name=""
                )

                assert not global_queue.empty(), "Global queue powinna zawierać dane snapshotu"
                message = global_queue.get()
                assert message == json.dumps(snapshot), "Dane snapshotu powinny być zserializowane jako string JSON"

        class TestDataSinkSnapshotStrategy:

            def test_given_all_save_options_true_when_handle_snapshot_called_then_all_methods_called(self):
                data_saver = MagicMock(spec=StreamDataSaverAndSender)
                snapshot_strategy = DataSinkSnapshotStrategy(
                    data_saver=data_saver,
                    save_to_json=True,
                    save_to_zip=True,
                    send_zip_to_blob=True
                )

                snapshot = {"snapshot": "data", "_rq": 1234567890, "_rc": 1234567891}
                file_name = "file_name.json"
                file_path = os.path.join("dump_path", file_name)

                snapshot_strategy.handle_snapshot(
                    snapshot=snapshot,
                    pair="BTCUSDT",
                    market=Market.SPOT,
                    dump_path="dump_path",
                    file_name=file_name
                )

                data_saver.save_to_json.assert_called_with(snapshot, file_path)
                data_saver.save_to_zip.assert_called_with(snapshot, file_name, file_path)
                data_saver.send_zipped_json_to_cloud_storage.assert_called_with(snapshot, file_name)

            def test_given_save_to_json_false_when_handle_snapshot_called_then_save_to_json_not_called(self):
                data_saver = MagicMock(spec=StreamDataSaverAndSender)
                snapshot_strategy = DataSinkSnapshotStrategy(
                    data_saver=data_saver,
                    save_to_json=False,
                    save_to_zip=True,
                    send_zip_to_blob=True
                )

                snapshot = {"snapshot": "data"}
                file_name = "file_name.json"
                file_path = os.path.join("dump_path", file_name)

                snapshot_strategy.handle_snapshot(
                    snapshot=snapshot,
                    pair="BTCUSDT",
                    market=Market.SPOT,
                    dump_path="dump_path",
                    file_name=file_name
                )

                data_saver.save_to_json.assert_not_called()
                data_saver.save_to_zip.assert_called_once()
                data_saver.send_zipped_json_to_cloud_storage.assert_called_once()

            def test_given_save_to_zip_false_when_handle_snapshot_called_then_save_to_zip_not_called(self):
                data_saver = MagicMock(spec=StreamDataSaverAndSender)
                snapshot_strategy = DataSinkSnapshotStrategy(
                    data_saver=data_saver,
                    save_to_json=True,
                    save_to_zip=False,
                    send_zip_to_blob=True
                )

                snapshot = {"snapshot": "data"}
                file_name = "file_name.json"
                file_path = os.path.join("dump_path", file_name)

                snapshot_strategy.handle_snapshot(
                    snapshot=snapshot,
                    pair="BTCUSDT",
                    market=Market.SPOT,
                    dump_path="dump_path",
                    file_name=file_name
                )

                data_saver.save_to_json.assert_called_once()
                data_saver.save_to_zip.assert_not_called()
                data_saver.send_zipped_json_to_cloud_storage.assert_called_once()
                data_saver.send_zipped_json_to_cloud_storage.assert_called_once()

            def test_given_send_zip_to_blob_false_when_handle_snapshot_called_then_send_zip_to_blob_not_called(self):
                data_saver = MagicMock(spec=StreamDataSaverAndSender)
                snapshot_strategy = DataSinkSnapshotStrategy(
                    data_saver=data_saver,
                    save_to_json=True,
                    save_to_zip=True,
                    send_zip_to_blob=False
                )

                snapshot = {"snapshot": "data"}
                file_name = "file_name.json"
                file_path = os.path.join("dump_path", file_name)

                snapshot_strategy.handle_snapshot(
                    snapshot=snapshot,
                    pair="BTCUSDT",
                    market=Market.SPOT,
                    dump_path="dump_path",
                    file_name=file_name
                )

                data_saver.save_to_json.assert_called_once()
                data_saver.save_to_zip.assert_called_once()
                data_saver.send_zipped_json_to_cloud_storage.assert_not_called()


    class TestCommandLineInterface:

        def test_given_modify_subscription_when_adding_asset_then_asset_is_added_to_instruments(self):
            config = {
                'instruments': {
                    "spot": ["BTCUSDT"]
                },
                'websocket_lifetime_seconds': 60
            }
            logger = setup_logger()
            global_shutdown_flag = threading.Event()
            queue_pool = QueuePoolDataSink()
            stream_service = StreamService(
                config=config,
                logger=logger,
                queue_pool=queue_pool,
                global_shutdown_flag=global_shutdown_flag
            )
            cli = CommandLineInterface(
                config=config,
                logger=logger,
                stream_service=stream_service
            )

            message = {'modify_subscription': {'type': 'subscribe', 'market': 'spot', 'asset': 'BNBUSDT'}}
            cli.handle_command(message)

            assert 'BNBUSDT' in config['instruments']['spot'], "Asset not added to instruments"
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_modify_subscription_when_removing_asset_then_asset_is_removed_from_instruments(self):
            config = {
                'instruments': {
                    "spot": ["BTCUSDT"]
                },
                'websocket_lifetime_seconds': 60
            }
            logger = setup_logger()
            global_shutdown_flag = threading.Event()
            queue_pool = QueuePoolDataSink()
            stream_service = StreamService(
                config=config,
                logger=logger,
                queue_pool=queue_pool,
                global_shutdown_flag=global_shutdown_flag
            )
            cli = CommandLineInterface(
                config=config,
                logger=logger,
                stream_service=stream_service
            )

            message = {'modify_subscription': {'type': 'unsubscribe', 'market': 'spot', 'asset': 'BNBUSDT'}}
            cli.handle_command(message)

            assert 'BNBUSDT' not in config['instruments']['spot'], "Asset not removed from instruments"
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_handle_command_with_invalid_command_logs_warning(self):
            config = {
                'instruments': {
                    "spot": ["BTCUSDT"]
                },
                'websocket_lifetime_seconds': 60
            }
            logger = setup_logger()
            stream_service = MagicMock(spec=StreamService)
            cli = CommandLineInterface(
                config=config,
                logger=logger,
                stream_service=stream_service
            )

            with patch.object(logger, 'warning') as mock_warning:
                message = {'invalid_command': {'type': 'subscribe', 'market': 'spot', 'asset': 'BNBUSDT'}}
                cli.handle_command(message)
                mock_warning.assert_called_with('Bad command, try again')

        def test_modify_subscription_with_invalid_market_logs_warning(self):
            config = {
                'instruments': {
                    "spot": ["BTCUSDT"]
                },
                'websocket_lifetime_seconds': 60
            }
            logger = setup_logger()
            stream_service = MagicMock(spec=StreamService)
            cli = CommandLineInterface(
                config=config,
                logger=logger,
                stream_service=stream_service
            )

            with patch.object(logger, 'warning') as mock_warning:
                message = {'modify_subscription': {'type': 'subscribe', 'market': 'invalid_market', 'asset': 'BNBUSDT'}}
                with pytest.raises(KeyError):
                    cli.handle_command(message)


    class TestDataSaverSender:

        def setup_method(self):
            self.logger = setup_logger()
            self.global_shutdown_flag = threading.Event()
            self.azure_blob_parameters_with_key = ('DefaultEndpointsProtocol=https;'
                                                   'AccountName=test_account;AccountKey=test_key;'
                                                   'EndpointSuffix=core.windows.net')
            self.container_name = 'test_container'
            self.backblaze_s3_parameters = {
                'access_key_id': 'test_access_key_id',
                'secret_access_key': 'test_secret_access_key',
                'endpoint_url': 'https://s3.eu-central-003.backblazeb2.com'
            }
            self.backblaze_bucket_name = 'test_bucket'

        def test_given_blob_parameters_when_initializing_then_blob_service_client_is_initialized(self):
            data_saver = StreamDataSaverAndSender(
                logger=self.logger,
                config={},
                azure_blob_parameters_with_key=self.azure_blob_parameters_with_key,
                azure_container_name=self.container_name,
                backblaze_s3_parameters=None,
                backblaze_bucket_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            assert data_saver.azure_blob_service_client is not None, "BlobServiceClient should be initialized"
            assert data_saver.azure_container_name == self.container_name, "Container name should be set"

        def test_given_no_blob_parameters_when_initializing_then_blob_service_client_is_none(self):
            data_saver = StreamDataSaverAndSender(
                logger=self.logger,
                config={},
                azure_blob_parameters_with_key=None,
                azure_container_name=None,
                backblaze_s3_parameters=None,
                backblaze_bucket_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            assert data_saver.azure_blob_service_client is None, "BlobServiceClient should be None when parameters are missing"

        def test_given_data_saver_when_run_then_stream_writers_are_started(self):
            config = {
              "instruments": {
                "spot": [
                  "BTCUSDT"
                ]
              },
              "file_duration_seconds": 60,
              "snapshot_fetcher_interval_seconds": 60,
              "websocket_life_time_seconds": 60,
              "save_to_json": True,
              "save_to_zip": False,
              "send_zip_to_blob": False
            }
            data_saver = StreamDataSaverAndSender(
                logger=self.logger,
                config=config,
                azure_blob_parameters_with_key=None,
                azure_container_name=None,
                backblaze_s3_parameters=None,
                backblaze_bucket_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            queue_pool = QueuePoolDataSink()
            with patch.object(StreamDataSaverAndSender, 'start_stream_writer') as mock_start_stream_writer:
                data_saver.run_data_saver(
                    queue_pool=queue_pool,
                    dump_path='dump/',
                    file_duration_seconds=60,
                    save_to_json=True,
                    save_to_zip=False,
                    send_zip_to_blob=False
                )

                assert mock_start_stream_writer.call_count == len(
                    queue_pool.queue_lookup), "start_stream_writer should be called for each queue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_start_stream_writer_when_called_then_thread_is_started(self):
            config = {
              "instruments": {
                "spot": [
                  "BTCUSDT"
                ]
              },
              "file_duration_seconds": 60,
              "snapshot_fetcher_interval_seconds": 60,
              "websocket_life_time_seconds": 60,
              "save_to_json": True,
              "save_to_zip": False,
              "send_zip_to_blob": False
            }
            data_saver = StreamDataSaverAndSender(
                logger=self.logger,
                config={},
                azure_blob_parameters_with_key=None,
                azure_container_name=None,
                backblaze_s3_parameters=None,
                backblaze_bucket_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            queue = DifferenceDepthQueue(market=Market.SPOT)
            with patch('threading.Thread') as mock_thread:
                data_saver.start_stream_writer(
                    queue=queue,
                    market=Market.SPOT,
                    file_duration_seconds=60,
                    dump_path='dump/',
                    stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                    save_to_json=True,
                    save_to_zip=False,
                    send_zip_to_blob=False
                )

                mock_thread.assert_called_once()
                args, kwargs = mock_thread.call_args
                assert kwargs['target'] == data_saver._stream_writer, "Thread target should be _stream_writer"
                mock_thread.return_value.start.assert_called_once()
            DifferenceDepthQueue.clear_instances()

        def test_given_stream_writer_when_shutdown_flag_set_then_exits_loop(self):
            config = {
              "instruments": {
                "spot": [
                  "BTCUSDT"
                ]
              },
              "file_duration_seconds": 60,
              "snapshot_fetcher_interval_seconds": 60,
              "websocket_life_time_seconds": 60,
              "save_to_json": True,
              "save_to_zip": False,
              "send_zip_to_blob": False
            }
            data_saver = StreamDataSaverAndSender(
                logger=self.logger,
                config=config,
                azure_blob_parameters_with_key=None,
                azure_container_name=None,
                backblaze_s3_parameters=None,
                backblaze_bucket_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            stream_listener_id = StreamId(pairs=['BTCUSDT'])

            queue = DifferenceDepthQueue(market=Market.SPOT)
            queue.put_queue_message(
                message='{"stream": "btcusdt@depth", "data": {}}',
                stream_listener_id=stream_listener_id,
                timestamp_of_receive=1234567890
            )

            with patch.object(StreamDataSaverAndSender, '_process_stream_data') as mock_process_stream_data, \
                    patch.object(StreamDataSaverAndSender, '_sleep_with_flag_check') as mock_sleep_with_flag_check:
                def side_effect(duration):
                    self.global_shutdown_flag.set()

                mock_sleep_with_flag_check.side_effect = side_effect

                data_saver._stream_writer(
                    queue=queue,
                    market=Market.SPOT,
                    file_duration_seconds=1,
                    dump_path='dump/',
                    stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                    save_to_json=True,
                    save_to_zip=False,
                    send_zip_to_blob=False
                )

                assert mock_process_stream_data.call_count == 2, "Should process data during and after loop"
                mock_sleep_with_flag_check.assert_called_once_with(60)
            DifferenceDepthQueue.clear_instances()

        def test_given_process_stream_data_when_queue_is_empty_then_no_action_is_taken(self):
            config = {
              "instruments": {
                "spot": [
                  "BTCUSDT"
                ]
              },
              "file_duration_seconds": 60,
              "snapshot_fetcher_interval_seconds": 60,
              "websocket_life_time_seconds": 60,
              "save_to_json": True,
              "save_to_zip": False,
              "send_zip_to_blob": False
            }
            data_saver = StreamDataSaverAndSender(
                logger=self.logger,
                config=config,
                azure_blob_parameters_with_key=None,
                azure_container_name=None,
                backblaze_s3_parameters=None,
                backblaze_bucket_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            queue = DifferenceDepthQueue(market=Market.SPOT)
            with patch.object(StreamDataSaverAndSender, 'save_to_json') as mock_save_to_json:
                data_saver._process_stream_data(
                    queue=queue,
                    market=Market.SPOT,
                    dump_path='dump/',
                    stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                    save_to_json=True,
                    save_to_zip=False,
                    send_zip_to_blob=False
                )

                mock_save_to_json.assert_not_called(), "Should not call _save_to_json when queue is empty"
            DifferenceDepthQueue.clear_instances()

        def test_given_process_stream_data_when_queue_has_data_then_data_is_processed(self, tmpdir):
            config = {
              "instruments": {
                "spot": [
                  "BTCUSDT"
                ]
              },
              "file_duration_seconds": 60,
              "snapshot_fetcher_interval_seconds": 60,
              "websocket_life_time_seconds": 60,
              "save_to_json": True,
              "save_to_zip": False,
              "send_zip_to_blob": False
            }
            data_saver = StreamDataSaverAndSender(
                logger=self.logger,
                config=config,
                azure_blob_parameters_with_key=None,
                azure_container_name=None,
                backblaze_s3_parameters=None,
                backblaze_bucket_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            stream_listener_id = StreamId(pairs=['BTCUSDT'])

            queue = DifferenceDepthQueue(market=Market.SPOT)
            message = '{"stream": "btcusdt@depth", "data": {}}'

            queue.currently_accepted_stream_id = stream_listener_id.id

            queue.put_queue_message(
                message=message,
                stream_listener_id=stream_listener_id,
                timestamp_of_receive=1234567890
            )

            dump_path = tmpdir.mkdir("dump")

            with patch.object(StreamDataSaverAndSender, 'save_to_json') as mock_save_to_json, \
                    patch.object(StreamDataSaverAndSender, 'save_to_zip') as mock_save_to_zip, \
                    patch.object(StreamDataSaverAndSender, 'send_zipped_json_to_cloud_storage') as mock_send_zip:
                data_saver._process_stream_data(
                    queue=queue,
                    market=Market.SPOT,
                    dump_path=str(dump_path),
                    stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                    save_to_json=True,
                    save_to_zip=True,
                    send_zip_to_blob=True
                )

                assert mock_save_to_json.called, "save_to_json should be called"
                assert mock_save_to_zip.called, "save_to_zip should be called"
                assert mock_send_zip.called, "send_zipped_json_to_cloud_storage should be called"
            DifferenceDepthQueue.clear_instances()

        def test_given_get_file_name_when_called_then_correct_format_is_returned(self):
            config = {
              "instruments": {
                "spot": [
                  "BTCUSDT"
                ]
              },
              "file_duration_seconds": 60,
              "snapshot_fetcher_interval_seconds": 60,
              "websocket_life_time_seconds": 60,
              "save_to_json": True,
              "save_to_zip": False,
              "send_zip_to_blob": False
            }

            data_saver = StreamDataSaverAndSender(
                logger=self.logger,
                config=config,
                azure_blob_parameters_with_key=None,
                azure_container_name=None,
                backblaze_s3_parameters=None,
                backblaze_bucket_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            pair = "BTCUSDT"
            market = Market.SPOT
            stream_type = StreamType.DIFFERENCE_DEPTH_STREAM

            with patch('binance_archiver.timestamps_generator'
                       '.TimestampsGenerator.get_utc_formatted_timestamp_for_file_name',
                       return_value='01-01-2022T00-00-00Z'):
                file_name = data_saver.get_file_name(pair, market, stream_type)

                expected_prefix = "difference_depth_stream"
                expected_market_name = "spot"
                expected_file_name = (f"binance_{expected_prefix}_{expected_market_name}_{pair.lower()}"
                                      f"_01-01-2022T00-00-00Z")
                assert file_name == expected_file_name, "File name should be correctly formatted"
            DifferenceDepthQueue.clear_instances()

        def test_data_saver_is_not_initialized_in_listener_mode(self):
            config = {
              "instruments": {
                "spot": [
                  "BTCUSDT"
                ]
              },
              "file_duration_seconds": 60,
              "snapshot_fetcher_interval_seconds": 60,
              "websocket_life_time_seconds": 60,
              "save_to_json": True,
              "save_to_zip": False,
              "send_zip_to_blob": False
            }
            logger = setup_logger()
            global_shutdown_flag = threading.Event()
            data_saver = StreamDataSaverAndSender(
                logger=logger,
                config=config,
                azure_blob_parameters_with_key=None,
                azure_container_name=None,
                backblaze_s3_parameters=None,
                backblaze_bucket_name=None,
                global_shutdown_flag=global_shutdown_flag
            )

            assert data_saver.azure_blob_service_client is None, "BlobServiceClient should be None in LISTENER mode"
            assert data_saver.s3_client is None, "Backblaze S3 Client should be None in LISTENER mode"


    class TestTimestampsGenerator:

        def test_given_time_utils_when_getting_utc_formatted_timestamp_then_format_is_correct(self):
            timestamp = TimestampsGenerator.get_utc_formatted_timestamp_for_file_name()
            pattern = re.compile(r'\d{2}-\d{2}-\d{4}T\d{2}-\d{2}-\d{2}Z')
            assert re.match(r'\d{2}-\d{2}-\d{4}T\d{2}-\d{2}-\d{2}Z', timestamp), \
                "Timestamp should match the format '%d-%m-%YT%H-%M-%SZ'"
            assert pattern.match(timestamp), f"Timestamp {timestamp} does not match the expected format %d-%m-%YT%H-%M-%SZ"

        def test_given_time_utils_when_getting_utc_timestamp_epoch_milliseconds_then_timestamp_is_accurate(self):
            timestamp_milliseconds_method = TimestampsGenerator.get_utc_timestamp_epoch_milliseconds()
            timestamp_milliseconds_now = round(datetime.now(timezone.utc).timestamp() * 1000)

            assert isinstance(timestamp_milliseconds_now, int), "Timestamp should be an integer"
            assert (abs(timestamp_milliseconds_method - timestamp_milliseconds_now) < 2000,
                    "The timestamp in milliseconds is not accurate or not in UTC.")

        def test_given_time_utils_when_getting_utc_timestamp_epoch_seconds_then_timestamp_is_accurate(self):
            timestamp_seconds_method = TimestampsGenerator.get_utc_timestamp_epoch_seconds()
            timestamp_seconds_now = round(datetime.now(timezone.utc).timestamp())

            assert (abs(timestamp_seconds_method - timestamp_seconds_now) < 2,
                    "The timestamp in seconds is not accurate or not in UTC.")

        def test_given_get_actual_epoch_timestamp_when_called_then_timestamps_are_in_utc(self):
            timestamp_seconds_method = TimestampsGenerator.get_utc_timestamp_epoch_seconds()
            timestamp_milliseconds_method = TimestampsGenerator.get_utc_timestamp_epoch_milliseconds()

            datetime_seconds = datetime.fromtimestamp(timestamp_seconds_method, tz=timezone.utc)
            datetime_milliseconds = datetime.fromtimestamp(timestamp_milliseconds_method / 1000, tz=timezone.utc)

            assert datetime_seconds.tzinfo == timezone.utc, "The timestamp in seconds is not in UTC."
            assert datetime_milliseconds.tzinfo == timezone.utc, "The timestamp in milliseconds is not in UTC."

        def test_get_utc_timestamp_epoch_seconds_returns_int(self):
            timestamp = TimestampsGenerator.get_utc_timestamp_epoch_seconds()
            assert isinstance(timestamp, int), "Timestamp should be an integer"


    class TestLaunchDataSink:

        def test_given_config_has_no_instrument_then_exception_is_thrown(self):

            config = {
                "instruments": {},
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": True
            }

            azure_blob_parameters_with_key = 'DefaultEndpointsProtocol=...'
            container_name = 'some_container_name'

            with pytest.raises(BadConfigException) as excinfo:
                launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    azure_container_name=container_name
                )

            assert str(excinfo.value) == "Config must contain 1 to 3 markets and must be a dictionary."

        def test_given_market_type_is_empty_then_exception_is_thrown(self):
            config = {
                "instruments": {
                    "spot": [],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": True
            }

            azure_blob_parameters_with_key = 'DefaultEndpointsProtocol=...'
            container_name = 'some_container_name'

            with pytest.raises(BadConfigException) as excinfo:
                launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    azure_container_name=container_name
                )

            assert str(excinfo.value) == "Pairs for market spot are missing or invalid."

        def test_given_too_many_markets_then_exception_is_thrown(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"],
                    "actions": ["AAPL", "TSLA"],
                    "mining": ["BTCMINING"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": True
            }

            azure_blob_parameters_with_key = 'DefaultEndpointsProtocol=...'
            container_name = 'some_container_name'

            with pytest.raises(BadConfigException) as excinfo:
                launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    azure_container_name=container_name
                )

            assert str(excinfo.value) == "Config must contain 1 to 3 markets and must be a dictionary."

        def test_given_not_handled_market_type_then_exception_is_thrown(self):
            config = {
                "instruments": {
                    "mining": ["BTCMINING"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": True
            }

            azure_blob_parameters_with_key = 'DefaultEndpointsProtocol=...'
            container_name = 'some_container_name'

            with pytest.raises(BadConfigException) as excinfo:
                launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    azure_container_name=container_name
                )

            assert str(excinfo.value) == "Invalid pairs for market mining or market not handled."

        def test_given_valid_config_then_archiver_facade_is_returned(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70
            }

            data_sink_listener = launch_data_listener(config=config)

            assert isinstance(data_sink_listener, ListenerFacade), "ArchiverFacade should be returned"

            data_sink_listener.shutdown()
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()


    class TestLaunchDataListener:

        def test_given_invalid_instruments_config_then_exception_is_raised(self):
            config = {
                "instruments": {},
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": True
            }

            with pytest.raises(BadConfigException) as excinfo:
                launch_data_listener(config=config)

            assert str(excinfo.value) == "Config must contain 1 to 3 markets and must be a dictionary."

        def test_given_invalid_market_in_instruments_then_exception_is_raised(self):
            config = {
                "instruments": {
                    "invalid_market": ["BTCUSDT"]
                },
                "websocket_life_time_seconds": 70
            }

            with pytest.raises(BadConfigException) as excinfo:
                launch_data_listener(config=config)

            assert str(excinfo.value) == "Invalid pairs for market invalid_market."

        def test_given_invalid_websocket_life_time_seconds_then_exception_is_raised(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT"]
                },
                "websocket_life_time_seconds": 10  # less than 60
            }

            with pytest.raises(WebSocketLifeTimeException) as excinfo:
                launch_data_listener(config=config)

            assert str(excinfo.value) == "Bad websocket_life_time_seconds"

        def test_given_valid_config_then_archiver_facade_is_returned(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70
            }

            data_sink_facade = launch_data_listener(config=config)

            assert isinstance(data_sink_facade, ListenerFacade), "ArchiverFacade should be returned"

            data_sink_facade.shutdown()
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()
