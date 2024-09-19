import re
import threading
import time
from datetime import datetime, timezone

import pytest

import binance_archiver.binance_archiver.difference_depth_queue
from binance_archiver.binance_archiver.archiver_daemon import ArchiverDaemon, BadConfigException, \
    launch_data_sink, BadAzureParameters
from binance_archiver.binance_archiver.difference_depth_queue import DifferenceDepthQueue
from binance_archiver.binance_archiver.market_enum import Market
from binance_archiver.binance_archiver.setup_logger import setup_logger
from binance_archiver.binance_archiver.stream_type_enum import StreamType
from binance_archiver.binance_archiver.trade_queue import TradeQueue


class TestArchiverDaemon:

    def test_init(self):
        assert True

    class TestArchiverDaemonInit:

        def test_given_send_zip_to_blob_is_true_and_azure_blob_parameters_with_key_is_bad_then_is_exception_thrown(self):
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

            azure_blob_parameters_with_key = ''
            container_name = 'some_container_name'

            with pytest.raises(BadAzureParameters) as excinfo:
                data_sink = launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    container_name=container_name
                )

            assert str(excinfo.value) == "Azure blob parameters with key or container name is missing or empty"

        def test_given_send_zip_to_blob_is_true_and_container_name_is_bad_then_is_exception_thrown(self):
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
            container_name = ''

            with pytest.raises(BadAzureParameters) as excinfo:
                data_sink = launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    container_name=container_name
                )

            assert str(excinfo.value) == "Azure blob parameters with key or container name is missing or empty"

        def test_given_archiver_daemon_when_init_then_global_shutdown_flag_is_false(self):
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
            logger = setup_logger()
            archiver_daemon = ArchiverDaemon(instruments=config['instruments'], logger=logger)

            assert not archiver_daemon.global_shutdown_flag.is_set()
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_archiver_daemon_when_init_then_queues_are_set_properly(self):
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

            logger = setup_logger()
            archiver_daemon = ArchiverDaemon(instruments=config['instruments'], logger=logger)

            assert isinstance(archiver_daemon.spot_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(archiver_daemon.usd_m_futures_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(archiver_daemon.coin_m_orderbook_stream_message_queue, DifferenceDepthQueue)

            assert isinstance(archiver_daemon.spot_trade_stream_message_queue, TradeQueue)
            assert isinstance(archiver_daemon.usd_m_futures_trade_stream_message_queue, TradeQueue)
            assert isinstance(archiver_daemon.coin_m_trade_stream_message_queue, TradeQueue)

            assert len(TradeQueue._instances) == 3
            assert len(DifferenceDepthQueue._instances) == 3

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_archiver_daemon_when_init_then_queues_amount_is_accurate(self):
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

            logger = setup_logger()
            archiver_daemon = ArchiverDaemon(instruments=config['instruments'], logger=logger)

            assert len(TradeQueue._instances) == 3
            assert len(DifferenceDepthQueue._instances) == 3

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_archiver_daemon_when_init_then_7_trade_queue_instances_exception_is_thrown(self):
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

            logger = setup_logger()
            archiver_daemon = ArchiverDaemon(instruments=config['instruments'], logger=logger)

            archiver_daemon.fourth = TradeQueue(market=Market.SPOT)

            with pytest.raises(
                    binance_archiver.binance_archiver.trade_queue.ClassInstancesAmountLimitException
            ) as excinfo:
                archiver_daemon.seventh = TradeQueue(market=Market.SPOT)

            assert str(excinfo.value) == "Cannot create more than 4 instances of TradeQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_archiver_daemon_when_init_then_7_difference_depth_queue_instances_is_exception_is_thrown(self):

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

            logger = setup_logger()
            archiver_daemon = ArchiverDaemon(
                instruments=config['instruments'],
                logger=logger)

            archiver_daemon.fourth = DifferenceDepthQueue(market=Market.SPOT)

            with pytest.raises(
                    binance_archiver.binance_archiver.difference_depth_queue.ClassInstancesAmountLimitException
            ) as excinfo:
                archiver_daemon.seventh = DifferenceDepthQueue(market=Market.SPOT)

            assert str(excinfo.value) == f"Cannot create more than 4 instances of DifferenceDepthQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

    class TestArchiverDaemonGetQueue:

        def test_given_archiver_daemon_when__get_queue_is_called_then_accurate_queue_hook_returned(self):
            logger = setup_logger()
            archiver_daemon = ArchiverDaemon(
                instruments={},
                logger=logger)

            expected_queues = {
                (Market.SPOT, StreamType.DIFFERENCE_DEPTH): DifferenceDepthQueue,
                (Market.SPOT, StreamType.TRADE): TradeQueue,
                (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH): DifferenceDepthQueue,
                (Market.USD_M_FUTURES, StreamType.TRADE): TradeQueue,
                (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH): DifferenceDepthQueue,
                (Market.COIN_M_FUTURES, StreamType.TRADE): TradeQueue
            }

            for (market, stream_type), expected_queue_type in expected_queues.items():
                hooked_queue = archiver_daemon._get_queue(market=market, stream_type=stream_type)
                assert isinstance(hooked_queue, expected_queue_type), (f"Queue for {market}, {stream_type} is not of type"
                                                                       f" {expected_queue_type}")
                assert hooked_queue.market == market, (f"Queue market {hooked_queue.market} does not match expected market "
                                                       f"{market}")

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

    class TestArchiverDaemonShutdown:

        @pytest.mark.parametrize('execution_number', range(3))
        def test_given_archiver_daemon_when_shutdown_method_during_no_stream_switch_is_called_then_no_threads_are_left(
                self,
                execution_number
        ):
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

            time.sleep(15)

            archiver_daemon.shutdown()

            for _ in range(20):
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

        @pytest.mark.parametrize('execution_number', range(3))
        def test_given_archiver_daemon_when_shutdown_method_during_stream_switch_is_called_then_no_threads_are_left(
                self,
                execution_number
        ):
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
                "websocket_life_time_seconds": 5,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            archiver_daemon = launch_data_sink(config)

            time.sleep(5)

            archiver_daemon.shutdown()

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

            assert len(active_threads) == 0, (f"Still active threads after run {execution_number + 1}"
                                              f": {[thread.name for thread in active_threads]}")

            del archiver_daemon

    class TestArchiverDaemonRun:

        def test_given_archiver_daemon_run_call_when_threads_invocation_then_accurate_set_of_threads_are_started(self):

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

            archiver_daemon = launch_data_sink(config)

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

            for _ in thread_names: print(_)

            for market in ["SPOT", "USD_M_FUTURES", "COIN_M_FUTURES"]:
                assert f'stream_service: market: {Market[market]}, stream_type: {StreamType.DIFFERENCE_DEPTH}' in thread_names, f'bad stream_service: market: {market}, stream_type:{StreamType.DIFFERENCE_DEPTH}'
                assert f'stream_service: market: {Market[market]}, stream_type: {StreamType.TRADE}' in thread_names, f'bad stream_service: market: {market}, stream_type:{StreamType.TRADE}'
                assert f'stream_writer: market: {Market[market]}, stream_type: {StreamType.DIFFERENCE_DEPTH}' in thread_names, f'bad stream_writer: market: {market}, stream_type:{StreamType.DIFFERENCE_DEPTH}'
                assert f'stream_writer: market: {Market[market]}, stream_type: {StreamType.TRADE}' in thread_names, f'bad stream_writer: market: {market}, stream_type:{StreamType.TRADE}'
                assert f'snapshot_daemon: market: {Market[market]}' in thread_names, 'bad amount of snapshot daemons'

            for _ in daemon_threads:
                print(_)

            assert len(daemon_threads) == total_expected_threads

            archiver_daemon.shutdown()

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

    class TestArchiverDaemonStreamService:

        @pytest.mark.skip
        def test_given_difference_stream_service_when_start_then_stream_listeners_differs_couple_of_times(self):
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
                "websocket_life_time_seconds": 5,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            archiver_daemon = launch_data_sink(config)

            switch_count = 0
            id_change_count = 0

            def monitor_switch_indicator():
                timeout_seconds = 120  # Increased timeout
                start_time = time.time()
                nonlocal switch_count

                while switch_count < 3 and time.time() - start_time < timeout_seconds:
                    #currently abandoned as switch time takes too long
                    if archiver_daemon.spot_orderbook_stream_message_queue.did_websockets_switch_successfully:
                        switch_count += 1
                        archiver_daemon.spot_orderbook_stream_message_queue.did_websockets_switch_successfully = False  # Reset
                        print(f'switch_count {switch_count}')

            def monitor_currently_accepted_stream_id():
                timeout_seconds = 120
                start_time = time.time()
                nonlocal id_change_count
                current_id = archiver_daemon.spot_orderbook_stream_message_queue.currently_accepted_stream_id

                while id_change_count < 3 and time.time() - start_time < timeout_seconds:
                    #currently abandoned as switch time takes too long
                    new_id = archiver_daemon.spot_orderbook_stream_message_queue.currently_accepted_stream_id
                    if new_id != current_id:
                        id_change_count += 1
                        current_id = new_id
                        print(f'id_change_count {id_change_count}')

            switch_thread = threading.Thread(target=monitor_switch_indicator)
            id_thread = threading.Thread(target=monitor_currently_accepted_stream_id)

            switch_thread.start()
            id_thread.start()

            time.sleep(20)

            switch_thread.join()
            id_thread.join()

            archiver_daemon.shutdown()

            assert id_change_count == 3, 'id_change_count not accurate'
            assert switch_count == 3, 'switch_count not accurate'

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

    class TestArchiverDaemonTimeUtils:

        def test_get_utc_formatted_timestamp(self):
            timestamp = ArchiverDaemon._get_utc_formatted_timestamp()
            pattern = re.compile(r'\d{2}-\d{2}-\d{4}T\d{2}-\d{2}-\d{2}Z')
            assert pattern.match(timestamp), f"Timestamp {timestamp} does not match the expected format %d-%m-%YT%H-%M-%SZ"

        def test_get_utc_timestamp_epoch_milliseconds(self):
            timestamp_milliseconds_method = ArchiverDaemon._get_utc_timestamp_epoch_milliseconds()
            timestamp_milliseconds_now = round(datetime.now(timezone.utc).timestamp() * 1000)

            assert (abs(timestamp_milliseconds_method - timestamp_milliseconds_now) < 2000,
                    "The timestamp in milliseconds is not accurate or not in UTC.")

        def test_get_utc_timestamp_epoch_seconds(self):
            timestamp_seconds_method = ArchiverDaemon._get_utc_timestamp_epoch_seconds()
            timestamp_seconds_now = round(datetime.now(timezone.utc).timestamp())

            assert (abs(timestamp_seconds_method - timestamp_seconds_now) < 2,
                    "The timestamp in seconds is not accurate or not in UTC.")

        def test_given_get_actual_epoch_timestamp_are_timestamps_in_utc(self):
            timestamp_seconds_method = ArchiverDaemon._get_utc_timestamp_epoch_seconds()
            timestamp_milliseconds_method = ArchiverDaemon._get_utc_timestamp_epoch_milliseconds()

            datetime_seconds = datetime.fromtimestamp(timestamp_seconds_method, tz=timezone.utc)
            datetime_milliseconds = datetime.fromtimestamp(timestamp_milliseconds_method / 1000, tz=timezone.utc)

            assert datetime_seconds.tzinfo == timezone.utc, "The timestamp in seconds is not in UTC."
            assert datetime_milliseconds.tzinfo == timezone.utc, "The timestamp in milliseconds is not in UTC."

    class TestLaunchDataSink:

        def test_given_config_has_no_instrument_then_is_exception_thrown(self):

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
                    container_name=container_name
                )

            assert str(excinfo.value) == "Instruments config is missing or not a dictionary."

        def test_given_market_type_is_empty_then_is_exception_thrown(self):
            config = {
                "instruments": {
                    "spot": [],  # Empty market type
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
                    container_name=container_name
                )

            assert str(excinfo.value) == "Pairs for market spot are missing or invalid."

        def test_given_too_many_markets_then_is_exception_thrown(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"],
                    "actions": ["AAPL", "TSLA"],  # Extra market
                    "mining": ["BTCMINING"]  # Extra market
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
                    container_name=container_name
                )

            assert str(excinfo.value) == "Config must contain 1 to 3 markets."

        def test_given_not_handled_market_type_then_is_exception_thrown(self):
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
                    container_name=container_name
                )

            assert str(excinfo.value) == "Invalid or not handled market: mining"
