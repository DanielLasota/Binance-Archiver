import json
import logging
import time
import threading
import pytest
import websocket

from binance_archiver.queue_pool import QueuePoolListener, QueuePoolDataSink
from binance_archiver.difference_depth_queue import DifferenceDepthQueue
from binance_archiver.enum_.market_enum import Market
from binance_archiver.setup_logger import setup_logger
from binance_archiver.stream_id import StreamId
from binance_archiver.stream_listener import StreamListener, WrongListInstanceException, \
    PairsLengthException
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.blackout_supervisor import BlackoutSupervisor
from binance_archiver.trade_queue import TradeQueue


class TestStreamListener:

    def test_given_stream_listener_when_init_then_stream_listener_initializes_with_valid_parameters(self):
        config = {
            "instruments": {
                "spot": [
                    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
                    "ADAUSDT", "SHIBUSDT", "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"
                ],
                "usd_m_futures": [
                    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
                    "ADAUSDT", "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"
                ],
                "coin_m_futures": [
                    "BTCUSD_PERP", "ETHUSD_PERP", "BNBUSD_PERP", "SOLUSD_PERP", "XRPUSD_PERP",
                    "DOGEUSD_PERP", "ADAUSD_PERP", "LTCUSD_PERP", "AVAXUSD_PERP", "TRXUSD_PERP",
                    "DOTUSD_PERP"
                ]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 60,
            "websocket_life_time_seconds": 5,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        logger = setup_logger()
        instruments = config['instruments']
        global_shutdown_flag = threading.Event()

        queue_pool = QueuePoolDataSink()

        for market_str in ['spot', 'usd_m_futures', 'coin_m_futures']:
            market = Market[market_str.upper()]
            pairs = instruments[market_str]

            queue = queue_pool.get_queue(market, StreamType.DIFFERENCE_DEPTH_STREAM)
            difference_depth_stream_listener = StreamListener(
                logger=logger,
                queue=queue,
                pairs=pairs,
                stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                market=market
            )

            expected_pairs_amount = len(pairs)

            assert isinstance(difference_depth_stream_listener.websocket_app, websocket.WebSocketApp)
            assert isinstance(difference_depth_stream_listener.id, StreamId)
            assert difference_depth_stream_listener.id.pairs_amount == expected_pairs_amount
            assert difference_depth_stream_listener.websocket_app.on_message.__name__ == "_on_difference_depth_message", \
                "on_message should be assigned to _on_difference_depth_message when stream_type is DIFFERENCE_DEPTH"

            queue = queue_pool.get_queue(market, StreamType.TRADE_STREAM)
            trade_stream_listener = StreamListener(
                logger=logger,
                queue=queue,
                pairs=pairs,
                stream_type=StreamType.TRADE_STREAM,
                market=market
            )

            assert isinstance(trade_stream_listener.websocket_app, websocket.WebSocketApp)
            assert isinstance(trade_stream_listener.id, StreamId)
            assert trade_stream_listener.id.pairs_amount == expected_pairs_amount
            assert trade_stream_listener.websocket_app.on_message.__name__ == "_on_trade_message", \
                "on_message should be assigned to _on_trade_message when stream_type is TRADE"

            trade_stream_listener.websocket_app.close()
            trade_stream_listener._blackout_supervisor.shutdown_supervisor()
            difference_depth_stream_listener.websocket_app.close()
            difference_depth_stream_listener._blackout_supervisor.shutdown_supervisor()

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_stream_listener_when_init_with_pairs_argument_as_str_then_exception_is_thrown(self):
        config = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 10,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        logger = setup_logger()
        instruments = config['instruments']
        queue_pool = QueuePoolDataSink()

        with pytest.raises(WrongListInstanceException) as excinfo:
            queue = queue_pool.get_queue(Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM)
            stream_listener = StreamListener(
                logger=logger,
                queue=queue,
                pairs=instruments['spot'][0],  # Incorrect type as intended to be should be a list
                stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                market=Market.SPOT
            )

        assert str(excinfo.value) == "pairs argument is not a list"

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_stream_listener_when_init_with_pairs_amount_of_zero_then_exception_is_thrown(self):
        config = {
            "instruments": {
                "spot": [],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 10,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        logger = setup_logger()
        instruments = config['instruments']
        queue_pool = QueuePoolDataSink()

        with pytest.raises(PairsLengthException) as excinfo:
            pairs = instruments['spot']
            queue = queue_pool.get_queue(Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM)
            stream_listener = StreamListener(
                logger=logger,
                queue=queue,
                pairs=pairs,
                stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                market=Market.SPOT
            )

        assert str(excinfo.value) == "pairs len is zero"

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_trade_stream_listener_when_on_open_then_message_is_being_logged(self, caplog):
        pairs = ['BTCUSDT']
        queue = TradeQueue(market=Market.SPOT)
        stream_listener = StreamListener(
            logger=setup_logger(),
            queue=queue,
            pairs=pairs,
            stream_type=StreamType.TRADE_STREAM,
            market=Market.SPOT
        )

        with caplog.at_level(logging.INFO):
            stream_listener.websocket_app.on_open(stream_listener.websocket_app)

        assert "WebSocket connection opened" in caplog.text

        stream_listener.websocket_app.close()
        stream_listener._blackout_supervisor.shutdown_supervisor()

        TradeQueue.clear_instances()

    def test_given_trade_stream_listener_when_on_close_then_close_message_is_being_logged(self, caplog):
        pairs = ['BTCUSDT']
        queue = TradeQueue(market=Market.SPOT)
        stream_listener = StreamListener(
            logger=setup_logger(),
            queue=queue,
            pairs=pairs,
            stream_type=StreamType.TRADE_STREAM,
            market=Market.SPOT
        )

        close_status_code = 1000
        close_msg = "Normal closure"

        with caplog.at_level(logging.INFO):
            stream_listener.websocket_app.on_close(stream_listener.websocket_app, close_status_code, close_msg)

        assert "WebSocket connection closed" in caplog.text
        assert close_msg in caplog.text
        assert str(close_status_code) in caplog.text

        stream_listener.websocket_app.close()
        stream_listener._blackout_supervisor.shutdown_supervisor()

        TradeQueue.clear_instances()

    def test_given_trade_stream_listener_when_connected_then_error_is_being_logged(self, caplog):
        logger=setup_logger()
        pairs = ['BTCUSDT']
        queue = TradeQueue(market=Market.SPOT)
        stream_listener = StreamListener(
            logger=logger,
            queue=queue,
            pairs=pairs,
            stream_type=StreamType.TRADE_STREAM,
            market=Market.SPOT)

        error_message = "Test error"

        with caplog.at_level(logging.ERROR):
            stream_listener.websocket_app.on_error(stream_listener.websocket_app, error_message)

        assert error_message in caplog.text

        stream_listener.websocket_app.close()
        stream_listener._blackout_supervisor.shutdown_supervisor()
        TradeQueue.clear_instances()

    def test_given_trade_stream_listener_when_connected_then_message_is_correctly_passed_to_trade_queue(self):

        logger = setup_logger()
        config = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 10,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        trade_queue = TradeQueue(market=Market.SPOT)

        trade_stream_listener = StreamListener(
            logger=logger,
            queue=trade_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.TRADE_STREAM,
            market=Market.SPOT
        )

        trade_queue.currently_accepted_stream_id = trade_stream_listener.id

        trade_stream_listener_thread = threading.Thread(
            target=trade_stream_listener.websocket_app.run_forever
        )

        trade_stream_listener_thread.start()

        time.sleep(5)

        trade_stream_listener.websocket_app.close()

        sample_message = trade_queue.get_nowait()
        sample_message_dict = json.loads(sample_message[0])

        assert trade_stream_listener.websocket_app.on_message.__name__ == "_on_trade_message", \
            "on_message should be assigned to _on_trade_message when stream_type is TRADE"
        assert trade_queue.qsize() > 0
        assert sample_message_dict['stream'].split('@')[1] == 'trade'
        assert sample_message_dict['data']['e'] == 'trade'

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_difference_depth_stream_listener_when_connected_then_message_is_correctly_passed_to_diff_queue(self):
        logger = setup_logger()

        config = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 10,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        difference_depth_stream_listener = StreamListener(
            logger=logger,
            queue=difference_depth_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
            market=Market.SPOT
        )

        difference_depth_queue.currently_accepted_stream_id = difference_depth_stream_listener.id.id

        difference_depth_stream_listener_thread = threading.Thread(
            target=difference_depth_stream_listener.websocket_app.run_forever
        )

        difference_depth_stream_listener_thread.start()

        time.sleep(10)

        difference_depth_stream_listener.websocket_app.close()

        sample_message = difference_depth_queue.queue.get_nowait()
        sample_message_dict = json.loads(sample_message[0])

        assert difference_depth_stream_listener.websocket_app.on_message.__name__ == "_on_difference_depth_message", \
            "on_message should be assigned to _on_difference_depth_message when stream_type is DIFFERENCE_DEPTH"
        assert difference_depth_queue.qsize() > 0
        assert sample_message_dict['stream'].split('@')[1] == 'depth'
        assert sample_message_dict['data']['e'] == 'depthUpdate'

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_trade_stream_listener_when_init_then_supervisor_starts_correctly_and_is_being_notified(self):
        logger = setup_logger()

        from unittest.mock import patch

        config = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 100,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        trade_queue = TradeQueue(market=Market.SPOT)

        trade_stream_listener = StreamListener(
            logger=logger,
            queue=trade_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.TRADE_STREAM,
            market=Market.SPOT
        )

        trade_queue.currently_accepted_stream_id = trade_stream_listener.id

        with patch.object(StreamListener, 'restart_websocket_app') as mock_restart, \
                patch.object(BlackoutSupervisor, 'notify') as mock_notify:
            trade_stream_listener.start_websocket_app()

            time.sleep(10)

            mock_notify.assert_called()

            mock_restart.assert_not_called()

        active_threads = [
            thread.name for thread in threading.enumerate()
            if thread is not threading.current_thread()
        ]

        presumed_thread_name = f'stream_listener blackout supervisor {StreamType.TRADE_STREAM} {Market.SPOT}'

        assert trade_stream_listener._blackout_supervisor is not None, (
            "Supervisor should be instantiated within StreamListener"
        )
        assert isinstance(trade_stream_listener._blackout_supervisor, BlackoutSupervisor)
        for name_ in active_threads:
            print(name_)
        assert presumed_thread_name in active_threads
        assert len(active_threads) == 2
        trade_stream_listener.websocket_app.close()
        TradeQueue.clear_instances()

    def test_given_difference_depth_stream_listener_when_init_then_supervisor_starts_correctly_and_is_being_notified(self):

        from unittest.mock import patch

        config = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 100,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        difference_depth_queue_listener = StreamListener(
            logger=setup_logger(),
            queue=difference_depth_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
            market=Market.SPOT
        )

        with patch.object(StreamListener, 'restart_websocket_app') as mock_restart, \
                patch.object(BlackoutSupervisor, 'notify') as mock_notify:
            difference_depth_queue_listener.start_websocket_app()

            time.sleep(10)
            mock_notify.assert_called()

            mock_restart.assert_not_called()

        active_threads = [
            thread.name for thread in threading.enumerate()
            if thread is not threading.current_thread()
        ]

        presumed_thread_name = f'stream_listener blackout supervisor {StreamType.DIFFERENCE_DEPTH_STREAM} {Market.SPOT}'

        assert difference_depth_queue_listener._blackout_supervisor is not None, "Supervisor should be instantiated within StreamListener"
        assert isinstance(difference_depth_queue_listener._blackout_supervisor, BlackoutSupervisor)
        assert presumed_thread_name in active_threads
        assert len(active_threads) == 2

        difference_depth_queue_listener.websocket_app.close()

        DifferenceDepthQueue.clear_instances()

    @pytest.mark.skip
    def test_given_trade_stream_listener_when_message_income_stops_then_supervisors_sets_flag_to_stop(self):
        config = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 100,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        trade_queue = TradeQueue(market=Market.SPOT)

        trade_stream_listener = StreamListener(
            queue=trade_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.TRADE_STREAM,
            market=Market.SPOT
        )

        trade_stream_listener_thread = threading.Thread(target=trade_stream_listener.websocket_app.run_forever)
        trade_stream_listener_thread.start()

        time.sleep(5)

        trade_stream_listener.websocket_app.on_message = None
        time.sleep(12)

        assert trade_stream_listener.supervisor_signal_shutdown_flag.is_set(), \
            "Supervisor's shutdown signal should be set after a period without incoming messages."

        trade_stream_listener.websocket_app.close()
        TradeQueue.clear_instances()

    @pytest.mark.skip
    def test_given_difference_depth_stream_listener_when_message_income_stops_then_supervisors_sets_flag_to_stop(self):
        config = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 100,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        difference_depth_queue_listener = StreamListener(
            queue=difference_depth_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
            market=Market.SPOT
        )

        difference_depth_stream_listener_thread = threading.Thread(
            target=difference_depth_queue_listener.websocket_app.run_forever)
        difference_depth_stream_listener_thread.start()

        time.sleep(5)

        difference_depth_queue_listener.websocket_app.on_message = None
        time.sleep(12)

        assert difference_depth_queue_listener.supervisor_signal_shutdown_flag.is_set(), \
            "Supervisor's shutdown signal should be set after a period without incoming messages."

        difference_depth_queue_listener.websocket_app.close()
        DifferenceDepthQueue.clear_instances()

    @pytest.mark.skip
    def test_given_trade_stream_listener_when_message_received_then_supervisor_is_notified(self):
        from unittest.mock import patch

        config = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 100,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        trade_queue = TradeQueue(market=Market.SPOT)

        trade_stream_listener = StreamListener(
            queue=trade_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.TRADE_STREAM,
            market=Market.SPOT
        )

        with patch.object(trade_stream_listener._blackout_supervisor, 'notify') as mock_notify:
            simulated_message = '{"e": "trade", "E": 123456789, "s": "BTCUSDT", "t": 12345, "p": "0.001", "q": "100", "b": 88, "a": 50, "T": 123456785, "m": True, "M": True}'

            trade_stream_listener.websocket_app.on_message(trade_stream_listener.websocket_app, simulated_message)

            mock_notify.assert_called_once()

        trade_stream_listener.websocket_app.close()
        TradeQueue.clear_instances()

    @pytest.mark.skip
    def test_given_difference_depth_stream_listener_when_message_received_then_supervisor_is_notified(self):
        from unittest.mock import patch

        config = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 100,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        difference_depth_queue_listener = StreamListener(
            queue=difference_depth_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
            market=Market.SPOT
        )

        with patch.object(difference_depth_queue_listener._blackout_supervisor, 'notify') as mock_notify:
            simulated_message = '{"e": "depthUpdate", "E": 123456789, "s": "BTCUSDT", "U": 157, "u": 160, "b": [["0.0024", "10"]], "a": [["0.0026", "100"]]}'

            difference_depth_queue_listener.websocket_app.on_message(difference_depth_queue_listener.websocket_app,
                                                                     simulated_message)

            mock_notify.assert_called_once()

        difference_depth_queue_listener.websocket_app.close()
        DifferenceDepthQueue.clear_instances()

    def test_given_trade_stream_listener_when_websocket_app_close_then_supervisor_is_properly_stopped(self):
        ...

    def test_given_difference_depth_stream_listener_when_websocket_app_close_then_supervisor_is_properly_stopped(self):
        ...
