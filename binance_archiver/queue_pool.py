from __future__ import annotations

from queue import Queue

from binance_archiver.difference_depth_queue import DifferenceDepthQueue
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.trade_queue import TradeQueue


class QueuePoolListener:

    __slots__ = [
        'global_queue',
        'spot_orderbook_stream_message_queue',
        'spot_trade_stream_message_queue',
        'usd_m_futures_orderbook_stream_message_queue',
        'usd_m_futures_trade_stream_message_queue',
        'coin_m_orderbook_stream_message_queue',
        'coin_m_trade_stream_message_queue',
        'queue_lookup'
    ]

    def __init__(self):
        self.global_queue = Queue()

        self.spot_orderbook_stream_message_queue = DifferenceDepthQueue(market=Market.SPOT, global_queue=self.global_queue)
        self.spot_trade_stream_message_queue = TradeQueue(market=Market.SPOT, global_queue=self.global_queue)
        self.usd_m_futures_orderbook_stream_message_queue = DifferenceDepthQueue(market=Market.USD_M_FUTURES, global_queue=self.global_queue)
        self.usd_m_futures_trade_stream_message_queue = TradeQueue(market=Market.USD_M_FUTURES, global_queue=self.global_queue)
        self.coin_m_orderbook_stream_message_queue = DifferenceDepthQueue(market=Market.COIN_M_FUTURES, global_queue=self.global_queue)
        self.coin_m_trade_stream_message_queue = TradeQueue(market=Market.COIN_M_FUTURES, global_queue=self.global_queue)

        self.queue_lookup = {
            (Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM): self.spot_orderbook_stream_message_queue,
            (Market.SPOT, StreamType.TRADE_STREAM): self.spot_trade_stream_message_queue,
            (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM): self.usd_m_futures_orderbook_stream_message_queue,
            (Market.USD_M_FUTURES, StreamType.TRADE_STREAM): self.usd_m_futures_trade_stream_message_queue,
            (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM): self.coin_m_orderbook_stream_message_queue,
            (Market.COIN_M_FUTURES, StreamType.TRADE_STREAM): self.coin_m_trade_stream_message_queue,
        }

    def get_queue(self, market: Market, stream_type: StreamType) -> DifferenceDepthQueue | TradeQueue:
        return self.queue_lookup.get((market, stream_type))


class QueuePoolDataSink:

    __slots__ = [
        'spot_orderbook_stream_message_queue',
        'spot_trade_stream_message_queue',
        'usd_m_futures_orderbook_stream_message_queue',
        'usd_m_futures_trade_stream_message_queue',
        'coin_m_orderbook_stream_message_queue',
        'coin_m_trade_stream_message_queue',
        'queue_lookup'
    ]

    def __init__(self):

        self.spot_orderbook_stream_message_queue = DifferenceDepthQueue(market=Market.SPOT)
        self.spot_trade_stream_message_queue = TradeQueue(market=Market.SPOT)
        self.usd_m_futures_orderbook_stream_message_queue = DifferenceDepthQueue(market=Market.USD_M_FUTURES)
        self.usd_m_futures_trade_stream_message_queue = TradeQueue(market=Market.USD_M_FUTURES)
        self.coin_m_orderbook_stream_message_queue = DifferenceDepthQueue(market=Market.COIN_M_FUTURES)
        self.coin_m_trade_stream_message_queue = TradeQueue(market=Market.COIN_M_FUTURES)

        self.queue_lookup = {
            (Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM): self.spot_orderbook_stream_message_queue,
            (Market.SPOT, StreamType.TRADE_STREAM): self.spot_trade_stream_message_queue,
            (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM): self.usd_m_futures_orderbook_stream_message_queue,
            (Market.USD_M_FUTURES, StreamType.TRADE_STREAM): self.usd_m_futures_trade_stream_message_queue,
            (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM): self.coin_m_orderbook_stream_message_queue,
            (Market.COIN_M_FUTURES, StreamType.TRADE_STREAM): self.coin_m_trade_stream_message_queue,
        }

    def get_queue(self, market: Market, stream_type: StreamType) -> DifferenceDepthQueue | TradeQueue:
        return self.queue_lookup.get((market, stream_type))
