# -*— coding:utf-8 -*-

"""
OKEx Future Market Server.
https://www.okex.com/docs/zh/#futures_ws-all

Author: HuangTao
Date:   2018/12/20
Email:  huangtao@ifclover.com
"""

import zlib
import json
import copy

from quant import const
from quant.utils import tools
from quant.utils import logger
from quant.utils.websocket import Websocket
from quant.event import EventOrderbook, EventKline, EventTrade
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL


class OKExFuture(Websocket):
    """ OKEx Future Market Server.

    Attributes:
        kwargs:
            platform: Exchange platform name, must be `okex_future`.
            wss: Exchange Websocket host address, default is "wss://real.okex.com:10442".
            symbols: symbol list, OKEx Future instrument_id list.
            channels: channel list, only `orderbook` , `kline` and `trade` to be enabled.
    """

    def __init__(self, **kwargs):
        self._platform = kwargs["platform"]
        self._wss = kwargs.get("wss", "wss://real.okex.com:10442")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")

        self._orderbooks = {}  # orderbook data, e.g. {"symbol": {"bids": {"price": quantity, ...}, "asks": {...}}}
        self._length = 20  # orderbook's length to be published in OrderbookEvent.

        url = self._wss + "/ws/v3"
        super(OKExFuture, self).__init__(url)
        self.heartbeat_msg = "ping"
        self.initialize()

    async def connected_callback(self):
        """ After create connection to Websocket server successfully, we will subscribe orderbook/kline/trade event.
        """
        ches = []
        for ch in self._channels:
            if ch == "orderbook":
                for symbol in self._symbols:
                    ch = "futures/depth:{s}".format(s=symbol)
                    ches.append(ch)
            elif ch == "trade":
                for symbol in self._symbols:
                    ch = "futures/trade:{s}".format(s=symbol.replace("/", '-'))
                    ches.append(ch)
            elif ch == "kline":
                for symbol in self._symbols:
                    ch = "futures/candle60s:{s}".format(s=symbol.replace("/", '-'))
                    ches.append(ch)
            else:
                logger.error("channel error! channel:", ch, caller=self)
            if len(ches) > 0:
                msg = {
                    "op": "subscribe",
                    "args": ches
                }
                await self.ws.send_json(msg)
                logger.info("subscribe orderbook/kline/trade success.", caller=self)

    async def process_binary(self, raw):
        """ Process message that received from Websocket connection.

        Args:
            raw: Raw binary message received from Websocket connection.
        """
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        msg = decompress.decompress(raw)
        msg += decompress.flush()
        msg = msg.decode()
        if msg == "pong":  # Heartbeat message.
            return
        msg = json.loads(msg)
        logger.debug("msg:", msg, caller=self)

        table = msg.get("table")
        if table == "futures/depth":
            if msg.get("action") == "partial":
                for d in msg["data"]:
                    await self.deal_orderbook_partial(d)
            elif msg.get("action") == "update":
                for d in msg["data"]:
                    await self.deal_orderbook_update(d)
            else:
                logger.warn("unhandle msg:", msg, caller=self)
        elif table == "futures/trade":
            for d in msg["data"]:
                await self.deal_trade_update(d)
        elif table == "futures/candle60s":
            for d in msg["data"]:
                await self.deal_kline_update(d)
        else:
            logger.warn("unhandle msg:", msg, caller=self)

    async def deal_orderbook_partial(self, data):
        """ Deal with orderbook partial message.
        """
        symbol = data.get("instrument_id")
        if symbol not in self._symbols:
            return
        asks = data.get("asks")
        bids = data.get("bids")
        self._orderbooks[symbol] = {"asks": {}, "bids": {}, "timestamp": 0}
        for ask in asks:
            price = float(ask[0])
            quantity = int(ask[1])
            self._orderbooks[symbol]["asks"][price] = quantity
        for bid in bids:
            price = float(bid[0])
            quantity = int(bid[1])
            self._orderbooks[symbol]["bids"][price] = quantity
        timestamp = tools.utctime_str_to_mts(data.get("timestamp"))
        self._orderbooks[symbol]["timestamp"] = timestamp

    async def deal_orderbook_update(self, data):
        """ Deal with orderbook update message.
        """
        symbol = data.get("instrument_id")
        asks = data.get("asks")
        bids = data.get("bids")
        timestamp = tools.utctime_str_to_mts(data.get("timestamp"))

        if symbol not in self._orderbooks:
            return
        self._orderbooks[symbol]["timestamp"] = timestamp

        for ask in asks:
            price = float(ask[0])
            quantity = int(ask[1])
            if quantity == 0 and price in self._orderbooks[symbol]["asks"]:
                self._orderbooks[symbol]["asks"].pop(price)
            else:
                self._orderbooks[symbol]["asks"][price] = quantity

        for bid in bids:
            price = float(bid[0])
            quantity = int(bid[1])
            if quantity == 0 and price in self._orderbooks[symbol]["bids"]:
                self._orderbooks[symbol]["bids"].pop(price)
            else:
                self._orderbooks[symbol]["bids"][price] = quantity

        await self.publish_orderbook()

    async def publish_orderbook(self, *args, **kwargs):
        """ Publish orderbook message to EventCenter via OrderbookEvent.
        """
        for symbol, data in self._orderbooks.items():
            ob = copy.copy(data)
            if not ob["asks"] or not ob["bids"]:
                logger.warn("symbol:", symbol, "asks:", ob["asks"], "bids:", ob["bids"], caller=self)
                continue

            ask_keys = sorted(list(ob["asks"].keys()))
            bid_keys = sorted(list(ob["bids"].keys()), reverse=True)
            if ask_keys[0] <= bid_keys[0]:
                logger.warn("symbol:", symbol, "ask1:", ask_keys[0], "bid1:", bid_keys[0], caller=self)
                continue

            # 卖
            asks = []
            for k in ask_keys[:self._length]:
                price = "%.8f" % k
                quantity = str(ob["asks"].get(k))
                asks.append([price, quantity])

            # 买
            bids = []
            for k in bid_keys[:self._length]:
                price = "%.8f" % k
                quantity = str(ob["bids"].get(k))
                bids.append([price, quantity])

            # 推送订单薄数据
            orderbook = {
                "platform": self._platform,
                "symbol": symbol,
                "asks": asks,
                "bids": bids,
                "timestamp": ob["timestamp"]
            }
            EventOrderbook(**orderbook).publish()
            logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)

    async def deal_trade_update(self, data):
        """ Deal with trade data, and publish trade message to EventCenter via TradeEvent.
        """
        symbol = data["instrument_id"]
        if symbol not in self._symbols:
            return
        action = ORDER_ACTION_BUY if data["side"] == "buy" else ORDER_ACTION_SELL
        price = "%.8f" % float(data["price"])
        quantity = "%.8f" % float(data["qty"])
        timestamp = tools.utctime_str_to_mts(data["timestamp"])

        # Publish EventTrade.
        trade = {
            "platform": self._platform,
            "symbol": symbol,
            "action": action,
            "price": price,
            "quantity": quantity,
            "timestamp": timestamp
        }
        EventTrade(**trade).publish()
        logger.info("symbol:", symbol, "trade:", trade, caller=self)

    async def deal_kline_update(self, data):
        """ Deal with 1min kline data, and publish kline message to EventCenter via KlineEvent.

        Args:
            data: Newest kline data.
        """
        symbol = data["instrument_id"]
        if symbol not in self._symbols:
            return
        timestamp = tools.utctime_str_to_mts(data["candle"][0])
        _open = "%.8f" % float(data["candle"][1])
        high = "%.8f" % float(data["candle"][2])
        low = "%.8f" % float(data["candle"][3])
        close = "%.8f" % float(data["candle"][4])
        volume = "%.8f" % float(data["candle"][5])

        # Publish EventKline
        kline = {
            "platform": self._platform,
            "symbol": symbol,
            "open": _open,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "timestamp": timestamp,
            "kline_type": const.MARKET_TYPE_KLINE
        }
        EventKline(**kline).publish()
        logger.info("symbol:", symbol, "kline:", kline, caller=self)
