# -*— coding:utf-8 -*-

"""
Bitmex 行情
https://www.bitmex.com/app/wsAPI

Author: HuangTao
Date:   2018/09/13
"""

from quant.utils import tools
from quant.utils import logger
from quant.const import MARKET_TYPE_KLINE
from quant.utils.websocket import Websocket
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.event import EventOrderbook, EventTrade, EventKline


class Bitmex(Websocket):
    """ Bitmex 行情
    """

    def __init__(self, **kwargs):
        self._platform = kwargs["platform"]
        self._wss = kwargs.get("wss", "wss://www.bitmex.com")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")

        self._c_to_s = {}  # {"channel": "symbol"}
        url = self._wss + "/realtime"
        super(Bitmex, self).__init__(url)
        self.initialize()

    async def connected_callback(self):
        """ 建立连接之后，订阅事件 ticker/deals
        """
        channels = []
        for ch in self._channels:
            if ch == "orderbook":  # 订单薄
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "orderBook10")
                    channels.append(channel)
            if ch == "trade":  # 成交数据
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "trade")
                    channels.append(channel)
            if ch == "kline":  # 1分钟K线数据
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "tradeBin1m")
                    channels.append(channel)
        if channels:
            data = {
                "op": "subscribe",
                "args": channels
            }
            await self.ws.send_json(data)
            logger.info("subscribe orderbook/trade/kline success.", caller=self)

    async def process(self, msg):
        """ 处理websocket上接收到的消息
        """
        logger.debug("msg:", msg, caller=self)
        if not isinstance(msg, dict):
            return

        table = msg.get("table")
        if table == "orderBook10":  # 订单薄数据
            for item in msg["data"]:
                symbol = item.get("symbol")
                orderbook = {
                    "platform": self._platform,
                    "symbol": symbol,
                    "asks": item.get("asks"),
                    "bids": item.get("bids"),
                    "timestamp": tools.utctime_str_to_mts(item["timestamp"])
                }
                EventOrderbook(**orderbook).publish()
                logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)
        elif table == "trade":  # 成交数据
            for item in msg["data"]:
                symbol = item["symbol"]
                trade = {
                    "platform": self._platform,
                    "symbol": symbol,
                    "action":  ORDER_ACTION_BUY if item["side"] else ORDER_ACTION_SELL,
                    "price": "%.1f" % item["price"],
                    "quantity": str(item["size"]),
                    "timestamp": tools.utctime_str_to_mts(item["timestamp"])
                }
                EventTrade(**trade).publish()
                logger.info("symbol:", symbol, "trade:", trade, caller=self)
        elif table == "tradeBin1m":  # 1分钟K线数据
            for item in msg["data"]:
                symbol = item["symbol"]
                kline = {
                    "platform": self._platform,
                    "symbol": symbol,
                    "open": "%.1f" % item["open"],  # 开盘价
                    "high": "%.1f" % item["high"],  # 最高价
                    "low": "%.1f" % item["low"],  # 最低价
                    "close": "%.1f" % item["close"],  # 收盘价
                    "volume": str(item["volume"]),  # 交易量
                    "timestamp": tools.utctime_str_to_mts(item["timestamp"]),  # 时间戳
                    "kline_type": MARKET_TYPE_KLINE
                }
                EventKline(**kline).publish()
                logger.info("symbol:", symbol, "kline:", kline, caller=self)

    def _symbol_to_channel(self, symbol, channel_type):
        """ symbol转换到channel
        @param symbol symbol名字
        @param channel_type 订阅频道类型
        """
        channel = "{channel_type}:{symbol}".format(channel_type=channel_type, symbol=symbol)
        self._c_to_s[channel] = symbol
        return channel
