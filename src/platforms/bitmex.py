# -*— coding:utf-8 -*-

"""
Bitmex 行情
https://www.bitmex.com/app/wsAPI

Author: HuangTao
Date:   2018/09/13
"""

from quant.utils import tools
from quant.utils import logger
from quant.config import config
from quant.const import BITMEX
from quant.event import EventOrderbook
from quant.utils.websocket import Websocket


class Bitmex(Websocket):
    """ Bitmex 行情
    """

    def __init__(self):
        self._platform = BITMEX
        self._wss = config.platforms.get(self._platform).get("wss", "wss://www.bitmex.com")
        self._symbols = list(set(config.platforms.get(self._platform).get("symbols")))
        self._channels = config.platforms.get(self._platform).get("channels")
        self._last_update = 0

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
                    if not channel:
                        continue
                    channels.append(channel)
        data = {
            "op": "subscribe",
            "args": channels
        }
        await self.ws.send_json(data)
        logger.info("subscribe orderbook success.", caller=self)

    async def process(self, msg):
        """ 处理websocket上接收到的消息
        """
        logger.debug("msg:", msg, caller=self)
        if not isinstance(msg, dict):
            return

        table = msg.get("table")
        action = msg.get("action")
        if action != "update":
            return

        if table == "orderBook10":  # 订单薄数据
            data = msg.get("data")[0]
            symbol = data.get("symbol")
            orderbook = {
                "platform": self._platform,
                "symbol": symbol,
                "asks": data.get("asks"),
                "bids": data.get("bids"),
                "timestamp": tools.utctime_str_to_mts(data["timestamp"])
            }
            EventOrderbook(**orderbook).publish()
            logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)

    def _symbol_to_channel(self, symbol, channel_type):
        """ symbol转换到channel
        @param symbol symbol名字
        @param channel_type 订阅频道类型
        """
        channel = "{channel_type}:{symbol}".format(channel_type=channel_type, symbol=symbol)
        self._c_to_s[channel] = symbol
        return channel
