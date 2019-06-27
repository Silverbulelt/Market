# -*— coding:utf-8 -*-

"""
Huobi 行情数据
https://github.com/huobiapi/API_Docs

Author: HuangTao
Date:   2018/08/28
"""

import gzip
import json

from quant.utils import logger
from quant.utils.websocket import Websocket
from quant.const import MARKET_TYPE_KLINE
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.event import EventTrade, EventKline, EventOrderbook


class Huobi(Websocket):
    """ Huobi 行情数据
    """

    def __init__(self, **kwargs):
        self._platform = kwargs["platform"]
        self._wss = kwargs.get("wss", "wss://api.huobi.pro")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")

        self._c_to_s = {}  # {"channel": "symbol"}

        url = self._wss + "/ws"
        super(Huobi, self).__init__(url, send_hb_interval=5)
        self.initialize()

    async def connected_callback(self):
        """ 订阅消息
        """
        for ch in self._channels:
            if ch == "kline":  # 订阅K线数据
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "kline")
                    if not channel:
                        continue
                    kline = {
                        "sub": channel
                    }
                    await self.ws.send_json(kline)
            elif ch == "orderbook":  # 订阅订单薄数据
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "depth")
                    if not channel:
                        continue
                    data = {
                        "sub": channel
                    }
                    await self.ws.send_json(data)
            elif ch == "trade":  # 实时交易数据
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "trade")
                    if not channel:
                        continue
                    data = {
                        "sub": channel
                    }
                    await self.ws.send_json(data)
            else:
                logger.error("channel error! channel:", ch, caller=self)

    async def process_binary(self, msg):
        """ 处理websocket上接收到的消息
        """
        data = json.loads(gzip.decompress(msg).decode())
        logger.debug("data:", json.dumps(data), caller=self)
        channel = data.get("ch")
        if not channel:
            if data.get("ping"):
                self.heartbeat_msg = data
            return

        symbol = self._c_to_s[channel]

        if channel.find("kline") != -1:  # K线
            d = data.get("tick")
            kline = {
                "platform": self._platform,
                "symbol": symbol,
                "open": "%.8f" % d["open"],  # 开盘价
                "high": "%.8f" % d["high"],  # 最高价
                "low": "%.8f" % d["low"],  # 最低价
                "close": "%.8f" % d["close"],  # 收盘价
                "volume": "%.8f" % d["amount"],  # 成交量
                "timestamp": int(data.get("ts")),  # 时间戳
                "kline_type": MARKET_TYPE_KLINE
            }
            EventKline(**kline).publish()
            logger.info("symbol:", symbol, "kline:", kline, caller=self)
        elif channel.find("depth") != -1:  # 订单薄
            d = data.get("tick")
            asks, bids = [], []
            for item in d.get("asks")[:10]:
                price = "%.8f" % item[0]
                quantity = "%.8f" % item[1]
                asks.append([price, quantity])
            for item in d.get("bids")[:10]:
                price = "%.8f" % item[0]
                quantity = "%.8f" % item[1]
                bids.append([price, quantity])
            orderbook = {
                "platform": self._platform,
                "symbol": symbol,
                "asks": asks,
                "bids": bids,
                "timestamp": d.get("ts")
            }
            EventOrderbook(**orderbook).publish()
            logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)
        elif channel.find("trade") != -1:  # 实时交易数据
            tick = data.get("tick")
            direction = tick["data"][0].get("direction")
            price = tick["data"][0].get("price")
            quantity = tick["data"][0].get("amount")
            trade = {
                "platform": self._platform,
                "symbol": symbol,
                "action": ORDER_ACTION_BUY if direction == "buy" else ORDER_ACTION_SELL,
                "price": "%.8f" % price,
                "quantity": "%.8f" % quantity,
                "timestamp": tick.get("ts")
            }
            EventTrade(**trade).publish()
            logger.info("symbol:", symbol, "trade:", trade, caller=self)
        else:
            logger.error("event error! msg:", msg, caller=self)

    def _symbol_to_channel(self, symbol, channel_type):
        """ symbol转换到channel
        @param symbol 交易对名字
        @param channel_type 频道类型 kline K线 / ticker 行情 / depth 订单薄
        """
        if channel_type == "kline":
            channel = "market.{s}.kline.1min".format(s=symbol.replace("/", '').lower())
        elif channel_type == "depth":
            channel = "market.{s}.depth.step0".format(s=symbol.replace("/", '').lower())
        elif channel_type == "trade":
            channel = "market.{s}.trade.detail".format(s=symbol.replace("/", '').lower())
        else:
            logger.error("channel type error! channel type:", channel_type, calle=self)
            return None
        self._c_to_s[channel] = symbol
        return channel
