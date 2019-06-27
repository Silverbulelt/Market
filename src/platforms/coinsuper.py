# -*— coding:utf-8 -*-

"""
Coinsuper 行情数据

Author: HuangTao
Date:   2018/11/14

NOTE:   1. 使用REST API获取行情；
        2. 配置文件里 symbols 交易对数量不宜太多(最好不要超过10个)，因为使用REST API拉取数据，请求频率有限制；
"""

import asyncio

from quant import const
from quant.utils import tools
from quant.utils import logger
from quant.heartbeat import heartbeat
from quant.event import EventTrade, EventKline, EventOrderbook
from quant.platform.coinsuper import CoinsuperRestAPI


class CoinsuperMarket:
    """ Coinsuper 行情数据
    """

    def __init__(self, **kwargs):
        self._platform = kwargs["platform"]
        self._host = kwargs.get("wss", "https://api.coinsuper.com")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]

        self._orderbook_update_interval = kwargs.get("orderbook_update_interval", 2)  # 订单薄更新时间间隔(秒)，默认2秒
        self._orderbook_fetch_count = kwargs.get("orderbook_fetch_count", 10)  # 订单薄推送条数，默认10条
        self._trade_fetch_interval = kwargs.get("trade_fetch_interval", 5)  # trade数据更新时间间隔(秒)，默认5秒

        # 创建rest api请求对象
        self._rest_api = CoinsuperRestAPI(self._host, self._access_key, self._secret_key)

        self._initialize()

    def _initialize(self):
        """ 初始化
        """
        # 注册定时回调
        for channel in self._channels:
            if channel == "orderbook":
                heartbeat.register(self.do_orderbook_update, self._orderbook_update_interval)
            elif channel == const.MARKET_TYPE_KLINE_5M:
                heartbeat.register(self.create_kline_tasks, 60 * 5, const.MARKET_TYPE_KLINE_5M)
            elif channel == const.MARKET_TYPE_KLINE_15M:
                heartbeat.register(self.create_kline_tasks, 60 * 15, const.MARKET_TYPE_KLINE_15M)
            elif channel == "trade":
                heartbeat.register(self.do_trade_update, self._trade_fetch_interval)
            else:
                logger.error("channel error! channel:", channel, caller=self)

    async def do_orderbook_update(self, *args, **kwargs):
        """ 执行订单薄数据更新
        """
        for symbol in self._symbols:
            result, error = await self._rest_api.get_orderbook(symbol, self._orderbook_fetch_count)
            if error:
                continue
            bids = []
            asks = []
            for item in result["asks"]:
                a = [item["limitPrice"], item["quantity"]]
                asks.append(a)
            for item in result["bids"]:
                b = [item["limitPrice"], item["quantity"]]
                bids.append(b)

            if not bids and not asks:
                logger.warn("no orderbook data", caller=self)
                continue

            # 判断买一是否小于卖一，防止异常数据
            if len(bids) > 0 and len(asks) > 0 and float(bids[0][0]) >= float(asks[0][0]):
                logger.warn("symbol:", symbol, "bids one is grate than asks one! asks:", asks, "bids:",
                            bids, caller=self)
                continue

            orderbook = {
                "platform": self._platform,
                "symbol": symbol,
                "asks": asks,
                "bids": bids,
                "timestamp": tools.get_cur_timestamp_ms()
            }
            EventOrderbook(**orderbook).publish()
            logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)

            # 间隔0.1秒发起下一次请求
            await asyncio.sleep(0.1)

    async def do_trade_update(self, *args, **kwargs):
        """ 执行trade数据更新
        """
        for symbol in self._symbols:
            # 获取ticker数据
            result, error = await self._rest_api.get_ticker(symbol)
            if error:
                continue
            for data in result:
                trade = {
                    "platform": self._platform,
                    "symbol": symbol,
                    "action": data["tradeType"],
                    "price": data["price"],
                    "quantity": data["volume"],
                    "timestamp": data["timestamp"]
                }
                EventTrade(**trade).publish()
                logger.info("symbol:", symbol, "trade:", trade, caller=self)

            # 间隔0.1秒发起下一次请求
            await asyncio.sleep(0.1)

    async def create_kline_tasks(self, kline_type, *args, **kwargs):
        """ 创建kline刷新任务
        @param kline_type K线类型
        * NOTE: 每秒钟只发起一个交易对的HTTP请求，避免多个请求同时发起导致的并发过载
        """
        for index, symbol in enumerate(self._symbols):
            asyncio.get_event_loop().call_later(index, self.delay_kline_update, symbol, kline_type)

    def delay_kline_update(self, symbol, kline_type):
        """ 执行kline数据更新
        """
        asyncio.get_event_loop().create_task(self.do_kline_update(symbol, kline_type))

    async def do_kline_update(self, symbol, kline_type):
        if kline_type == const.MARKET_TYPE_KLINE_5M:
            range_type = "5min"
        elif kline_type == const.MARKET_TYPE_KLINE_15M:
            range_type = "15min"
        else:
            return
        result, error = await self._rest_api.get_kline(symbol, 1, range_type)
        if error:
            return

        kline = {
            "platform": self._platform,
            "symbol": symbol,
            "open": "%.8f" % float(result[0]["open"]),
            "high": "%.8f" % float(result[0]["high"]),
            "low": "%.8f" % float(result[0]["low"]),
            "close": "%.8f" % float(result[0]["close"]),
            "volume": "%.8f" % float(result[0]["volume"]),
            "timestamp": result[0]["timestamp"],
            "kline_type": kline_type
        }
        EventKline(**kline).publish()
        logger.info("symbol:", symbol, "kline:", kline, caller=self)
