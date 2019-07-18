# -*- coding:utf-8 -*-

"""
Market Server.

Market Server will get market data from Exchange via Websocket or REST as soon as possible, then packet market data into
MarketEvent and publish into EventCenter.

Author: HuangTao
Date:   2018/05/04
Email:  huangtao@ifclover.com
"""

import sys

from quant import const
from quant.quant import quant
from quant.config import config


def initialize():
    """Initialize Server."""

    for platform in config.platforms:
        if platform == const.OKEX or platform == const.OKEX_MARGIN:
            from platforms.okex import OKEx as Market
        elif platform == const.OKEX_FUTURE:
            from platforms.okex_ftu import OKExFuture as Market
        elif platform == const.BINANCE:
            from platforms.binance import Binance as Market
        elif platform == const.DERIBIT:
            from platforms.deribit import Deribit as Market
        elif platform == const.BITMEX:
            from platforms.bitmex import Bitmex as Market
        elif platform == const.HUOBI:
            from platforms.huobi import Huobi as Market
        elif platform == const.COINSUPER:
            from platforms.coinsuper import CoinsuperMarket as Market
        elif platform == const.COINSUPER_PRE:
            from platforms.coinsuper_pre import CoinsuperPreMarket as Market
        elif platform == const.KRAKEN:
            from platforms.kraken import KrakenMarket as Market
        elif platform == const.GATE:
            from platforms.gate import GateMarket as Market
        else:
            from quant.utils import logger
            logger.error("platform error! platform:", platform)
            continue
        cc = config.platforms[platform]
        cc["platform"] = platform
        Market(**cc)


def main():
    config_file = sys.argv[1]  # config file, e.g. config.json.
    quant.initialize(config_file)
    initialize()
    quant.start()


if __name__ == "__main__":
    main()
