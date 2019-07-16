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

from quant.quant import quant
from quant.config import config
from quant.const import OKEX, OKEX_MARGIN, OKEX_FUTURE, BINANCE, DERIBIT, BITMEX, HUOBI, COINSUPER, KRAKEN, GATE


def initialize():
    """Initialize Server."""

    for platform in config.platforms:
        if platform == OKEX or platform == OKEX_MARGIN:
            from platforms.okex import OKEx as Market
        elif platform == OKEX_FUTURE:
            from platforms.okex_ftu import OKExFuture as Market
        elif platform == BINANCE:
            from platforms.binance import Binance as Market
        elif platform == DERIBIT:
            from platforms.deribit import Deribit as Market
        elif platform == BITMEX:
            from platforms.bitmex import Bitmex as Market
        elif platform == HUOBI:
            from platforms.huobi import Huobi as Market
        elif platform == COINSUPER:
            from platforms.coinsuper import CoinsuperMarket as Market
        elif platform == KRAKEN:
            from platforms.kraken import KrakenMarket as Market
        elif platform == GATE:
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
