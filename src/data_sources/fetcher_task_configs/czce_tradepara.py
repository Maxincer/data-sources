#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CZCE 交易参数表 (FutureTradeParam.txt)
URL: https://www.czce.com.cn/cn/DFSStaticFiles/Future/{YYYY}/{YYYYMMDD}/FutureTradeParam.txt

包含: 最小变动价位, 涨跌停板幅度(%), 上日结算价,
      最小开仓量下单量, 限价单最大下单量, 市价单最大下单量
"""

from data_sources.constants import CZCE_BASE_URL

EXCHANGE = "CZCE"
SUFFIX = "txt"
DESCRIPTION = "TradingParameters"
FETCH_METHOD = "_fetch_czce_tradepara"
URL_TEMPLATE = CZCE_BASE_URL + "/cn/DFSStaticFiles/Future/{YYYY}/{YYYYMMDD}/FutureTradeParam.txt"
