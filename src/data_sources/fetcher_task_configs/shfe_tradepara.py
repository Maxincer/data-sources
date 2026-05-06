#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SHFE 交易参数表
URL: https://www.shfe.com.cn/data/busiparamdata/future/ContractDailyTradeArgument{YYYYMMDD}.dat
"""

from data_sources.constants import SHFE_BASE_URL

EXCHANGE = "SHFE"
SUFFIX = "dat"
DESCRIPTION = "TradingParameters"
FETCH_METHOD = "_fetch_shfe_tradepara"
URL_TEMPLATE = SHFE_BASE_URL + "/data/busiparamdata/future/ContractDailyTradeArgument{YYYYMMDD}.dat"
