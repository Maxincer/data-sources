#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
INE 交易参数表
URL: https://www.ine.com.cn/data/busiparamdata/future/ContractDailyTradeArgument{YYYYMMDD}.dat
"""

from data_sources.constants import INE_BASE_URL

EXCHANGE = "INE"
SUFFIX = "dat"
DESCRIPTION = "TradingParameters"
FETCH_METHOD = "_fetch_ine_tradepara"
URL_TEMPLATE = INE_BASE_URL + "/data/busiparamdata/future/ContractDailyTradeArgument{YYYYMMDD}.dat"
