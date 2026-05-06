#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CFFEX 交易参数表 (Contract Trading Parameters)
URL: http://www.cffex.com.cn/sj/jycs/{YYYYMM}/{DD}/{YYYYMMDD}_1.csv
"""

from data_sources.constants import CFFEX_BASE_URL

EXCHANGE = "CFFEX"
SUFFIX = "csv"
DESCRIPTION = "TradingParameters"
FETCH_METHOD = "_fetch_cffex_tradepara"
URL_TEMPLATE = CFFEX_BASE_URL + "/sj/jycs/{YYYYMM}/{DD}/{YYYYMMDD}_1.csv"
