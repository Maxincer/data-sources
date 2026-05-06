#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GFEX 交易参数表
POST: /u/interfacesWebTtQueryTradPara/loadDayList, payload: trade_type=0
"""

EXCHANGE = "GFEX"
SUFFIX = "json"
DESCRIPTION = "TradingParameters"
FETCH_METHOD = "_fetch_gfex_tradepara"
URL_TEMPLATE = "http://www.gfex.com.cn/u/interfacesWebTtQueryTradPara/loadDayList"
