#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DCE 交易参数表
API: /dceapi/forward/publicweb/tradepara/dayTradPara (POST with Bearer token)
"""

from data_sources.constants import DCE_BASE_URL

EXCHANGE = "DCE"
SUFFIX = "json"
DESCRIPTION = "TradingParameters"
FETCH_METHOD = "_fetch_dce_tradepara"
URL_TEMPLATE = DCE_BASE_URL + "/dceapi/forward/publicweb/tradepara/dayTradPara"
