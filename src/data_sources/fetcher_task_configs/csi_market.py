#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CSI (中证指数) 市场行情
POST: https://www.csindex.com.cn/csindex-home/index-list/query-index-item
"""

CSI_BASE_URL = "https://www.csindex.com.cn"

EXCHANGE = "CSI"
SUFFIX = "json"
DESCRIPTION = "MarketData"
FETCH_METHOD = "_fetch_csi_market"
URL_TEMPLATE = "https://www.csindex.com.cn/csindex-home/index-list/query-index-item"
