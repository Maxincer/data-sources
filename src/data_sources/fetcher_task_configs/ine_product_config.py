#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
INE 合约规格 HTML 下载（非日频，按需运行）
URL 从产品列表页动态发现，非固定模板。
"""

from data_sources.constants import INE_BASE_URL

EXCHANGE = "INE"
SUFFIX = "html"
DESCRIPTION = "ProductConfig"
FETCH_METHOD = "_fetch_ine_product_config"
URL_TEMPLATE = INE_BASE_URL + "/products/"
