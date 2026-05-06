#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SHFE 合约规格 HTML 下载（非日频，按需运行）
URL 从产品列表页动态发现，非固定模板。
"""

from data_sources.constants import SHFE_BASE_URL

EXCHANGE = "SHFE"
SUFFIX = "html"
DESCRIPTION = "ProductConfig"
FETCH_METHOD = "_fetch_shfe_product_config"
URL_TEMPLATE = SHFE_BASE_URL + "/products/"
