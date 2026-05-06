#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""SHFE exchange configuration."""

from data_sources.constants import SHFE_BASE_URL

EXCHANGE = "SHFE"
SUFFIX = "dat"
DESCRIPTION = "SettlementParameters"
FETCH_METHOD = "_fetch_shfe_settlement"
URL_TEMPLATE = SHFE_BASE_URL + "/data/tradedata/future/dailydata/js{YYYYMMDD}.dat"
