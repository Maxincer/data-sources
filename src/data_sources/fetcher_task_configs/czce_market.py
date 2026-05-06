"""CZCE daily market data configuration."""

from data_sources.constants import CZCE_BASE_URL

EXCHANGE = "CZCE"
SUFFIX = "txt"
DESCRIPTION = "DailyMarketData"
FETCH_METHOD = "_fetch_czce_market"
URL_TEMPLATE = CZCE_BASE_URL + "/cn/DFSStaticFiles/Future/{YYYY}/{YYYYMMDD}/FutureDataDaily.txt"
