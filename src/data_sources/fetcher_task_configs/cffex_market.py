"""CFFEX exchange configuration."""

from data_sources.constants import CFFEX_BASE_URL

EXCHANGE = "CFFEX"
SUFFIX = "csv"
DESCRIPTION = "DailyMarketData"
FETCH_METHOD = "_fetch_cffex_market"
URL_TEMPLATE = CFFEX_BASE_URL + "/sj/hqsj/rtj/{YYYY}{MM}/{DD}/{YYYYMMDD}_1.csv"
