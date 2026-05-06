"""CFFEX settlement parameters configuration."""

from data_sources.constants import CFFEX_BASE_URL

EXCHANGE = "CFFEX"
SUFFIX = "csv"
DESCRIPTION = "SettlementParameters"
FETCH_METHOD = "_fetch_cffex_settlement"
URL_TEMPLATE = CFFEX_BASE_URL + "/sj/jscs/{YYYY}{MM}/{DD}/{YYYYMMDD}_1.csv"
