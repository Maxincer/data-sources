"""CZCE exchange configuration."""

from data_sources.constants import CZCE_BASE_URL

EXCHANGE = "CZCE"
SUFFIX = "txt"
DESCRIPTION = "SettlementParameters"
FETCH_METHOD = "_fetch_czce_settlement"
URL_TEMPLATE = CZCE_BASE_URL + "/cn/DFSStaticFiles/Future/{YYYY}/{YYYYMMDD}/FutureDataClearParams.txt"
