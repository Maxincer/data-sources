"""INE exchange configuration."""

from data_sources.constants import INE_BASE_URL

EXCHANGE = "INE"
SUFFIX = "dat"
DESCRIPTION = "SettlementParameters"
FETCH_METHOD = "_fetch_ine_settlement"
URL_TEMPLATE = INE_BASE_URL + "/data/tradedata/future/dailydata/js{YYYYMMDD}.dat"
