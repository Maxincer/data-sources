"""GFEX daily market data configuration."""

from data_sources.constants import GFEX_BASE_URL

EXCHANGE = "GFEX"
SUFFIX = "json"
DESCRIPTION = "DailyMarketData"
FETCH_METHOD = "_fetch_gfex_market"
URL_TEMPLATE = GFEX_BASE_URL + "/u/interfacesWebTiDayQuotes/loadList"
