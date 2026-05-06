"""DCE daily market data configuration."""

from data_sources.constants import DCE_BASE_URL

EXCHANGE = "DCE"
SUFFIX = "json"
DESCRIPTION = "DailyMarketData"
FETCH_METHOD = "_fetch_dce_market"
URL_TEMPLATE = DCE_BASE_URL + "/dceapi/forward/publicweb/dailystat/dayQuotes"
