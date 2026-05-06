"""INE daily market data configuration."""

from data_sources.constants import INE_BASE_URL

EXCHANGE = "INE"
SUFFIX = "dat"
DESCRIPTION = "DailyMarketData"
FETCH_METHOD = "_fetch_ine_market"
# params=$ts is a cache-busting timestamp, appended dynamically in fetch
URL_TEMPLATE = INE_BASE_URL + "/data/tradedata/future/dailydata/kx{YYYYMMDD}.dat?params="
