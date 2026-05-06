"""SHFE daily market data configuration."""

from data_sources.constants import SHFE_BASE_URL

EXCHANGE = "SHFE"
SUFFIX = "dat"
DESCRIPTION = "DailyMarketData"
FETCH_METHOD = "_fetch_shfe_market"
# params=$ts is a cache-busting timestamp, appended dynamically in fetch
URL_TEMPLATE = SHFE_BASE_URL + "/data/tradedata/future/dailydata/kx{YYYYMMDD}.dat?params="
