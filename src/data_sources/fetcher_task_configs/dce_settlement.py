"""DCE exchange configuration."""

from data_sources.constants import DCE_BASE_URL

EXCHANGE = "DCE"
SUFFIX = "json"
DESCRIPTION = "SettlementParameters"
FETCH_METHOD = "_fetch_dce_settlement"
URL_TEMPLATE = DCE_BASE_URL + "/dceapi/forward/publicweb/tradepara/futAndOptSettle"
