"""GFEX exchange configuration."""

from data_sources.constants import GFEX_BASE_URL

EXCHANGE = "GFEX"
SUFFIX = "json"
DESCRIPTION = "SettlementParameters"
FETCH_METHOD = "_fetch_gfex_settlement"
URL_TEMPLATE = GFEX_BASE_URL + "/u/interfacesWebTiFutAndOptSettle/loadList"
