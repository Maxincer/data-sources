#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""All fetcher task configurations + URL constants."""

from data_sources.task import TaskConfig

SHFE_BASE_URL = "http://www.shfe.com.cn"
INE_BASE_URL = "http://www.ine.com.cn"
GFEX_BASE_URL = "http://www.gfex.com.cn"
DCE_BASE_URL = "http://www.dce.com.cn"
CZCE_BASE_URL = "https://www.czce.com.cn"
CFFEX_BASE_URL = "http://www.cffex.com.cn"
CSI_BASE_URL = "https://www.csindex.com.cn"

_CONFIGS = [
    # SHFE
    ("SHFE", "dat", "SettlementParameters", "_fetch_shfe_settlement",
     SHFE_BASE_URL + "/data/tradedata/future/dailydata/js{YYYYMMDD}.dat"),
    ("SHFE", "dat", "DailyMarketData", "_fetch_shfe_market",
     SHFE_BASE_URL + "/data/tradedata/future/dailydata/kx{YYYYMMDD}.dat?params="),
    ("SHFE", "dat", "TradingParameters", "_fetch_shfe_tradepara",
     SHFE_BASE_URL + "/data/busiparamdata/future/ContractDailyTradeArgument{YYYYMMDD}.dat"),
    ("SHFE", "html", "ProductConfig", "_fetch_shfe_product_config",
     SHFE_BASE_URL + "/products/"),
    # INE
    ("INE", "dat", "SettlementParameters", "_fetch_ine_settlement",
     INE_BASE_URL + "/data/tradedata/future/dailydata/js{YYYYMMDD}.dat"),
    ("INE", "dat", "DailyMarketData", "_fetch_ine_market",
     INE_BASE_URL + "/data/tradedata/future/dailydata/kx{YYYYMMDD}.dat?params="),
    ("INE", "dat", "TradingParameters", "_fetch_ine_tradepara",
     INE_BASE_URL + "/data/busiparamdata/future/ContractDailyTradeArgument{YYYYMMDD}.dat"),
    ("INE", "html", "ProductConfig", "_fetch_ine_product_config",
     INE_BASE_URL + "/products/"),
    # DCE
    ("DCE", "json", "SettlementParameters", "_fetch_dce_settlement",
     DCE_BASE_URL + "/dceapi/forward/publicweb/tradepara/futAndOptSettle"),
    ("DCE", "json", "DailyMarketData", "_fetch_dce_market",
     DCE_BASE_URL + "/dceapi/forward/publicweb/dailystat/dayQuotes"),
    ("DCE", "json", "TradingParameters", "_fetch_dce_tradepara",
     DCE_BASE_URL + "/dceapi/forward/publicweb/tradepara/dayTradPara"),
    # CZCE
    ("CZCE", "txt", "SettlementParameters", "_fetch_czce_settlement",
     CZCE_BASE_URL + "/cn/DFSStaticFiles/Future/{YYYY}/{YYYYMMDD}/FutureDataClearParams.txt"),
    ("CZCE", "txt", "DailyMarketData", "_fetch_czce_market",
     CZCE_BASE_URL + "/cn/DFSStaticFiles/Future/{YYYY}/{YYYYMMDD}/FutureDataDaily.txt"),
    ("CZCE", "txt", "TradingParameters", "_fetch_czce_tradepara",
     CZCE_BASE_URL + "/cn/DFSStaticFiles/Future/{YYYY}/{YYYYMMDD}/FutureTradeParam.txt"),
    # CFFEX
    ("CFFEX", "csv", "SettlementParameters", "_fetch_cffex_settlement",
     CFFEX_BASE_URL + "/sj/jscs/{YYYY}{MM}/{DD}/{YYYYMMDD}_1.csv"),
    ("CFFEX", "csv", "DailyMarketData", "_fetch_cffex_market",
     CFFEX_BASE_URL + "/sj/hqsj/rtj/{YYYY}{MM}/{DD}/{YYYYMMDD}_1.csv"),
    ("CFFEX", "csv", "TradingParameters", "_fetch_cffex_tradepara",
     CFFEX_BASE_URL + "/sj/jycs/{YYYYMM}/{DD}/{YYYYMMDD}_1.csv"),
    # GFEX
    ("GFEX", "json", "SettlementParameters", "_fetch_gfex_settlement",
     GFEX_BASE_URL + "/u/interfacesWebTiFutAndOptSettle/loadList"),
    ("GFEX", "json", "DailyMarketData", "_fetch_gfex_market",
     GFEX_BASE_URL + "/u/interfacesWebTiDayQuotes/loadList"),
    ("GFEX", "json", "TradingParameters", "_fetch_gfex_tradepara",
     "http://www.gfex.com.cn/u/interfacesWebTtQueryTradPara/loadDayList"),
    # CSI
    ("CSI", "json", "MarketData", "_fetch_csi_market",
     CSI_BASE_URL + "/csindex-home/index-list/query-index-item"),
]


def build_task_configs(fetcher_instance) -> list[TaskConfig]:
    """Build TaskConfig list, binding fetch methods to Fetcher instance."""
    configs = []
    for exchange, suffix, description, fetch_method, url_template in _CONFIGS:
        fetch_func = getattr(fetcher_instance, fetch_method, None)
        if fetch_func is None:
            raise AttributeError(
                f"Fetcher has no method {fetch_method} "
                f"for exchange {exchange}"
            )
        configs.append(TaskConfig(
            exchange=exchange,
            fetch_func=fetch_func,
            suffix=suffix,
            description=description,
            url_template=url_template,
        ))
    return configs
