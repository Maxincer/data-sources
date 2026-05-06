"""Registry of all fetcher task configurations.

Each module exports: EXCHANGE, SUFFIX, DESCRIPTION, FETCH_METHOD, URL_TEMPLATE
"""

from data_sources.models import TaskConfig

from data_sources.fetcher_task_configs import (
    shfe_settlement, ine_settlement, gfex_settlement,
    dce_settlement, czce_settlement, cffex_settlement,
    shfe_market, ine_market, czce_market,
    gfex_market, dce_market, cffex_market,
    cffex_tradepara, dce_tradepara, gfex_tradepara,
    shfe_tradepara, ine_tradepara,
    czce_tradepara,
    csi_market,
    shfe_product_config, ine_product_config,
)

ALL_CONFIGS = [
    shfe_settlement, ine_settlement, gfex_settlement,
    dce_settlement, czce_settlement, cffex_settlement,
    shfe_market, ine_market, czce_market,
    gfex_market, dce_market, cffex_market,
    cffex_tradepara, dce_tradepara, gfex_tradepara,
    shfe_tradepara, ine_tradepara,
    czce_tradepara,
    csi_market,
    shfe_product_config, ine_product_config,
]


def build_task_configs(fetcher_instance) -> list[TaskConfig]:
    """Build TaskConfig list, binding fetch methods to Fetcher instance."""
    configs = []
    for mod in ALL_CONFIGS:
        fetch_func = getattr(fetcher_instance, mod.FETCH_METHOD, None)
        if fetch_func is None:
            raise AttributeError(
                f"Fetcher has no method {mod.FETCH_METHOD} "
                f"for exchange {mod.EXCHANGE}"
            )
        configs.append(TaskConfig(
            exchange=mod.EXCHANGE,
            fetch_func=fetch_func,
            suffix=mod.SUFFIX,
            description=mod.DESCRIPTION,
            url_template=mod.URL_TEMPLATE,
        ))
    return configs
