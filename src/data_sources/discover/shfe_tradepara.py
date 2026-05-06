#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SHFE / INE 交易参数表 (ContractDailyTradeArgument)
===================================================

SHFE: https://www.shfe.com.cn/data/busiparamdata/future/ContractDailyTradeArgument{YYYYMMDD}.dat
INE:  https://www.ine.com.cn/data/busiparamdata/future/ContractDailyTradeArgument{YYYYMMDD}.dat

字段: INSTRUMENTID, UPPER_VALUE(涨停板比例), LOWER_VALUE(跌停板比例),
      PRICE_LIMITS(特殊处理), SPEC_LONGMARGINRATIO, SPEC_SHORTMARGINRATIO,
      HDEGE_LONGMARGINRATIO, HDEGE_SHORTMARGINRATIO, TRADINGDAY

样例运行: python3 -m data_sources.discover.shfe_tradepara
"""

import json
import urllib.request

SHFE_URL = "https://www.shfe.com.cn/data/busiparamdata/future/ContractDailyTradeArgument{}.dat"
INE_URL = "https://www.ine.com.cn/data/busiparamdata/future/ContractDailyTradeArgument{}.dat"


def fetch_tradepara(url_template: str, trade_date: str) -> list[dict]:
    """Fetch SHFE or INE trading parameter JSON."""
    url = url_template.format(trade_date)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    )
    resp = urllib.request.urlopen(req, timeout=15)
    data = json.loads(resp.read().decode("utf-8"))
    return data.get("ContractDailyTradeArgument", [])


def main():
    trade_date = "20260428"

    for name, url_tpl in [("SHFE", SHFE_URL), ("INE", INE_URL)]:
        records = fetch_tradepara(url_tpl, trade_date)
        print(f"{name} 交易参数表 ({trade_date}): {len(records)} 条记录")
        print()

        print(f"{'合约':>12} {'涨停比例':>8} {'跌停比例':>8} {'保证金多%':>8} {'保证金空%':>8}")
        print("-" * 55)
        for rec in sorted(records, key=lambda r: r["INSTRUMENTID"])[:10]:
            upper = _float(rec.get("UPPER_VALUE"))
            lower = _float(rec.get("LOWER_VALUE"))
            spec_lm = _float(rec.get("SPEC_LONGMARGINRATIO"))
            spec_sm = _float(rec.get("SPEC_SHORTMARGINRATIO"))
            print(
                f"{rec['INSTRUMENTID'][:12]:>12} "
                f"{upper or 0:>7.0%} "
                f"{lower or 0:>7.0%} "
                f"{spec_lm or 0:>7.0%} "
                f"{spec_sm or 0:>7.0%}"
            )

        with_limit = [
            r for r in records
            if _float(r.get("UPPER_VALUE")) is not None
        ]
        print(f"  → 其中 {len(with_limit)}/{len(records)} 条有涨跌停比例")
        print()

        # Note: PRICE_LIMITS is empty for normal cases
        empty_pl = sum(1 for r in records if not r.get("PRICE_LIMITS"))
        print(f"  → PRICE_LIMITS 为空: {empty_pl}/{len(records)}")
        print()


def _float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    main()
