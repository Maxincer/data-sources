#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GFEX 交易参数表 (Trading Parameters)
=====================================

POST: /u/interfacesWebTtQueryTradPara/loadDayList
Payload: trade_type=0

字段包含: riseLimitRate, riseLimit, fallLimit, specBuyRate, position limits

样例运行: python3 -m data_sources.discover.gfex_tradepara
"""

import json
import urllib.request

GFEX_BASE_URL = "http://www.gfex.com.cn"


def fetch_gfex_tradepara(trade_date: str) -> list[dict]:
    """Fetch GFEX day trading parameters."""
    url = f"{GFEX_BASE_URL}/u/interfacesWebTtQueryTradPara/loadDayList"
    payload = json.dumps({
        "trade_type": "0",
        "trade_date": trade_date,
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Content-Type": "application/json",
        },
    )
    resp = urllib.request.urlopen(req, timeout=15)
    result = json.loads(resp.read().decode("utf-8"))

    if result.get("code") == "0" and result.get("data"):
        return result["data"]
    print(f"GFEX error: {result.get('msg')}")
    return []


def main():
    records = fetch_gfex_tradepara("20260428")
    print(f"GFEX 交易参数表: {len(records)} 条记录")
    print()

    print(f"{'合约':>12} {'涨跌幅%':>8} {'涨停价':>12} {'跌停价':>12} {'保证金%':>8} {'持仓限额':>10}")
    print("-" * 70)
    for rec in sorted(records, key=lambda r: r["contractId"])[:15]:
        print(
            f"{rec['contractId'][:12]:>12} "
            f"{rec['riseLimitRate'] or 0:>8.0%} "
            f"{rec['riseLimit'] or 0:>12.2f} "
            f"{rec['fallLimit'] or 0:>12.2f} "
            f"{rec['specBuyRate'] or 0:>7.0%} "
            f"{rec.get('selfTotBuyPosiQuota',''):>10}"
        )

    if len(records) > 15:
        print(f"  ... ({len(records) - 15} more)")


if __name__ == "__main__":
    main()
