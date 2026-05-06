#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Discover GFEX additional data endpoints.

GFEX 当前已知 JSON API:
  - /u/interfacesWebTiDayQuotes/loadList (日行情, 已有)
  - /u/interfacesWebTiFutAndOptSettle/loadList (结算参数, 已有)

需要探索是否还有其他接口包含涨跌停价。

运行: python3 -m data_sources.discover.gfex
"""

import json
import urllib.request

from data_sources.constants import GFEX_BASE_URL


def try_gfex_post(endpoint, payload):
    """Try a GFEX POST endpoint."""
    url = f"{GFEX_BASE_URL}{endpoint}"
    data = json.dumps(payload).encode("utf-8")
    try:
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Origin": GFEX_BASE_URL,
                "Referer": f"{GFEX_BASE_URL}/",
            },
        )
        resp = urllib.request.urlopen(req, timeout=15)
        content = json.loads(resp.read().decode("utf-8"))
        print(f"\n{'=' * 60}")
        print(f"Endpoint: {endpoint}")
        print(f"{'=' * 60}")
        print(f"Success: {content.get('success', content.get('code') == 0)}")

        # Show data structure
        data = content.get("data", content)
        if isinstance(data, list) and len(data) > 0:
            print(f"Records: {len(data)}")
            print(f"Columns: {list(data[0].keys())}")
            print(f"Sample: {json.dumps(data[0], ensure_ascii=False)[:600]}")

            # Check for price limit keywords
            sample = json.dumps(data[0])
            keywords = ["limit", "up", "down", "max", "min", "rate", "ratio"]
            found = [kw for kw in keywords if kw in sample.lower()]
            if found:
                print(f"⚠️  Possible fields: {found}")
        elif isinstance(data, dict):
            print(f"Top keys: {list(data.keys())[:10]}")
            print(f"Sample: {json.dumps(data, ensure_ascii=False)[:500]}")
        else:
            print(f"Data: {json.dumps(content, ensure_ascii=False)[:500]}")
    except Exception as e:
        print(f"\n{endpoint}: {type(e).__name__}: {e}")


def main():
    """Try all known and speculative GFEX endpoints."""
    print("=" * 60)
    print("GFEX 数据源探索")
    print("=" * 60)
    print(f"Base URL: {GFEX_BASE_URL}")
    print()

    endpoints = {
        # Already have
        "/u/interfacesWebTiDayQuotes/loadList": {"trade_date": "20260428", "trade_type": "0"},
        "/u/interfacesWebTiFutAndOptSettle/loadList": {"trade_date": "20260428"},
        # New endpoints to try
        "/u/interfacesWebTiTradingParam/loadList": {"trade_date": "20260428"},
        "/u/interfacesWebTiContractInfo/loadList": {"contract_code": "all"},
        "/u/interfacesWebTiContractInfo/loadList": {},
        "/u/interfacesWebTiProductInfo/loadList": {},
        "/u/interfacesWebTiSettlementParam/loadList": {"trade_date": "20260428"},
    }

    for endpoint, payload in endpoints.items():
        try_gfex_post(endpoint, payload)

    print(f"\n{'=' * 60}")
    print("结论")
    print(f"{'=' * 60}")
    print("""
    GFEX 是所有交易所中最新的(2022年), API 设计较完整。
    可能存在的接口:
    - /u/interfacesWebTiTradingParam/loadList - 交易参数(可能含涨跌停板)
    - /u/interfacesWebTiContractInfo/loadList - 合约信息(可能含 minoq/maxoq)
    
    需从 GFEX 官网开发者工具中进一步分析网络请求。
    """)


if __name__ == "__main__":
    main()
