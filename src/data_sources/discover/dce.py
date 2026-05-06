#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Discover DCE additional data endpoints.

DCE 使用 token 认证的 REST API, 当前已知 endpoints:
  - /dceapi/forward/publicweb/tradepara/futAndOptSettle (结算参数, 已有)
  - /dceapi/forward/publicweb/dailystat/dayQuotes (日行情, 已有)

需要探索:
  1. 日交易参数 endpoint (含涨跌停板)
  2. 合约信息 endpoint (含 minoq/maxoq)

运行: python3 -m data_sources.discover.dce
"""

import os
import json

import requests

from data_sources.constants import DCE_BASE_URL

DCE_API_KEY = os.environ.get("DCE_API_KEY", "")
DCE_API_SECRET = os.environ.get("DCE_API_SECRET", "")


def get_dce_token():
    """Get DCE bearer token."""
    url = f"{DCE_BASE_URL}/dceapi/cms/auth/accessToken"
    headers = {"apikey": DCE_API_KEY}
    payload = {"secret": DCE_API_SECRET}
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if data.get("success"):
        return data["data"]["token"]
    print(f"Token error: {data.get('msg')}")
    return None


def try_dce_endpoint(token, path, payload=None):
    """Try a DCE endpoint and print response structure."""
    url = f"{DCE_BASE_URL}{path}"
    headers = {
        "apikey": DCE_API_KEY,
        "Authorization": f"Bearer {token}",
    }
    if payload is None:
        payload = {
            "varietyId": "all",
            "tradeDate": "20260428",
            "tradeType": "1",
            "lang": "zh",
        }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        print(f"\n{'=' * 60}")
        print(f"Endpoint: {path}")
        print(f"Status: {resp.status_code}")
        data = resp.json()
        print(f"Success: {data.get('success')}")
        if data.get("success"):
            items = data.get("data", [])
            if isinstance(items, list) and len(items) > 0:
                print(f"Records: {len(items)}")
                print(f"Columns: {list(items[0].keys())}")
                print(f"Sample: {json.dumps(items[0], ensure_ascii=False)[:800]}")
            elif isinstance(items, dict):
                print(f"Top keys: {list(items.keys())[:10]}")
                # Find sub-arrays
                for k, v in items.items():
                    if isinstance(v, list) and len(v) > 0:
                        print(f"  {k}: {len(v)} items, keys={list(v[0].keys())}")
                        print(f"    Sample: {json.dumps(v[0], ensure_ascii=False)[:400]}")
                        break
            else:
                print(f"Data: {json.dumps(items, ensure_ascii=False)[:500]}")
        else:
            print(f"Error: {data.get('msg')}")
    except Exception as e:
        print(f"Exception: {e}")


def main():
    """Try all known and discovered DCE endpoints."""
    print("=" * 60)
    print("DCE 数据源探索")
    print("=" * 60)
    print(f"Base URL: {DCE_BASE_URL}")
    print(f"API Key: {'SET' if DCE_API_KEY else 'MISSING'}")
    print()

    if not DCE_API_KEY:
        print("❌ DCE_API_KEY 环境变量未设置")
        print("   请在运行前设置: export DCE_API_KEY=xxx DCE_API_SECRET=xxx")
        return

    token = get_dce_token()
    if not token:
        print("❌ 无法获取 DCE token")
        return
    print(f"✅ Token obtained: {token[:20]}...")

    # Known endpoints to probe
    endpoints = {
        # Already have
        "/dceapi/forward/publicweb/tradepara/futAndOptSettle": "结算参数(已有)",
        "/dceapi/forward/publicweb/dailystat/dayQuotes": "日行情(已有)",
        # New endpoints to discover
        "/dceapi/forward/publicweb/tradepara/dayTradPara": "日交易参数(可能含涨跌停)",
        "/dceapi/forward/publicweb/tradepara/dayTradParaOpt": "日交易参数(含期权)",
        "/dceapi/forward/publicweb/tradepara/futAndOptSettleParams": "结算参数完整版",
        "/dceapi/forward/publicweb/contract/futAndOptContractInfo": "合约信息(可能含min/maxoq)",
        "/dceapi/forward/publicweb/contract/queryContractByVariety": "品种合约信息",
    }

    for path, desc in endpoints.items():
        print(f"\n--- {desc} ---")
        try_dce_endpoint(token, path)

    # Try queryDayTradPara endpoint with different payload
    print(f"\n{'=' * 60}")
    print("尝试 日交易参数 HTML 页面")
    print(f"{'=' * 60}")
    try:
        url = f"{DCE_BASE_URL}/publicweb/notificationtips/queryDayTradParaOptByVariety.html"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": f"{DCE_BASE_URL}/dce/channel/list/159.html",
        }
        # Need to get cookies first
        sess = requests.Session()
        sess.headers.update({"User-Agent": headers["User-Agent"]})
        resp = sess.get(url, params={"variety": "i"}, timeout=10)
        resp.encoding = "utf-8"
        print(f"Status: {resp.status_code}")
        print(f"Content(snip): {resp.text[:1000]}")
    except Exception as e:
        print(f"Exception: {e}")


if __name__ == "__main__":
    main()
