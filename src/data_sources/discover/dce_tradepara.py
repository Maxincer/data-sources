#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DCE 交易参数表 (DayTradPara)
=============================

API: /dceapi/forward/publicweb/tradepara/dayTradPara
认证: Bearer Token (通过 accessToken 接口获取)
Header: apikey

字段包含: riseLimitRate, riseLimit, fallLimit, specBuyRate, position limits

样例运行: DCE_API_KEY="ofxc69rpmd59" DCE_API_SECRET="2UdFW^2G4!4^7#@URqWx" \\
           python3 -m data_sources.discover.dce_tradepara
"""

import json
import os
import urllib.request

DCE_BASE_URL = "http://www.dce.com.cn"
DCE_API_KEY = os.environ.get("DCE_API_KEY", "ofxc69rpmd59")
DCE_API_SECRET = os.environ.get("DCE_API_SECRET", "2UdFW^2G4!4^7#@URqWx")


def _req(method: str, url: str, headers: dict = None, body: dict = None):
    """Simple HTTP request helper."""
    if headers is None:
        headers = {}
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(
        url, data=data, headers=headers, method=method
    )
    resp = urllib.request.urlopen(req, timeout=15)
    return json.loads(resp.read().decode("utf-8"))


def get_dce_token() -> str | None:
    """Get DCE bearer token."""
    url = f"{DCE_BASE_URL}/dceapi/cms/auth/accessToken"
    headers = {"apikey": DCE_API_KEY, "Content-Type": "application/json"}
    result = _req("POST", url, headers, {"secret": DCE_API_SECRET})
    if result.get("success"):
        return result["data"]["token"]
    print(f"Token error: {result.get('msg')}")
    return None


def fetch_dce_tradepara(trade_date: str, token: str) -> list[dict]:
    """Fetch DCE day trading parameters."""
    url = f"{DCE_BASE_URL}/dceapi/forward/publicweb/tradepara/dayTradPara"
    headers = {
        "apikey": DCE_API_KEY,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "varietyId": "all",
        "tradeDate": trade_date,
        "tradeType": "1",
        "lang": "zh",
    }
    result = _req("POST", url, headers, payload)
    if result.get("success") and result.get("data"):
        return result["data"]
    print(f"DCE dayTradPara error: {result.get('msg')}")
    return []


def main():
    trade_date = "20260428"
    token = get_dce_token()
    if not token:
        return

    records = fetch_dce_tradepara(trade_date, token)
    print(f"DCE 交易参数表 ({trade_date}): {len(records)} 条记录")
    print()

    cols = ["contractId", "riseLimitRate", "riseLimit", "fallLimit",
            "specBuyRate", "selfTotBuyPosiQuota"]
    print(f"{'合约':>12} {'涨跌幅%':>8} {'涨停价':>10} {'跌停价':>10} {'保证金%':>8} {'持仓限额':>10}")
    print("-" * 65)

    for rec in sorted(records, key=lambda r: r["contractId"])[:15]:
        print(
            f"{rec['contractId'][:12]:>12} "
            f"{rec['riseLimitRate'] or 0:>8.0%} "
            f"{rec['riseLimit'] or 0:>10.2f} "
            f"{rec['fallLimit'] or 0:>10.2f} "
            f"{rec['specBuyRate'] or 0:>7.0%} "
            f"{rec['selfTotBuyPosiQuota'] or '':>10}"
        )

    if len(records) > 15:
        print(f"  ... ({len(records) - 15} more)")


if __name__ == "__main__":
    main()
