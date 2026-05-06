#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CFFEX 交易参数表 (Contract Trading Parameters)
===============================================

URL: http://www.cffex.com.cn/sj/jycs/{YYYYMM}/{DD}/{YYYYMMDD}_1.csv
日期格式: /sj/jycs/202604/29/20260429_1.csv

样例运行: python3 -m data_sources.discover.cffex_tradepara
"""

import csv
from datetime import datetime
from pathlib import Path
import urllib.request
import urllib.error

BASE_URL = "http://www.cffex.com.cn"
TEST_DATE = "20260428"  # YYYYMMDD


def fetch_cffex_tradepara(trade_date: str) -> list[dict]:
    """Fetch CFFEX trading parameter CSV."""
    ym = trade_date[:6]
    dd = trade_date[6:8]
    url = f"{BASE_URL}/sj/jycs/{ym}/{dd}/{trade_date}_1.csv"

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    )
    resp = urllib.request.urlopen(req, timeout=15)
    content = resp.read()
    text = content.decode("gbk", errors="replace")

    reader = csv.DictReader(text.strip().split("\n")[1:])  # skip title
    records = []
    for row in reader:
        rec = {
            "instrument_id": row.get("合约代码", "").strip(),
            "contract_month": row.get("合约月份", "").strip(),
            "listing_base_price": _float(row.get("挂盘基准价")),
            "listing_date": row.get("上市日", "").strip(),
            "last_trade_date": row.get("最后交易日", "").strip(),
            "rise_limit_pct": _strip_pct(row.get("涨停板幅度（%）")),
            "fall_limit_pct": _strip_pct(row.get("跌停板幅度（%）")),
            "rise_limit_price": _float(row.get("涨停板价位")),
            "fall_limit_price": _float(row.get("跌停板价位")),
            "position_limit": row.get("持仓限额", "").strip(),
        }
        rec["code"] = rec["instrument_id"]
        records.append(rec)
    return records


def _float(val):
    if val is None:
        return None
    val = str(val).replace(",", "").replace(" ", "").strip()
    if not val or val in ("null", "None", "--"):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _strip_pct(val):
    """Parse percentage like '10%' → 0.10, keep '--' as None."""
    v = _float(val)
    return v / 100.0 if v is not None else None


def main():
    records = fetch_cffex_tradepara(TEST_DATE)
    print(f"CFFEX 交易参数表 ({TEST_DATE}): {len(records)} 条记录")
    print()

    # Column headers
    cols = ["code", "rise_limit_pct", "fall_limit_pct",
            "rise_limit_price", "fall_limit_price", "position_limit"]
    print(f"{'合约':>12} {'涨跌幅%':>8} {'跌跌幅%':>8} {'涨停价':>10} {'跌停价':>10} {'持仓限额':>12}")
    print("-" * 70)

    for rec in records[:15]:
        print(
            f"{rec['code'][:12]:>12} "
            f"{rec['rise_limit_pct'] or 0:>8.2%} "
            f"{rec['fall_limit_pct'] or 0:>8.2%} "
            f"{rec['rise_limit_price'] or '':>10} "
            f"{rec['fall_limit_price'] or '':>10} "
            f"{str(rec['position_limit'])[:12]:>12}"
        )

    if len(records) > 15:
        print(f"  ... ({len(records) - 15} more)")


if __name__ == "__main__":
    main()
