#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Discover CZCE additional data fields.

CZCE 的结算参数文件 FutureDataClearParams.txt 已经包含 涨跌停板(%) 字段,
只需从现有数据源中提取即可, 无需额外链接。

列定义(GBK编码):
  合约代码|当日结算价|是否单边市|连续单边市天数|交易保证金率(%)|涨跌停板(%)|交易手续费|...

运行: python3 -m data_sources.discover.czce
"""

import csv
from pathlib import Path

RAW_DIR = Path("./data/raw")


def discover_maxup_maxdown():
    """Show that maxup/maxdown data is already in our settlement file."""
    print("=" * 60)
    print("CZCE: 涨跌停板(%) 已在结算参数文件中")
    print("=" * 60)

    for fpath in sorted(RAW_DIR.iterdir()):
        if "CZCE" not in fpath.name or "Settlement" not in fpath.name:
            continue
        if fpath.suffix != ".txt":
            continue

        print(f"\nFile: {fpath.name}")
        with open(fpath, encoding="gbk", errors="replace") as f:
            lines = f.read().strip().split("\n")

        # Line 0: title
        # Line 1: headers
        # Line 2+: data
        headers_raw = lines[1]
        headers = [h.strip() for h in headers_raw.split("|")]
        print(f"Total columns: {len(headers)}")

        # Find limit column index
        limit_idx = None
        for i, h in enumerate(headers):
            if "涨跌停" in h:
                limit_idx = i
                print(f"  Column {i}: '{h}' ← 涨跌停板数据")

        print(f"\nSample rows (code, settle, limit_pct):")
        for line in lines[2:12]:
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 6:
                code = parts[0]
                settle = parts[1]
                limit_pct = parts[limit_idx] if limit_idx and limit_idx < len(parts) else "N/A"
                print(f"  {code:10s} settle={settle:>10s}  limit%={limit_pct}")

    print("\n✅ CZCE: 涨跌停板(%) 字段已存在当前数据源")
    print("   下一本: 扩展到 parser.py 中解析")


def discover_minoq_maxoq():
    """CZCE min/max order qty."""
    print("\n" + "=" * 60)
    print("CZCE: minoq/maxoq 来自合约规则(静态)")
    print("=" * 60)

    for fpath in sorted(RAW_DIR.iterdir()):
        if "CZCE" not in fpath.name or "Settlement" not in fpath.name:
            continue
        if fpath.suffix != ".txt":
            continue

        with open(fpath, encoding="gbk", errors="replace") as f:
            lines = f.read().strip().split("\n")

        last_cols = set()
        for line in lines[2:]:
            parts = line.split("|")
            if len(parts) >= 12:
                # Last few columns: 日持仓限额|交易限价(trade_limit)
                day_limit = parts[-2].strip()
                trade_limit = parts[-1].strip()
                last_cols.add((parts[0].strip(), day_limit, trade_limit))

        print(f"Found {len(last_cols)} unique rows with limit data")
        for code, dl, tl in sorted(list(last_cols)[:5]):
            print(f"  {code:10s} 日持仓限额={dl:>6s}  交易限价={tl}")

    print("\n  minoq/maxoq 不在每日文件中，需从合约规则文档中提取")


if __name__ == "__main__":
    discover_maxup_maxdown()
    discover_minoq_maxoq()
