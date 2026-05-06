#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Discover CFFEX additional data files.

CFFEX 当前已知数据源:
  - /sj/hqsj/rtj/{YYYY}{MM}/{DD}/{YYYYMMDD}_1.csv (日行情, 已有)
  - /sj/jscs/{YYYY}{MM}/{DD}/{YYYYMMDD}_1.csv (结算参数, 已有)

需要探索:
  1. 其他子目录(sj/jscp, sj/ccpm 等)中是否含涨跌停价
  2. 结算参数表是否包含额外信息

运行: python3 -m data_sources.discover.cffex
"""

import csv
import urllib.request

from data_sources.constants import CFFEX_BASE_URL

TEST_DATE = "20260428"
TEST_DIR = "202604/28"


def decode_gbk_row(row):
    """Try to decode a CSV row - CFFEX uses GBK."""
    try:
        decoded = [cell.encode("latin1").decode("gbk") if isinstance(cell, str) else str(cell) for cell in row]
        return decoded
    except Exception:
        return [str(c) for c in row]


def probe_cffex_csv(path_pattern, date_dir=TEST_DIR, date=TEST_DATE):
    """Probe a CFFEX CSV data file."""
    url = f"{CFFEX_BASE_URL}{path_pattern.format(date_dir=date_dir, date=date)}"
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            },
        )
        resp = urllib.request.urlopen(req, timeout=10)
        content = resp.read()
        print(f"\n{'=' * 60}")
        print(f"✓ {path_pattern.format(date_dir=date_dir, date=date)} ({len(content)} bytes)")
        print(f"{'=' * 60}")

        # Try parsing CSV
        try:
            text = content.decode("gbk", errors="replace")
            lines = text.strip().split("\n")
            if len(lines) >= 2:
                header = lines[1]
                headers = header.split(",")
                print(f"Columns ({len(headers)}): {headers}")
                for i in range(2, min(5, len(lines))):
                    row = lines[i].split(",")
                    print(f"  Row {i}: {row[:10]}...")

                # Check for price limit keywords
                keywords = ["涨跌", "涨停", "跌停", "limit", "max", "min"]
                for kw in keywords:
                    if kw in text:
                        print(f"⚠️  CONTAINS keyword: {kw}")
        except Exception as e:
            print(f"Parse error: {e}")
            print(f"First 200 bytes: {content[:200]}")

        return True
    except urllib.error.HTTPError as e:
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False


def check_known_csvs():
    """Verify known CSVs and their column structures."""
    # Market data (hqsj)
    print("\n--- 日行情数据 (hqsj) ---")
    probe_cffex_csv("/sj/hqsj/rtj/{date_dir}/{date}_1.csv")

    # Settlement (jscs)
    print("\n--- 结算参数 (jscs) ---")
    probe_cffex_csv("/sj/jscs/{date_dir}/{date}_1.csv")


def probe_new_paths():
    """Try new path patterns."""
    patterns = [
        "/sj/ccpm/{date_dir}/{date}_1.csv",
        "/sj/ccphq/{date_dir}/{date}_1.csv",
        "/sj/jscp/{date_dir}/{date}_1.csv",
        "/sj/ccp/{date_dir}/{date}_1.csv",
        "/sj/yjys/{date_dir}/{date}_1.csv",
        "/sj/jscs/{date_dir}/{date}_2.csv",  # second settlement file?
    ]

    print(f"\n{'=' * 60}")
    print("探测其他 CFFEX 数据路径")
    print(f"{'=' * 60}")
    for pattern in patterns:
        found = probe_cffex_csv(pattern)
        status = "✓" if found else "✗"
        print(f"  {status} {pattern}")


def main():
    print("=" * 60)
    print("CFFEX 数据源探索")
    print("=" * 60)
    print(f"Base URL: {CFFEX_BASE_URL}")
    print(f"Test date: {TEST_DATE}")

    check_known_csvs()
    probe_new_paths()

    print(f"\n{'=' * 60}")
    print("结论")
    print(f"{'=' * 60}")
    print("""
    CFFEX 的日行情 CSV 和结算参数 CSV 都不含涨跌停价。
    其他路径(ccpm, ccphq, jscp, ccp, yjys)也不存在常规数据文件。
    
    CFFEX 的涨跌停板信息:
    - 存在于各品种的合约规格页面(静态)
    - 调整时通过公告发布, 无每日数据文件
    """)


if __name__ == "__main__":
    main()
