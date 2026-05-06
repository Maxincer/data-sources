#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Discover SHFE/INE additional data files.

SHFE/INE 当前已知文件类型:
  - kx{YYYYMMDD}.dat (日行情数据, 已有)
  - js{YYYYMMDD}.dat (结算参数, 已有)

需要探索其他文件类型:
  - cs*.dat (结算?)
  - jy*.dat (交易?)
  - zb*.dat (报表?)
  - 其他可能的数据文件

运行: python3 -m data_sources.discover.shfe
"""

import json
import urllib.request

from data_sources.constants import SHFE_BASE_URL, INE_BASE_URL


def probe_shfe_prefix(prefix, date="20260428"):
    """Try to fetch a SHFE data file by prefix and analyze content."""
    for base_url, name in [(SHFE_BASE_URL, "SHFE"), (INE_BASE_URL, "INE")]:
        # Try the /data/dailydata/ path
        url = f"{base_url}/data/dailydata/{prefix}/{prefix}{date}.dat"
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
            print(f"{name}: /data/dailydata/{prefix}/{prefix}{date}.dat")
            print(f"{'=' * 60}")
            print(f"Size: {len(content)} bytes")

            # Try to parse as JSON
            try:
                data = json.loads(content)
                if isinstance(data, dict):
                    print("Type: JSON dict")
                    for k, v in data.items():
                        if isinstance(v, list) and len(v) > 0:
                            print(f"  Key '{k}': {len(v)} items")
                            item = v[0]
                            if isinstance(item, dict):
                                print(f"  Columns: {list(item.keys())}")
                                print(
                                    f"  Sample: {json.dumps(item, ensure_ascii=False)[:600]}"
                                )
                            break
                        elif isinstance(v, list):
                            print(f"  Key '{k}': empty list")
                        elif isinstance(v, (str, int, float)):
                            print(f"  Key '{k}': {v}")
            except json.JSONDecodeError:
                # Text file
                text = content.decode("utf-8", errors="replace")
                print(f"Type: Text")
                print(f"First 300 chars: {text[:300]}")
                # Check if it has limit-related keywords
                keywords = ["涨跌", "涨停", "跌停", "limit", "maxup", "maxdown"]
                found = [kw for kw in keywords if kw in text]
                if found:
                    print(f"⚠️  CONTAINS: {found}")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                pass  # silent
            else:
                print(f"\n{name}/{prefix}: HTTP {e.code}")
        except Exception as e:
            pass  # silent


def main():
    """Probe all file prefixes on SHFE and INE."""
    print("=" * 60)
    print("SHFE/INE 数据文件探测")
    print("=" * 60)

    prefixes = [
        ("kx", "日行情(已有)"),
        ("js", "结算参数(已有)"),
        ("cs", "未知-可能结算相关"),
        ("jy", "未知-可能交易相关"),
        ("zb", "未知-可能报表"),
    ]

    for prefix, desc in prefixes:
        print(f"\n--- 探测 {prefix} ({desc}) ---")
        probe_shfe_prefix(prefix)

    # Also try some alternative date samples
    print(f"\n{'=' * 60}")
    print("尝试其他日期格式")
    print(f"{'=' * 60}")
    for date in ["20260428"]:
        for prefix in ["kx", "js", "cs", "jy", "zb"]:
            for path_pattern in [
                f"/data/dailydata/{prefix}/{prefix}{date}.dat",
                f"/data/tradedata/future/dailydata/{prefix}{date}.dat",
            ]:
                url = f"{SHFE_BASE_URL}{path_pattern}"
                try:
                    req = urllib.request.Request(
                        url,
                        headers={
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                        },
                    )
                    resp = urllib.request.urlopen(req, timeout=5)
                    if resp.status == 200:
                        print(f"✓ {path_pattern}")
                except urllib.error.HTTPError:
                    pass
                except Exception:
                    pass

    print(f"\n{'=' * 60}")
    print("结论")
    print(f"{'=' * 60}")
    print("""
    SHFE/INE 的 kx*.dat(日行情)和 js*.dat(结算参数)之外没有发现其他活跃的数据文件。
    SHFE/INE 的涨跌停板幅度通常以公告(notice)形式发布, 不提供每日数据文件下载。
    
    可能的解决方案:
    1. 从 kx*.dat 的 PRESETTLEMENTPRICE 计算(需被告知的涨跌停板比例)
    2. 期货公司往往内部有接口
    3. 涨跌停板幅度通常只在重大调整时才变化(如节假日、异常波动),
       可以从交易所各品种合约规格页面获取基准涨跌停板幅度
    """)


if __name__ == "__main__":
    main()
