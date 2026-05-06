#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CZCE 交易参数表 (Settlement Parameters → Limit%)
==================================================

CZCE 的结算参数文件是 UTF-8 编码，包含 涨跌停板(%) 字段。
URL 模板: {BASE_URL}/cn/DFSStaticFiles/Future/{YYYY}/{YYYYMMDD}/FutureDataClearParams.txt

实际字段顺序 (| 分隔):
  [0] 合约代码  [1] 当日结算价  [2] 是否单边市  [3] 连续单边市天数
  [4] 交易保证金率(%)  [5] 涨跌停板(%)  [6] 交易手续费  [7] 手续费收取方式
  [8] 交割手续费  [9] 日内平今仓交易手续费  [10] 日持仓限额  [11] 交易限价

样例运行: python3 -m data_sources.discover.czce_tradepara
"""

from pathlib import Path

RAW_DIR = Path("./data/raw")


def parse_czce_limit_pct(fpath: Path) -> list[dict]:
    """Parse CZCE settlement TXT - extract limit% from existing data source."""
    records = []
    with open(fpath, encoding="utf-8") as f:
        lines = f.read().strip().split("\n")

    for line in lines[2:]:  # skip title + header
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 6:
            continue
        code = parts[0]
        if not code or not code[0].isalpha():
            continue

        limit_raw = parts[5]
        rec = {
            "code": code + ".CZC",
            "settle": _float(parts[1]),
            "margin_pct": _pct(parts[4]),
            "limit_pct": _parse_limit(limit_raw),
            "position_limit": _int(parts[10]) if len(parts) > 10 else None,
        }
        records.append(rec)
    return records


def _float(val):
    if val is None:
        return None
    val = str(val).replace(",", "").replace(" ", "").strip()
    if not val or val in ("null", "None", ""):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _int(val):
    v = _float(val)
    return int(v) if v is not None else None


def _pct(val):
    v = _float(val)
    return v / 100.0 if v is not None else None


def _parse_limit(raw):
    """Parse '±13' → 0.13.  The raw value starts with ± (U+00B1)."""
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    # Remove leading ± or other prefix symbols
    text = raw.lstrip("±±+")
    try:
        return float(text.replace(",", "")) / 100.0
    except (ValueError, TypeError):
        return None


def main():
    for fpath in sorted(RAW_DIR.iterdir()):
        if "CZCE" not in fpath.name or "Settlement" not in fpath.name:
            continue
        if fpath.suffix != ".txt":
            continue

        records = parse_czce_limit_pct(fpath)
        print(f"CZCE ({fpath.name}): {len(records)} 条记录")

        with_limit = [r for r in records if r["limit_pct"] is not None]
        print(f"  其中有涨跌停板%: {len(with_limit)}/{len(records)}")
        print()

        if with_limit:
            print(f"{'合约':>12} {'结算价':>10} {'涨跌停%':>8} {'保证金%':>8} {'日持仓限额':>8}")
            print("-" * 55)
            for rec in with_limit[:10]:
                print(
                    f"{rec['code'][:12]:>12} "
                    f"{rec['settle'] or 0:>10.2f} "
                    f"{rec['limit_pct']:>7.0%} "
                    f"{rec['margin_pct'] or 0:>7.0%} "
                    f"{rec['position_limit'] or '':>8}"
                )
            print()


if __name__ == "__main__":
    main()
