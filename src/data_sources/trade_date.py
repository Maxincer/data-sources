#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
上交所交易日历工具。

存储为本地纯文本文件，每行一个 YYYYMMDD 整数，升序排列。
所有查询函数从本地文件读取，模块级缓存避免重复 I/O。

Shell 使用示例:
    grep -qx 20260506 data/trade_dates.txt             # 检查是否交易日
    awk -v d=20260506 '\$1<=d{last=\$1} END{print last}' \
        data/trade_dates.txt  # 向前最近
"""

from bisect import bisect_left, bisect_right
from datetime import datetime
from pathlib import Path
from typing import Optional
import os


DATA_DIR = Path(os.environ["DATA_DIR"])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load(force: bool = False) -> list[str]:
    """
    读取交易日历文件，返回升序字符串列表。

    模块级缓存：通过函数属性 load._cache 实现，首次调用后驻留内存。
    设置 force=True 可强制重新读取文件（手工改数据后使用）。
    """
    if not force:
        cache = getattr(load, "_cache", None)
        if cache is not None:
            return cache

    filepath = DATA_DIR / "trade_dates.txt"
    if not filepath.exists():
        load._cache = []
        return []

    with open(filepath, "r", encoding="utf-8") as f:
        load._cache = [line.strip() for line in f if line.strip()]
    return load._cache


def prev_trading_date(date_or_str) -> Optional[str]:
    """
    查找给定日期之前的最后一个交易日（不含自身）。
    """
    d = _normalize(date_or_str)
    if d is None:
        return None
    trade_dates = load()
    if not trade_dates:
        return None
    idx = bisect_left(trade_dates, d)
    return trade_dates[idx - 1] if idx > 0 else None


def nearest(date_or_str) -> Optional[str]:
    """
    向前查找最近交易日（含自身）。
    """
    d = _normalize(date_or_str)
    if d is None:
        return None
    trade_dates = load()
    if not trade_dates:
        return None
    idx = bisect_right(trade_dates, d)
    return trade_dates[idx - 1] if idx > 0 else None


def is_trading(date_or_str) -> bool:
    """
    判断给定日期是否为上交所交易日。
    """
    d = _normalize(date_or_str)
    if d is None:
        return False
    trade_dates = load()
    if not trade_dates:
        return False
    idx = bisect_right(trade_dates, d)
    return idx > 0 and trade_dates[idx - 1] == d


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize(d) -> Optional[str]:
    """统一输入为 YYYYMMDD 字符串，非法返回 None。"""
    if isinstance(d, str):
        d = d.replace("-", "")
        try:
            datetime.strptime(d, "%Y%m%d")
            return d
        except ValueError:
            return None
    if isinstance(d, int):
        d_str = str(d)
        try:
            datetime.strptime(d_str, "%Y%m%d")
            return d_str
        except ValueError:
            return None
    if isinstance(d, datetime):
        return d.strftime("%Y%m%d")
    return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 3:
        cmd, arg = sys.argv[1], sys.argv[2]
        if cmd == "prev":
            print(prev_trading_date(arg) or "")
        elif cmd == "nearest":
            print(nearest(arg) or "")
        elif cmd == "is_trading":
            print("yes" if is_trading(arg) else "no")
        else:
            print(f"未知命令: {cmd}")
    else:
        cached_dates = load()
        print(f"交易日: {len(cached_dates)} 个")
        first = cached_dates[0] if cached_dates else "N/A"
        last = cached_dates[-1] if cached_dates else "N/A"
        print(f"  最早: {first}")
        print(f"  最晚: {last}")
