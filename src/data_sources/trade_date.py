#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
上交所交易日历工具。

存储为本地纯文本文件，每行一个 YYYYMMDD 整数，升序排列。
所有查询函数从本地文件读取，模块级缓存避免重复 I/O。
仅在调用 update() 时引入 tushare。

Shell 使用示例:
    grep -qx 20260506 data/trade_dates.txt             # 检查是否交易日
    awk -v d=20260506 '$1<=d{last=$1} END{print last}' \
        data/trade_dates.txt  # 向前最近

Token: 01f10e786b76dd6212829418aa979f3e578253f57eecbf0e12c9a44b
"""

from bisect import bisect_left, bisect_right
from datetime import date, datetime
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

_TUSHARE_TOKEN = "01f10e786b76dd6212829418aa979f3e578253f57eecbf0e12c9a44b"
_DEFAULT_START_YEAR = 1990
_API_INTERVAL = 0.6  # 秒，保底 < 1.7 req/s，远低于限流
_DEFAULT_DATA_DIR = Path("./data")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def update(data_dir: Optional[Path] = None) -> list[str]:
    """
    从 tushare 拉取上交所全部交易日，写入本地文件。

    仅在调用此函数时才 import tushare。

    Args:
        data_dir: 存储目录，默认 ./data。自动创建。

    Returns:
        升序字符串列表 ["19901219", "19901220", ...]
    """
    if data_dir is None:
        data_dir = _DEFAULT_DATA_DIR
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    filepath = data_dir / "trade_dates.txt"

    # 仅在 update 内按需引入 tushare
    import time  # pylint: disable=import-outside-toplevel
    import tushare as ts  # pylint: disable=import-outside-toplevel

    pro = ts.pro_api(_TUSHARE_TOKEN)
    all_dates: set[str] = set()
    end_year = date.today().year + 1

    for y in range(_DEFAULT_START_YEAR, end_year + 1):
        df = pro.trade_cal(
            exchange="SSE",
            start_date=f"{y}0101",
            end_date=f"{y}1231",
        )
        trading = df[df["is_open"] == 1]["cal_date"].tolist()
        all_dates.update(trading)

        time.sleep(_API_INTERVAL)

    result = sorted(all_dates)

    with open(filepath, "w", encoding="utf-8") as f:
        for d in result:
            f.write(d + "\n")

    # 刷新函数属性缓存
    load._cache = result
    return result


def load(force: bool = False) -> list[str]:
    """
    读取交易日历文件，返回升序字符串列表。

    模块级缓存：通过函数属性 load._cache 实现，首次调用后驻留内存。
    设置 force=True 可强制重新读取文件（手工改数据后使用）。

    Args:
        force: 是否强制刷新缓存，重新读盘。

    Returns:
        list[str] 空列表表示文件不存在。
    """
    if not force:
        cache = getattr(load, "_cache", None)
        if cache is not None:
            return cache

    filepath = _DEFAULT_DATA_DIR / "trade_dates.txt"
    if not filepath.exists():
        load._cache = []
        return []

    with open(filepath, "r", encoding="utf-8") as f:
        load._cache = [line.strip() for line in f if line.strip()]
    return load._cache


def prev_trading_date(date_or_str) -> Optional[str]:
    """
    查找给定日期之前的最后一个交易日（不含自身）。

    与 nearest() 的区别：nearest 含自身（≤），prev_trading_date 不含自身（<）。

    Args:
        date_or_str: date 对象 / "YYYYMMDD" / "YYYY-MM-DD"

    Returns:
        "YYYYMMDD" 格式字符串，无匹配返回 None。
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

    Args:
        date_or_str: date 对象 / "YYYYMMDD" / "YYYY-MM-DD"

    Returns:
        "YYYYMMDD" 格式字符串，无匹配返回 None。
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

    Args:
        date_or_str: date 对象 / "YYYYMMDD" / "YYYY-MM-DD"

    Returns:
        bool
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
    if isinstance(d, date):
        return d.strftime("%Y%m%d")
    return None


# ---------------------------------------------------------------------------
# Compatibility aliases (used by writer.py and other modules)
# ---------------------------------------------------------------------------


def nearest_trading_day(d: date) -> date | None:
    """获取给定日期最近的交易日（date → date），兼容旧版接口。"""
    s = nearest(d)
    if s is None:
        return None
    return datetime.strptime(s, "%Y%m%d").date()


def yyyymmdd(d: date) -> str:
    """date → YYYYMMDD 字符串。"""
    return d.strftime("%Y%m%d")


def to_date(s: str) -> date | None:
    """YYYYMMDD 字符串 → date。"""
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    if "--update" in sys.argv:
        updated_dates = update()
        cal_file = _DEFAULT_DATA_DIR / "trade_dates.txt"
        print(f"已更新 {cal_file}，共 {len(updated_dates)} 个交易日")
        print(f"  最早: {updated_dates[0]}")
        print(f"  最晚: {updated_dates[-1]}")
    elif len(sys.argv) >= 3:
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
