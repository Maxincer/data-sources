#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Writer: parse raw data files and upsert into MySQL t_futures_info_exchange.

Usage:
    python3 -m data_sources.writer            # parse and write all
    python3 -m data_sources.writer --dry-run   # parse only, no write
"""

import argparse
from pathlib import Path

from data_sources.parser import parse_file, ParseStats, merge_by_code_date
from data_sources.modifier import should_filter_contract, fill_tas_settle, fix_dce_limit_prices, fix_gfe_margin, fix_all_margin, fill_zero_volume_close
from data_sources.trade_date import prev_trading_date
from data_sources.db import create_table, upsert_records
RAW_DIR = Path("./data/raw")


def _apply_modifiers(records):
    """Apply all Modifier transformations + filters to records."""
    records = fix_dce_limit_prices(records)
    records = fix_gfe_margin(records)
    records = fix_all_margin(records)
    records = fill_zero_volume_close(records)
    records = fill_tas_settle(records)
    before = len(records)
    records = [r for r in records if not should_filter_contract(r.get("code", ""))]
    filtered = before - len(records)
    if filtered:
        print(f"  Modifier 过滤: {filtered} 条 (EFP)")
    # 排除 CSI 指数数据（仅写入期货数据）
    before_csi = len(records)
    records = [r for r in records if not r.get("code", "").endswith(".CSI")]
    csi_filtered = before_csi - len(records)
    if csi_filtered:
        print(f"  Modifier 过滤: {csi_filtered} 条 (CSI 指数)")
    return records


def _prev_trading_day(date_str: str) -> str:
    """获取前一交易日（YYYYMMDD）。"""
    return prev_trading_date(date_str) or date_str


def _ensure_prev_dce_data(date_str: str):
    """确保前一交易日的 DCE 数据存在，缺失则下载。"""
    prev_date = _prev_trading_day(date_str)
    # Modifier 需要：交易参数表（riseLimit/fallLimit/margin）+ 日行情（lastClear）+ 结算参数（备用margin）
    needed = [
        f"{prev_date}.DCE.TradingParameters.json",
        f"{prev_date}.DCE.DailyMarketData.json",
        f"{prev_date}.DCE.SettlementParameters.json",
    ]
    missing = [f for f in needed if not (RAW_DIR / f).exists()]
    if not missing:
        return

    # 整理需要下载的接口名
    file_to_func = {
        "TradingParameters": "fetch_dce_tradepara",
        "SettlementParameters": "fetch_dce_settlement",
        "DailyMarketData": "fetch_dce_market",
    }
    print(f"  DCE 缺失前一交易日数据，正在下载 {prev_date}...")
    try:
        from data_sources.fetcher import (
            fetch_dce_tradepara, fetch_dce_settlement,
        )
        funcs = {
            "TradingParameters": fetch_dce_tradepara,
            "SettlementParameters": fetch_dce_settlement,
        }
        for fname in missing:
            for suffix, func in funcs.items():
                if suffix in fname:
                    ok = func(prev_date)
                    if ok:
                        print(f"    ✅ {fname}")
                    else:
                        print(f"    ⚠ {fname} 下载失败")
                    break
    except Exception as e:
        print(f"  ⚠ 下载失败: {e}")


def write_trade_date(date_str: str, dry_run: bool = False):
    # 确保 Modifier 所需的前一日 DCE 数据存在
    _ensure_prev_dce_data(date_str)

    # 加载目标日期 + 前一日的 DCE 数据（Modifier 需要前日值做继承）
    prev_date = _prev_trading_day(date_str)

    records = []
    stats_list = []

    for fpath in sorted(RAW_DIR.iterdir()):
        if not fpath.is_file() or fpath.suffix == ".jsonl":
            continue
        # 加载目标日期 和 前一日 的文件
        if date_str not in fpath.name and prev_date not in fpath.name:
            continue
        name = fpath.name
        exchange = name.split(".")[1] if len(name.split(".")) > 1 else "?"
        data_type = name.split(".")[2] if len(name.split(".")) > 2 else "?"
        stats = ParseStats(exchange, data_type)
        try:
            parsed = parse_file(fpath)
            stats.total = len(parsed)
            for rec in parsed:
                stats.add_record(rec)
            records.extend(parsed)
        except Exception as e:
            stats.add_error(str(e))
        stats_list.append(stats)

    if not records:
        return 0, [s.stats_summary for s in stats_list]

    records = merge_by_code_date(records)
    records = _apply_modifiers(records)

    # 写库时只写入目标日期的数据，排除前一日（仅用于 Modifier)）
    target_records = [r for r in records if r.get("date") == date_str]
    if dry_run:
        return len(target_records), [s.stats_summary for s in stats_list]
    create_table()
    written = upsert_records(target_records)
    return written, [s.stats_summary for s in stats_list]


def write_all(dry_run: bool = False):
    records = []
    stats_list = []

    for fpath in sorted(RAW_DIR.iterdir()):
        if not fpath.is_file() or fpath.suffix == ".jsonl":
            continue
        name = fpath.name
        exchange = name.split(".")[1] if len(name.split(".")) > 1 else "?"
        data_type = name.split(".")[2] if len(name.split(".")) > 2 else "?"
        stats = ParseStats(exchange, data_type)
        try:
            parsed = parse_file(fpath)
            stats.total = len(parsed)
            for rec in parsed:
                stats.add_record(rec)
            records.extend(parsed)
        except Exception as e:
            stats.add_error(str(e))
        stats_list.append(stats)

    if not records:
        return 0, [s.stats_summary for s in stats_list]

    records = merge_by_code_date(records)
    records = _apply_modifiers(records)
    if dry_run:
        return len(records), [s.stats_summary for s in stats_list]
    create_table()
    written = upsert_records(records)
    return written, [s.stats_summary for s in stats_list]


def main():
    parser = argparse.ArgumentParser(
        description="Parse raw data and write to DB"
    )
    parser.add_argument("--date", help="Trade date (YYYYMMDD), default: all")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse only, do not write to DB"
    )
    args = parser.parse_args()

    if args.date:
        written, stats = write_trade_date(args.date, args.dry_run)
    else:
        written, stats = write_all(args.dry_run)

    action = "[DRY RUN]" if args.dry_run else ""
    print(f"{action} Records written: {written}")
    for s in stats:
        missing = ", ".join(
            [f"{k}:{v}" for k, v in s.get("missing", {}).items() if v > 0]
        )
        print(
            f"  {s['exchange']:10s} {s['data_type']:20s} "
            f"total={s['total_records']:5d} "
            f"err={s['errors']} "
            + (f"missing=[{missing}]" if missing else "")
        )


if __name__ == "__main__":
    main()
