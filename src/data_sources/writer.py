#!/usr/bin/env python3
"""Writer: parse raw data and upsert into MySQL."""

import argparse
import logging
from pathlib import Path

from data_sources.parser import parse_file, ParseStats, merge_by_code_date
from data_sources.modifier import (should_filter_contract,
    fix_dce_limit_prices, fix_gfe_margin, fix_gfe_limit_prices,
    fix_all_margin, fill_zero_volume_close,
    fill_cffex_margin_from_history)
from data_sources.trade_date import prev_trading_date
from data_sources.db import create_table, upsert_records
from mxz_utils.logging_config import get_logger


RAW_DIR = Path("./data/raw")

logger = get_logger(
    "data_sources.writer", logging.DEBUG, "./logs", "writer",
)


def _apply_modifiers(records):
    """Apply all Modifier transformations + filters to records."""
    records = fix_dce_limit_prices(records)
    records = fix_gfe_margin(records)
    records = fix_gfe_limit_prices(records)
    records = fix_all_margin(records)
    records = fill_zero_volume_close(records)
    records = fill_cffex_margin_from_history(records)

    before = len(records)
    records = [
        r for r in records
        if not should_filter_contract(r.get("code", ""))
    ]
    filtered = before - len(records)
    if filtered:
        logger.info("Modifier 过滤: %s 条 (TAS/EFP)", filtered)
    before_csi = len(records)
    records = [r for r in records if not r.get("code", "").endswith(".CSI")]
    csi_filtered = before_csi - len(records)
    if csi_filtered:
        logger.info("Modifier 过滤: %s 条 (CSI 指数)", csi_filtered)
    return records


def _prev_trading_day(date_str: str) -> str:
    """获取前一交易日（YYYYMMDD）。"""
    return prev_trading_date(date_str) or date_str


def _ensure_prev_dce_data(date_str: str):
    """确保前一交易日的 DCE 数据存在，缺失则下载。"""
    prev_date = _prev_trading_day(date_str)
    needed = [
        f"{prev_date}.DCE.TradingParameters.json",
        f"{prev_date}.DCE.DailyMarketData.json",
        f"{prev_date}.DCE.SettlementParameters.json",
    ]
    missing = [f for f in needed if not (RAW_DIR / f).exists()]
    if not missing:
        return

    logger.info("DCE 缺失前一交易日数据，正在下载 %s...", prev_date)
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
                        logger.info("  ✅ %s", fname)
                    else:
                        logger.warning("  ⚠ %s 下载失败", fname)
                    break
    except Exception as e:
        logger.warning("下载失败: %s", e)


def write_trade_date(date_str: str, dry_run: bool = False,
                      config_override: dict | None = None):
    """Parse and write data for a specific trade date."""
    _ensure_prev_dce_data(date_str)
    prev_date = _prev_trading_day(date_str)
    logger.info("写入日期: %s (前日: %s)", date_str, prev_date)

    records = []
    stats_list = []

    for fpath in sorted(RAW_DIR.iterdir()):
        if not fpath.is_file() or fpath.suffix == ".jsonl":
            continue
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
            logger.debug("解析 %s: %s 条记录", fpath.name, len(parsed))
        except Exception as e:
            stats.add_error(str(e))
            logger.error("解析失败 %s: %s", fpath.name, e)
        stats_list.append(stats)

    if not records:
        logger.info("无数据可处理")
        return 0, [s.stats_summary for s in stats_list]

    records = merge_by_code_date(records)
    records = _apply_modifiers(records)

    target_records = [r for r in records if r.get("date") == date_str]
    logger.info("目标日期记录数: %s", len(target_records))
    if dry_run:
        return len(target_records), [s.stats_summary for s in stats_list]
    create_table(config_override)
    written = upsert_records(target_records, config_override)
    logger.info("写入完成: %s 条", written)
    return written, [s.stats_summary for s in stats_list]


def write_all(dry_run: bool = False,
              config_override: dict | None = None):
    """Parse and write all raw data files."""
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
            logger.debug("解析 %s: %s 条记录", fpath.name, len(parsed))
        except Exception as e:
            stats.add_error(str(e))
            logger.error("解析失败 %s: %s", fpath.name, e)
        stats_list.append(stats)

    if not records:
        logger.info("无数据可处理")
        return 0, [s.stats_summary for s in stats_list]

    records = merge_by_code_date(records)
    records = _apply_modifiers(records)
    logger.info("写入总记录数: %s", len(records))
    if dry_run:
        return len(records), [s.stats_summary for s in stats_list]
    create_table(config_override)
    written = upsert_records(records, config_override)
    logger.info("写入完成: %s 条", written)
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
    parser.add_argument(
        "--host", default=None,
        help="MySQL host (default: 192.168.1.202)"
    )
    parser.add_argument(
        "--port", type=int, default=None,
        help="MySQL port (default: 3306)"
    )
    parser.add_argument(
        "--db", default=None,
        help="MySQL database name (default: future_cn)"
    )
    parser.add_argument(
        "--table", default=None,
        help="MySQL table name (default: t_futures_info_exchange)"
    )
    args = parser.parse_args()

    config_override = {
        k: v for k, v in {
            "host": args.host,
            "port": args.port,
            "database": args.db,
            "table": args.table,
        }.items() if v is not None
    }

    if args.date:
        written, stats = write_trade_date(
            args.date, args.dry_run, config_override
        )
    else:
        written, stats = write_all(args.dry_run, config_override)

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
