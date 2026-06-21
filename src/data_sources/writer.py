#!/usr/bin/env python3
import argparse
import csv
import json
import os
from pathlib import Path
import re

from mxz_utils.logging_config import get_logger

from data_sources.parser import parse_file, ParseStats, merge_by_code_date
from data_sources.modifier import (should_filter_contract,
    pad_czce_code,
    fix_dce_limit_prices, fix_gfe_margin, fix_gfe_limit_prices,
    fix_all_margin, fill_zero_volume_close,
    fill_cffex_margin_from_history, fill_if_basis,
    margin_to_pct)
from data_sources.trade_date import prev_trading_date
from data_sources.db import upsert_rows, delete_rows


RAW_DIR = Path(os.environ["DATA_DIR"]) / "raw" / "structured"

logger = get_logger(
    "Writer", 'DEBUG', os.environ["LOG_DIR"], "Writer",
)


# exchange suffix → config exchange name
_SUFFIX_TO_EXCHANGE = {
    ".DCE": "DCE", ".CZC": "CZCE", ".SHF": "SHFE",
    ".INE": "INE", ".GFE": "GFEX", ".CFE": "CFFEX",
}


def _parse_eff_date_sort(eff: str) -> int:
    """effective_date → 排序键.
    '20260415' → 20260415, '2026MMDD' → 20260101, '' → 0
    """
    d = eff.strip().replace("-", "")
    if not d:
        return 0
    d = re.sub(r'[A-Z]+', lambda m: '01' * (len(m.group()) // 2), d)
    return int(d) if d.isdigit() else 0


def security_id_to_product_code(security_id: str) -> str:
    """合约代码 → 品种代码.
    'CU2507' → 'CU', 'PP2509-F' → 'PP-F', 'M2601' → 'M', 'SC2606TAS' → 'SC'.

    - 数字后如有 F 后缀, 保留为产品代码的一部分
    - 数字后如有 TAS/EFP 后缀, 剥离
    - 其他模式视为不合法
    """
    m = re.search(r'^([A-Za-z]+)\d+([A-Za-z]+)?$', security_id)
    if not m:
        raise ValueError(
            f"cannot extract product_code from security_id: {security_id}"
        )
    base = m.group(1).upper()
    suffix = m.group(2) or ''
    if suffix.upper() == 'F':
        return base + '-F'
    if suffix.upper() in ['TAS', 'EFP']:
        return base
    if not suffix:
        return base
    raise ValueError(f'Unknown suffix: {security_id}')


class OrderLimitBook:
    """从 fields_from_announcements.csv + announcements_metadata.json 构建.

    三级规则:
      daily   (日常公告调整)  — 最高优先级
      product (品种业务细则)
      general (交易所规则)    — 最低优先级
    """

    LEVELS = ("daily", "product", "general")

    def __init__(self):
        # rules[level]: [(sort_key, exchange, product_code, security_id, field, value), ...]
        self._rules: dict[str, list[tuple]] = {lv: [] for lv in self.LEVELS}
        self._loaded = False

    def load(self, csv_path, meta_path):
        if self._loaded:
            return

        # 加载 metadata: aid → category
        aid_cat: dict[str, str] = {}
        if meta_path.exists():
            with open(meta_path, encoding="utf-8") as f:
                meta = json.load(f)
            for aid, entry in meta.items():
                cat = entry.get("category", "").strip().lower()
                if cat in self.LEVELS:
                    aid_cat[aid] = cat

        if csv_path.exists():
            with open(csv_path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ex = row.get("exchange", "").strip()
                    pc = row.get("product_code", "").strip().upper()
                    field = row.get("field", "").strip()
                    val = row.get("value", "").strip()
                    sid = row.get("security_id", "").strip()
                    eff = row.get("effective_date", "").strip()
                    aid = row.get("announcement_id", "").strip()
                    if not ex or not pc or field not in ("minoq", "maxoq") or not val:
                        continue
                    try:
                        val = int(val)
                    except ValueError:
                        continue
                    sort_key = _parse_eff_date_sort(eff)
                    cat = aid_cat.get(aid, "daily")  # 默认 daily (最保守)
                    self._rules[cat].append(
                        (sort_key, ex, pc, sid, field, val)
                    )
        else:
            logger.warning("fields_from_announcements.csv 不存在: %s", csv_path)

        # 各 level 内按 effective_date 升序
        for lv in self.LEVELS:
            self._rules[lv].sort(key=lambda r: r[0])

        self._loaded = True
        logger.info(
            "OrderLimitBook: 加载 daily=%s product=%s general=%s",
            len(self._rules["daily"]),
            len(self._rules["product"]),
            len(self._rules["general"]),
        )

    def _best_for_level(self, level: str, exchange: str,
                        variety_id: str, contract_code: str) -> dict:
        """在指定 level 中按 effective_date 累积应用规则, 返回最新 minoq/maxoq."""
        result = {"minoq": None, "maxoq": None}
        for _, ex, pc, sid, field, val in self._rules[level]:
            if ex != exchange:
                continue
            # 品种匹配: 具体品种 或 通配
            if pc != variety_id and pc != "ALL":
                continue
            # 合约匹配: 具体合约 或 通配
            if sid != "ALL" and sid != contract_code:
                continue
            if field == "minoq":
                result["minoq"] = val
            elif field == "maxoq":
                result["maxoq"] = val
        return result

    def get(self, exchange: str, contract_code: str) -> dict:
        """按优先级 (daily > product > general) 取最新 minoq/maxoq.
        返回: {"minoq": int|None, "maxoq": int|None}
        """
        # 无数字 → 品种级代码（如 A, V, L-F, PP-F），直接作为 variety_id
        # 有数字 → 合约级代码（如 A2609, L2607F），需转换为品种代码
        if not any(c.isdigit() for c in contract_code):
            variety_id = contract_code
        else:
            variety_id = security_id_to_product_code(contract_code)
        result = {"minoq": None, "maxoq": None}
        for lv in self.LEVELS:
            lv_result = self._best_for_level(lv, exchange, variety_id, contract_code)
            for fld in ("minoq", "maxoq"):
                if result[fld] is None and lv_result[fld] is not None:
                    result[fld] = lv_result[fld]
        return result


_book: OrderLimitBook | None = None


def _get_book() -> OrderLimitBook:
    global _book
    if _book is None:
        _book = OrderLimitBook()
        data_dir = Path(os.environ["DATA_DIR"])
        csv_path = data_dir / "fields_from_announcements.csv"
        meta_path = data_dir / "raw" / "announcements" / "announcements_metadata.json"
        _book.load(csv_path, meta_path)
    return _book


def inject_order_limits(records: list) -> list:
    """对缺少 minoq/maxoq 的记录, 从 fields_from_announcements.csv 注入."""
    book = _get_book()
    for rec in records:
        code = rec["code"]
        if "." not in code:
            raise ValueError(f"缺少交易所后缀: {code!r}")
        suffix = "." + code.split(".")[-1]
        exchange = _SUFFIX_TO_EXCHANGE.get(suffix)
        if exchange is None:
            continue
        raw = code.split(".")[0]
        limits = book.get(exchange, raw.upper())
        if "minoq" not in rec and limits["minoq"] is not None:
            rec["minoq"] = limits["minoq"]
        if "maxoq" not in rec and limits["maxoq"] is not None:
            rec["maxoq"] = limits["maxoq"]
    return records


def _apply_modifiers(records):
    """Apply all Modifier transformations + filters to records."""
    # ---- Code normalization (always first) ----
    for r in records:
        code = r.get("code", "")
        # Uppercase all codes
        code = code.upper()
        # CZCE 3→4 digit expansion
        if code.endswith(".CZC"):
            code = pad_czce_code(code)
        r["code"] = code

    records = fix_dce_limit_prices(records)
    records = fix_gfe_margin(records)
    records = fix_gfe_limit_prices(records)
    records = fix_all_margin(records)
    records = fill_zero_volume_close(records)
    records = fill_cffex_margin_from_history(records)
    records = fill_if_basis(records)

    # ---- 公司口径调整 - Wind 标准 ----
    for r in records:
        # margin: decimal → 百分数 (0.12 → 12.0)
        if r.get("long_margin") is not None:
            r["long_margin"] = margin_to_pct(r["long_margin"])
        if r.get("short_margin") is not None:
            r["short_margin"] = margin_to_pct(r["short_margin"])
        # date: YYYYMMDD → YYYY-MM-DD
        d = r.get("date", "")
        if d and isinstance(d, str) and len(d) == 8:
            r["date"] = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    # ---- 公司口径调整 end ----

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


def _check_prev_dce_data(date_str: str):
    """检查前一交易日的 DCE 数据是否存在，缺失则报错中断。"""
    prev_date = _prev_trading_day(date_str)
    needed = [
        f"{prev_date}.DCE.TradingParameters.json",
        f"{prev_date}.DCE.DailyMarketData.json",
        f"{prev_date}.DCE.SettlementParameters.json",
    ]
    missing = [f for f in needed if not (RAW_DIR / f).exists()]
    if missing:
        raise FileNotFoundError(
            f"缺失前一交易日 ({prev_date}) DCE 数据: {missing}. "
            f"请先运行 Fetcher: python -m data_sources.fetcher run {prev_date}"
        )


def write_trade_date(date_str: str, table: str,
                      dry_run: bool = False,
                      config_override: dict | None = None):
    """Parse and write data for a specific trade date."""
    _check_prev_dce_data(date_str)
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

    records = merge_by_code_date(records, date_str)
    records = inject_order_limits(records)  # 外部 CSV → minoq/maxoq 合并
    records = _apply_modifiers(records)

    # 过滤品种级/指数级记录（无合约月份），只留合约级数据写入数据库
    records = [
        r for r in records if any(c.isdigit() for c in r.get("code", ""))
    ]

    norm_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    target_records = [r for r in records if r.get("date") == norm_date]
    logger.info("目标日期记录数: %s", len(target_records))
    if dry_run:
        return len(target_records), [s.stats_summary for s in stats_list]

    deleted = delete_rows(date_str, table, config_override)
    logger.info("已删除 %s 条旧数据 (date=%s)", deleted, date_str)
    written = upsert_rows(target_records, table, config_override)
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
        help="MySQL host (from env DB_HOST or override)"
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
        }.items() if v is not None
    }

    written, stats = write_trade_date(
        args.date, args.table, args.dry_run, config_override
    )

    action = "[DRY RUN] " if args.dry_run else ""
    total_records = sum(s["total_records"] for s in stats)
    total_errors = sum(s["errors"] for s in stats)
    print(
        f"{action}解析 {total_records} 条"
        f" | 写入 {written} 条"
        + (f" | {total_errors} errors" if total_errors else "")
    )


if __name__ == "__main__":
    main()
