#!/usr/bin/env python3
"""
order_limits_report.py — 按合约查询 minoq/maxoq 溯源报告

用法:
  python scripts/order_limits_report.py i2609.DCE lc2609.GFE MA2605.CZC
  python scripts/order_limits_report.py                          # 全量
  python scripts/order_limits_report.py --today                   # 当日活跃合约
"""

import argparse
import csv
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SUFFIX_TO_EXCHANGE = {
    ".DCE": "DCE", ".CZC": "CZCE", ".SHF": "SHFE",
    ".INE": "INE", ".GFE": "GFEX", ".CFE": "CFFEX",
}
# 反向映射
EXCHANGE_TO_SUFFIX = {v: k for k, v in SUFFIX_TO_EXCHANGE.items()}

# 合约代码 → 查找键（字母部分大写）
def variety_key(code: str) -> str:
    raw = code.split(".")[0]
    return "".join(c for c in raw if c.isalpha()).upper()


def load_csv_db() -> dict:
    """加载 order_limits.csv 为字典，同时存入去连字符键（处理 DCE -F）。"""
    db = {}
    path = PROJECT_ROOT / "data" / "order_limits.csv"
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(l for l in f if not l.startswith("#"))
        for row in reader:
            ex = row["exchange"]
            vid = row["variety_id"].strip().upper()
            db[(ex, vid)] = row
            # 去连字符副键（DCE -F 月均价合约）
            stripped = vid.replace("-", "")
            if stripped != vid:
                db[(ex, stripped)] = row
    return db


def parse_notes(row: dict) -> dict:
    """解析 notes 列为结构化信息。"""
    notes = (row.get("notes") or "").strip()
    general = notes
    adjustment = ""
    if "通用规则:" in notes:
        parts = notes.split("|")
        general = ""
        for p in parts:
            p = p.strip()
            if p.startswith("通用规则:"):
                general = p[len("通用规则:"):].strip()
            elif p.startswith("调整规则:"):
                adjustment = p[len("调整规则:"):].strip()
    return {"general": general, "adjustment": adjustment}


def resolve_record(db: dict, contract: str) -> dict:
    """解析单个合约的 minoq/maxoq 溯源信息。"""
    if "." not in contract:
        # 无后缀则尝试匹配
        return None
    suffix = "." + contract.split(".")[-1]
    exchange = SUFFIX_TO_EXCHANGE.get(suffix)
    if exchange is None:
        return None

    vid = variety_key(contract)
    if not vid:
        return None

    # 尝试精确匹配 → 去连字符 → ALL 通配
    entry = db.get((exchange, vid))
    if entry is None:
        stripped = vid.replace("-", "")
        entry = db.get((exchange, stripped))
    if entry is None:
        entry = db.get((exchange, "ALL"))
    # INE 没有 ALL，用 SHFE ALL?
    if entry is None and exchange == "INE":
        pass  # INE 精确匹配失败即无数据

    if entry is None:
        return {
            "contract": contract,
            "exchange": exchange,
            "found": False,
            "minoq": None,
            "maxoq": None,
            "general_rule": "",
            "adjustment_rule": "",
        }

    info = parse_notes(entry)
    return {
        "contract": contract,
        "exchange": exchange,
        "found": True,
        "minoq": entry.get("minoq", ""),
        "maxoq_limit": entry.get("maxoq_limit", ""),
        "maxoq_market": entry.get("maxoq_market", ""),
        "general_rule": info["general"],
        "adjustment_rule": info["adjustment"],
        "csv_variety_id": entry["variety_id"],
    }


def format_report(results: list) -> str:
    """生成可读报告。"""
    lines = []
    lines.append("")
    lines.append("=" * 72)
    lines.append(" 期货合约 minoq/maxoq 溯源报告")
    lines.append("=" * 72)

    for r in results:
        contract = r["contract"]
        ex = r["exchange"]
        found = r["found"]

        lines.append("")
        if not found:
            lines.append(f"  ⚠ {contract}  — 未在 order_limits.csv 中找到配置")
            continue

        minoq = r.get("minoq") or "—"
        maxoq_limit = r.get("maxoq_limit") or "—"
        maxoq_market = r.get("maxoq_market") or "—"
        csv_vid = r.get("csv_variety_id", "")

        # 根据交易所判断数据来源
        if ex == "CZCE":
            minoq_src = "📄 CZCE TradingParameters 日频文件提取"
            maxoq_src = "📄 CZCE TradingParameters 日频文件提取"
        elif ex == "DCE":
            minoq_src = "📋 order_limits.csv"
            maxoq_src = "📡 DCE tradingParam API (maxHand 实时)"
        elif ex == "SHFE":
            minoq_src = f"📋 order_limits.csv (通配: {csv_vid})"
            maxoq_src = f"📋 order_limits.csv (通配: {csv_vid})"
        else:
            minoq_src = f"📋 order_limits.csv (逐品种: {csv_vid})"
            maxoq_src = f"📋 order_limits.csv (逐品种: {csv_vid})"

        lines.append(f"  ┌─ {contract}")
        lines.append(f"  ├─ 手工-限价单 maxoq = {str(maxoq_limit):>5} 手  ← {maxoq_src}")
        if maxoq_market and maxoq_market != maxoq_limit:
            lines.append(f"  ├─ 市价单  maxoq = {str(maxoq_market):>5} 手  ← 📋 order_limits.csv")
        lines.append(f"  ├─ minoq          = {str(minoq):>5} 手  ← {minoq_src}")
        lines.append(f"  ├─ 通用规则: {r['general_rule']}")
        if r["adjustment_rule"]:
            lines.append(f"  └─ 调整规则: {r['adjustment_rule']}")
        else:
            lines.append(f"  └─ 调整规则: —")

    lines.append("")
    lines.append("=" * 72)
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="minoq/maxoq 溯源报告")
    parser.add_argument("contracts", nargs="*", help="合约代码，如 i2609.DCE lc2609.GFE")
    args = parser.parse_args()

    db = load_csv_db()

    if args.contracts:
        results = [resolve_record(db, c) for c in args.contracts]
    else:
        # 全量输出（按交易所分组）
        results = []
        seen = set()
        for (ex, vid), row in sorted(db.items()):
            if (ex, vid) in seen:
                continue
            seen.add((ex, vid))
            suffix = EXCHANGE_TO_SUFFIX.get(ex, "")
            code = f"{vid}{suffix}"
            r = resolve_record(db, code)
            if r:
                results.append(r)

    print(format_report(results))


if __name__ == "__main__":
    main()
