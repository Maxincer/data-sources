#!/usr/bin/env python3
"""
改造 order_limits CSV 文件：
1. adjustments.csv: announcement_ref → metadata.jsonl 完整 title
2. 所有文件: 添加 announcement_date/effective_date/source_url 列
3. 所有文件: 展开 "同上" 为完整内容
"""
import csv
import json
from io import StringIO
from pathlib import Path

PROJECT_ROOT = Path("/mnt/e/projects/data-sources")
DATA_DIR = PROJECT_ROOT / "data"
METADATA_FILE = DATA_DIR / "raw" / "announcements" / "metadata.jsonl"


def load_metadata() -> dict:
    """加载 metadata.jsonl，按 url 索引。"""
    meta_by_url = {}
    with open(METADATA_FILE) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            meta_by_url[d["url"]] = d
    return meta_by_url


def read_csv(filepath: Path) -> list[dict]:
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader), reader.fieldnames


def write_csv(filepath: Path, rows: list[dict], fieldnames: list[str]):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✓ {filepath.name} ({len(rows)} rows, {len(fieldnames)} cols)")


def expand_same_as(rows: list[dict], col: str, prev_val: str = None):
    """将 column 中的'同上'替换为上一个非同上行的值。"""
    prev = prev_val or ""
    for r in rows:
        val = r.get(col, "").strip()
        if val in ("同上", "同上。"):
            r[col] = prev
        elif val:
            prev = val


def process_adjustments():
    """调整文件: announcement_ref → metadata 完整 title"""
    meta = load_metadata()
    rows, fns = read_csv(DATA_DIR / "order_limits_adjustments.csv")

    changed = 0
    for r in rows:
        url = r.get("source_url", "")
        if url in meta:
            new_ref = meta[url]["title"]
            if r["announcement_ref"] != new_ref:
                r["announcement_ref"] = new_ref
                changed += 1
        # Also ensure source URL is the canonical one from metadata
        if url in meta:
            r["source_url"] = meta[url]["url"]

    # 用 title 映射做备选
    title_map = {m["title"]: m for m in meta.values()}
    for r in rows:
        ref = r.get("announcement_ref", "")
        if ref in title_map:
            r["source_url"] = title_map[ref]["url"]

    write_csv(DATA_DIR / "order_limits_adjustments.csv", rows, fns)
    print(f"    ({changed} 条 announcement_ref 已更新为完整标题)")


def process_general_rules():
    """通用规则: 加三列，source 已经是完整标题保持不变"""
    rows, fns = read_csv(DATA_DIR / "order_limits_general_rules.csv")

    # 添加新列（如果不存在）
    for col in ["announcement_date", "effective_date", "source_url"]:
        if col not in fns:
            fns = list(fns) + [col]

    for r in rows:
        r.setdefault("announcement_date", "")
        r.setdefault("effective_date", "")
        r.setdefault("source_url", "")

    write_csv(DATA_DIR / "order_limits_general_rules.csv", rows, fns)
    print("    (添加了 announcement_date / effective_date / source_url 列)")


def process_product_rules():
    """品种规则: 展开同上 + 加三列"""
    rows, fns = read_csv(DATA_DIR / "order_limits_product_rules.csv")

    expand_same_as(rows, "source")
    expand_same_as(rows, "evidence")

    for col in ["announcement_date", "effective_date", "source_url"]:
        if col not in fns:
            fns = list(fns) + [col]

    for r in rows:
        r.setdefault("announcement_date", "")
        r.setdefault("effective_date", "")
        r.setdefault("source_url", "")

    write_csv(DATA_DIR / "order_limits_product_rules.csv", rows, fns)
    print("    (展开了 '同上' + 添加三列)")


def process_summary():
    """总结文件: 展开同上 + 加三列"""
    rows, fns = read_csv(DATA_DIR / "order_limits_summary.csv")

    expand_same_as(rows, "source")
    expand_same_as(rows, "evidence")

    for col in ["announcement_date", "effective_date", "source_url"]:
        if col not in fns:
            fns = list(fns) + [col]

    for r in rows:
        r.setdefault("announcement_date", "")
        r.setdefault("effective_date", "")
        r.setdefault("source_url", "")

    write_csv(DATA_DIR / "order_limits_summary.csv", rows, fns)
    print("    (展开了 '同上' + 添加三列)")


def main():
    print("=== 公告CSV改造 ===\n")

    print("[1/4] adjustments.csv")
    process_adjustments()

    print("\n[2/4] general_rules.csv")
    process_general_rules()

    print("\n[3/4] product_rules.csv")
    process_product_rules()

    print("\n[4/4] summary.csv")
    process_summary()

    print("\n完成。")


if __name__ == "__main__":
    main()
