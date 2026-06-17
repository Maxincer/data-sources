#!/usr/bin/env python3
"""
order_limits_updater.py — 根据公告分析结果自动更新 order_limits.csv

在 announcement_watcher 的 LLM 分析阶段后调用：
  检测到 minoq/maxoq 变更 → 解析结构化数据 → 更新 CSV → 输出标准格式变更记录

用法:
  from order_limits_updater import process_minoq_maxoq_change, apply_csv_update
"""

import csv
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
ORDER_LIMITS_PATH = DATA_DIR / "order_limits.csv"
ORDER_LIMITS_BAK = DATA_DIR / "order_limits.csv.bak"

# 交易所名 → CSV 内 exchange 列值的映射
EXCHANGE_MAP = {
    "SHFE": "SHFE", "INE": "INE", "DCE": "DCE",
    "CZCE": "CZCE", "CFFEX": "CFFEX", "GFEX": "GFEX",
}

# 字段映射（LLM 分析中的字段名 → CSV 列名）
FIELD_MAP = {
    "minoq": "minoq",
    "maxoq": "maxoq_limit",
    "最大下单量": "maxoq_limit",
    "最小开仓量": "minoq",
    "最小开仓手数": "minoq",
    "最大开仓手数": "maxoq_limit",
    "限价单最大下单量": "maxoq_limit",
    "市价单最大下单量": "maxoq_market",
    "最大下单量(限价)": "maxoq_limit",
    "最大下单量(市价)": "maxoq_market",
}


def to_int(val) -> Optional[int]:
    """安全转换为 int。"""
    if val is None:
        return None
    if isinstance(val, int):
        return val
    s = str(val).strip().replace("手", "").replace(" ", "")
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def extract_change_data(llm_line: str) -> List[Dict]:
    """从 LLM 分析行中提取 minoq/maxoq 变更的结构化数据。

    输入示例：
        [1] CZCE MA2604/2605 甲醇合约调整 |
        影响: 手动 | 字段: [minoq] | 品种: [MA] |
        变更: [旧: 1手, 新: 8手] | 生效日期: [2026-05-20] |
        说明: CZCE调整MA2604/2605合约最小开仓量

    返回：
        [
            {
                "exchange": "CZCE",
                "variety": "MA",
                "field": "minoq",
                "old_value": 1,
                "new_value": 8,
                "effective_date": "2026-05-20",
                "contract_filter": "MA2604,MA2605",
                "summary": "最小开仓量调整",
            }
        ]
    """
    results = []

    # 解析标准字段
    def _extract(pattern: str, text: str, group: int = 1) -> str:
        m = re.search(pattern, text)
        return m.group(group).strip() if m else ""

    exchange = _extract(r"\]\s*([A-Z]{2,5})", llm_line)
    impact = _extract(r"影响:\s*(手动|自动|无)", llm_line)
    fields_raw = _extract(r"字段:\s*\[?([^\]]*?)\]?(?=\s*\||\s*$)", llm_line)
    variety_raw = _extract(r"品种:\s*\[?([^\]]*?)\]?(?=\s*\||\s*$)", llm_line)
    change_raw = _extract(r"变更:\s*\[?([^\]]*?)\]?(?=\s*\||\s*$)", llm_line)
    date_raw = _extract(r"生效日期:\s*\[?([^\]]*?)\]?(?=\s*\||\s*$)", llm_line)

    # 只处理需要手动处理的 minoq/maxoq 变更
    if impact != "手动" or ("minoq" not in llm_line and "maxoq" not in llm_line and "下单" not in llm_line):
        return results

    # 确定交易所
    exchange_key = EXCHANGE_MAP.get(exchange.upper(), exchange.upper())
    if exchange_key not in EXCHANGE_MAP.values():
        return results

    # 确定品种（取第一个品种代码）
    varieties = [v.strip().upper() for v in re.split(r"[,/，/、\s]+", variety_raw) if v.strip()]

    # 确定受影响的字段
    fields = [f.strip() for f in re.split(r"[,/，/、\s]+", fields_raw) if f.strip()]
    mapped_fields = []
    for f in fields:
        f_clean = f.strip("[]（）()")
        mapped = FIELD_MAP.get(f_clean, f_clean)
        if mapped not in mapped_fields:
            mapped_fields.append(mapped)

    if not mapped_fields:
        # 从 change_raw 推断
        if "min" in change_raw.lower() or "开仓" in change_raw or "最小" in change_raw:
            mapped_fields.append("minoq")
        if "max" in change_raw.lower() or "下单" in change_raw or "最大" in change_raw:
            mapped_fields.append("maxoq_limit")

    # 解析变更值
    old_val = new_val = None
    # 尝试多种格式: "旧: 1, 新: 8", "原1手调整为8手", "由1手改为8手", "1→8"
    for p in [
        r"旧(?:值|):?\s*(\d+)",
        r"原(?:为|):?\s*(\d+)",
        r"由(\d+)",
    ]:
        m = re.search(p, change_raw)
        if m:
            old_val = to_int(m.group(1))
            break

    for p in [
        r"新(?:值|):?\s*(\d+)",
        r"调整(?:为|后)\s*(\d+)",
        r"改为\s*(\d+)",
        r"→\s*(\d+)",
    ]:
        m = re.search(p, change_raw)
        if m:
            new_val = to_int(m.group(1))
            break

    # 直接数字 → 默认为新值
    if old_val is None and new_val is None:
        nums = re.findall(r"(\d+)", change_raw)
        if len(nums) >= 2:
            old_val, new_val = to_int(nums[-2]), to_int(nums[-1])
        elif len(nums) == 1:
            new_val = to_int(nums[0])

    # 确定合约范围
    contract_filter = ""
    date_effective = date_raw

    # 为每个品种 × 每个字段生成一条记录
    for variety in varieties:
        # 清理 variety
        variety = variety.split(".")[0].upper()
        variety = "".join(c for c in variety if c.isalpha())
        if not variety or variety in {"ALL"}:
            continue
        for field in mapped_fields:
            if field not in ("minoq", "maxoq_limit", "maxoq_market"):
                continue
            results.append({
                "exchange": exchange_key,
                "variety": variety,
                "field": field,
                "old_value": old_val,
                "new_value": new_val,
                "effective_date": date_effective,
                "contract_filter": contract_filter,
                "summary": llm_line.strip()[:120],
            })

    return results


def load_csv_rows():  # -> Tuple[List[str], int, List[str], List[Dict]]
    """加载 order_limits.csv，保持注释位置。

    Returns:
        all_lines: 所有原始行
        header_line_idx: 表头行索引
        fieldnames: 列名
        data: 解析后的数据行列表
    """
    with open(ORDER_LIMITS_PATH, encoding="utf-8") as f:
        all_lines = f.readlines()
    header_line_idx = next(
        (i for i, l in enumerate(all_lines) if l.strip().startswith("exchange,")),
        None,
    )
    if header_line_idx is None:
        raise ValueError("未找到 CSV 表头行 (exchange,...)")
    fieldnames = [f.strip() for f in all_lines[header_line_idx].strip().split(",")]

    # 跳过注释行后解析数据
    data_lines = [
        l for l in all_lines[header_line_idx + 1:]
        if l.strip() and not l.startswith("#")
    ]
    reader = csv.DictReader(data_lines, fieldnames=fieldnames)
    data = list(reader)
    return all_lines, header_line_idx, fieldnames, data


def save_csv_rows(all_lines: List[str], header_line_idx: int,
                  updated_data: List[Dict], fieldnames: List[str]):
    """保存回 order_limits.csv，保留注释位置。

    遍历原始行，遇到注释直接写入，遇到数据从 updated_data 按序取出写入。
    """
    backup_path = ORDER_LIMITS_PATH.with_suffix(".csv.bak")
    shutil.copy2(ORDER_LIMITS_PATH, backup_path)

    data_iter = iter(updated_data)
    with open(ORDER_LIMITS_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        # 写表头前的注释
        for i in range(header_line_idx):
            f.write(all_lines[i])
        # 写表头
        writer.writeheader()
        # 写数据行（注释直接保留，数据从 updated_data 取）
        for line in all_lines[header_line_idx + 1:]:
            if line.strip() and not line.startswith("#"):
                try:
                    row = next(data_iter)
                    writer.writerow(row)
                except StopIteration:
                    break
            else:
                f.write(line)

    print(f"  ✓ order_limits.csv 已更新 (备份: {backup_path.name})")


def apply_csv_update(changes: List[Dict]) -> List[str]:
    """将变更应用到 order_limits.csv。

    返回标准格式的变更记录列表。
    """
    if not changes:
        return []

    all_lines, header_line_idx, fieldnames, data = load_csv_rows()
    output_lines = []

    for change in changes:
        ex = change["exchange"]
        vid = change["variety"]
        field = change["field"]
        new_val = change["new_value"]
        old_val = change["old_value"]
        date_str = change.get("effective_date", datetime.now().strftime("%Y%m%d"))

        csv_col = field  # "minoq" → "minoq", "maxoq_limit" → "maxoq_limit"

        # 在 CSV 中查找匹配行
        matched = False
        for row in data:
            row_ex = row.get("exchange", "").strip()
            row_vid = row.get("variety_id", "").strip()
            if row_ex.upper() == ex.upper() and row_vid.upper() == vid.upper():
                old_str = row.get(csv_col, "")
                old_csv_val = to_int(old_str)

                # 更新值
                row[csv_col] = str(new_val)

                # 更新 notes
                notes = row.get("notes", "")
                update_note = f"{datetime.now().strftime('%Y%m%d')}公告: {change.get('summary', '')[:60]}"
                if notes:
                    if update_note not in notes:
                        row["notes"] = f"{notes}; {update_note}"
                else:
                    row["notes"] = update_note

                # 输出标准格式
                field_display = {"minoq": "最小开仓量", "maxoq_limit": "最大下单量", "maxoq_market": "最大下单量(市价)"}.get(field, field)
                old_display = old_csv_val if old_csv_val is not None else (old_val or "?")
                output_lines.append(
                    f"- 调整合约：{vid}<{date_str[:4]}>，"
                    f"调整项目：{field_display}，"
                    f"调整前：{old_display}手，"
                    f"调整后：{new_val}手，"
                    f"生效时间：{date_str.replace('-', '')}"
                )
                matched = True
                break

        if not matched:
            field_display = {"minoq": "最小开仓量", "maxoq_limit": "最大下单量"}.get(field, field)
            output_lines.append(
                f"- 调整合约：{vid}<{date_str[:4]}>，"
                f"调整项目：{field_display}，"
                f"调整前：{old_val or '?'}手，"
                f"调整后：{new_val}手，"
                f"生效时间：{date_str.replace('-', '')} "
                f"(⚠ 未在 order_limits.csv 中找到 {ex}/{vid}/{field} 条目，需手动添加)"
            )

    if output_lines:
        save_csv_rows(all_lines, header_line_idx, data, fieldnames)

    return output_lines


def process_minoq_maxoq_change(announcements: List[Dict]) -> List[str]:
    """主入口：从 announcement_watcher 的公告列表检测 minoq/maxoq 变更并自动更新 CSV。

    Args:
        announcements: announcement_watcher 输出的公告列表（含 LLM 分析结果 _llm_line）

    Returns:
        标准格式的变更记录列表（每条形如 "调整合约：MA<2604>，调整项目：最小开仓量，..."）
    """
    all_changes = []

    for ann in announcements:
        llm_line = ann.get("_llm_line", "")
        if not llm_line:
            continue
        changes = extract_change_data(llm_line)
        all_changes.extend(changes)

    if not all_changes:
        return []

    print(f"\n📋 检测到 {len(all_changes)} 条 minoq/maxoq 变更，更新 order_limits.csv...")
    output_lines = apply_csv_update(all_changes)

    return output_lines


# ═══════════════════════════════════════════════════════════════════
#  DCE tradingParam → order_limits.csv 导入
# ═══════════════════════════════════════════════════════════════════


def import_dce_tradingparam_to_csv(raw_dir: str = None) -> List[str]:
    """从 DCE TradingParam 数据更新 order_limits.csv 中的 maxoq。

    从 raw 目录中找到最新的 DCE.TradingParam.*.json 文件，
    解析其中的 maxHand（最大下单手数），更新到 order_limits.csv。

    Returns:
        标准格式的变更记录列表
    """
    if raw_dir is None:
        raw_dir = os.environ.get("DATA_DIR", str(PROJECT_ROOT / "data")) + "/raw"

    import glob
    # 找最新的 DCE.TradingParam 文件
    files = sorted(glob.glob(os.path.join(raw_dir, "*.DCE.TradingParam.*.json")))
    if not files:
        print("  ℹ️ 未找到 DCE.TradingParam 数据文件")
        return []

    latest = files[-1]
    print(f"  📂 读取: {os.path.basename(latest)}")

    with open(latest, encoding="utf-8") as f:
        data = json.load(f)

    items = data.get("data", [])
    if not items:
        print("  ⚠ DCE.TradingParam 数据为空")
        return []

    # 提取日期
    date_str = datetime.now().strftime("%Y%m%d")
    changes = []
    for item in items:
        vid = item.get("varietyId", "")
        if not vid or not re.match(r"^[a-z]+", vid):
            continue
        max_hand_str = item.get("maxHand")
        if max_hand_str is None:
            continue
        try:
            max_hand = int(float(max_hand_str))
        except (ValueError, TypeError):
            continue

        changes.append({
            "exchange": "DCE",
            "variety": vid.upper(),
            "field": "maxoq_limit",
            "new_value": max_hand,
            "old_value": None,
            "effective_date": date_str,
            "summary": f"DCE tradingParam maxHand={max_hand}",
        })

    if not changes:
        return []

    print(f"  📋 解析到 {len(changes)} 个 DCE 品种的 maxHand 数据")
    output_lines = apply_csv_update(changes)
    return output_lines


# ═══════════════════════════════════════════════════════════════════
#  独立命令行入口 — 用于手动测试或事后补录
# ═══════════════════════════════════════════════════════════════════

def main():
    """手动测试：直接修改 CSV 中的某品种值。"""
    import argparse

    parser = argparse.ArgumentParser(description="手动更新 order_limits.csv")
    parser.add_argument("--exchange", required=True, help="交易所 (SHFE/INE/DCE/CZCE/CFFEX/GFEX)")
    parser.add_argument("--variety", required=True, help="品种代码 (如 MA, lc, sc)")
    parser.add_argument("--field", required=True, choices=["minoq", "maxoq_limit", "maxoq_market"], help="字段")
    parser.add_argument("--value", type=int, required=True, help="新值")
    parser.add_argument("--old", type=int, help="旧值（可选，用于输出格式）")
    parser.add_argument("--date", default=datetime.now().strftime("%Y%m%d"), help="生效日期 YYYYMMDD")
    parser.add_argument("--reason", default="", help="变更原因")

    args = parser.parse_args()

    change = {
        "exchange": args.exchange.upper(),
        "variety": args.variety.upper(),
        "field": args.field,
        "new_value": args.value,
        "old_value": args.old,
        "effective_date": args.date,
        "summary": args.reason or f"手动调整 {args.exchange}/{args.variety} {args.field}→{args.value}",
    }

    lines = apply_csv_update([change])
    for line in lines:
        print(line)


if __name__ == "__main__":
    main()
