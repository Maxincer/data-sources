#!/usr/bin/env python3
"""
从 announcements_metadata.json 中读取公告，逐个解析 minoq/maxoq，
结果写入 data/fields_from_announcements.csv。

用法:
  python scripts/update_fields_from_announcements.py
  python scripts/update_fields_from_announcements.py --dry-run

CSV 格式:
  announcement_id, publish_date, exchange, product_code,
  security_id, field, value, effective_date, announcement_title
"""

import csv
import json
import os
import sys
import asyncio
from asyncio import Semaphore
from datetime import datetime, timezone, timedelta
from pathlib import Path

from mxz_utils.logging_config import get_logger

DATA_DIR = Path(os.environ.get("ANNOUNCEMENTS_DIR",
                 "data/raw/announcements"))
META_FILE = DATA_DIR / "announcements_metadata.json"
OUTPUT_FILE = Path(os.environ.get("OUTPUT_DIR", "data")) / \
    "fields_from_announcements.csv"
DAILY_START = os.environ.get("DAILY_START_DATE", "20260101")

LOG_DIR = Path(os.environ.get("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger = get_logger(
    name="FieldsFromAnnouncements",
    level="DEBUG",
    dirpath_logs=str(LOG_DIR),
    logfile_basename="FieldsFromAnnouncements",
)

HEADER = [
    "announcement_id", "publish_date", "exchange",
    "product_code", "security_id", "field", "value",
    "effective_date", "announcement_title",
]

# 附件下载目录
ATTACHMENT_DIR = DATA_DIR / "attachments"


def _now_str() -> str:
    return datetime.now(timezone(timedelta(hours=8))).strftime(
        "%Y%m%dT%H:%M:%S+08:00"
    )


def load_existing() -> set[str]:
    """加载已有 CSV，返回已解析的 announcement_id 集合。"""
    if not OUTPUT_FILE.exists():
        logger.info("CSV 不存在，从头开始")
        return set()

    seen = set()
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != HEADER:
            logger.warning("CSV 表头不匹配，重建文件")
            return set()
        for row in reader:
            aid = row.get("announcement_id", "").strip()
            if aid:
                seen.add(aid)
    logger.info("已有 CSV: %s 条，已解析 ID: %s 个", len(seen) if seen else 0, len(seen))
    return seen


def should_parse(entry: dict) -> bool:
    """判断公告是否需要解析。"""
    cat = entry.get("category", "")
    pub_date = entry.get("pub_date", "")

    if cat in ("general", "product"):
        return True
    if cat == "daily" and pub_date >= DAILY_START:
        return True
    return False


def main():
    from data_sources.parser import parse_announcement_fields

    # 1. 加载已有 CSV
    seen_ids = load_existing()

    # 2. 加载 metadata
    if not META_FILE.exists():
        logger.error("metadata 不存在: %s", META_FILE)
        sys.exit(1)

    with open(META_FILE, "r", encoding="utf-8") as f:
        meta = json.load(f)

    logger.info("metadata 总数: %s 条", len(meta))

    # 3. 筛选待解析
    candidates = []
    for aid, entry in meta.items():
        if aid in seen_ids:
            continue
        if not should_parse(entry):
            continue
        sf = entry.get("source_file", "")
        if not sf or not Path(sf).exists():
            logger.debug("跳过 %s: 源文件不存在 %s", aid, sf)
            continue
        candidates.append((aid, entry))

    candidates.sort(key=lambda x: (x[1].get("pub_date", ""), x[0]))
    logger.info("待解析: %s 条", len(candidates))

    if not candidates:
        logger.info("无新公告，退出")
        return

    # 4. 异步并发解析
    dry_run = "--dry-run" in sys.argv
    semaphore = int(os.environ.get("LLM_CONCURRENCY", "10"))
    sem = Semaphore(semaphore)
    logger.info("并发度: %s", semaphore)

    new_rows: list[dict] = []
    parsed = [0]
    skipped = [0]
    lock = asyncio.Lock()

    async def process_one(aid, entry):
        sf = entry.get("source_file", "")
        loop = asyncio.get_running_loop()

        async with sem:
            try:
                html = await loop.run_in_executor(
                    None, lambda: Path(sf).read_text(encoding="utf-8", errors="replace"))
                items = await loop.run_in_executor(
                    None,
                    lambda: parse_announcement_fields(
                        html=html,
                        exchange=entry.get("exchange", ""),
                        title=entry.get("title", ""),
                        publish_date=entry.get("pub_date", ""),
                        page_url=entry.get("url", ""),
                        attachment_dir=ATTACHMENT_DIR,
                    )
                )

                async with lock:
                    if not items:
                        skipped[0] += 1
                        logger.debug("  → 无 minoq/maxoq 信息，跳过 [%s] %s",
                                   entry.get("exchange"), aid)
                        return
                    for item in items:
                        new_rows.append({
                            "announcement_id": aid,
                            "publish_date": entry.get("pub_date", ""),
                            "exchange": entry.get("exchange", ""),
                            "product_code": item["product_code"],
                            "security_id": item["security_id"],
                            "field": item["field"],
                            "value": str(item["value"]),
                            "effective_date": item["effective_date"],
                            "announcement_title": entry.get("title", ""),
                        })
                        parsed[0] += 1
                    logger.info("  + [%s] %s: %s 条",
                               entry.get("exchange"), aid, len(items))
            except Exception as e:
                logger.warning("解析失败 [%s] %s: %s", entry.get("exchange"), aid, e)

    async def run_all():
        tasks = [process_one(aid, entry) for aid, entry in candidates]
        await asyncio.gather(*tasks)

    asyncio.run(run_all())

    logger.info("解析完成: +%s 条 (跳过 %s 条无信息)", parsed[0], skipped[0])

    logger.info("解析完成: +%s 条 (跳过 %s 条无信息)", parsed, skipped)

    if dry_run:
        logger.info("DRY RUN — 不写入文件")
        return

    if not new_rows:
        logger.info("无新增字段，退出")
        return

    # 5. 写入 CSV
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    write_mode = "a" if OUTPUT_FILE.exists() else "w"
    with open(OUTPUT_FILE, mode=write_mode, newline="",
              encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        if write_mode == "w":
            writer.writeheader()
        writer.writerows(new_rows)

    logger.info("写入完成: +%s 条 → %s", len(new_rows), OUTPUT_FILE)


if __name__ == "__main__":
    main()
