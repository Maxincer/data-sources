#!/usr/bin/env python3
"""
从 announcements_metadata.json 中读取公告，逐个解析 minoq/maxoq，
结果写入 data/fields_from_announcements.csv。

用法:
  python -m data_sources.services.analyse_announcements_service
  python -m data_sources.services.analyse_announcements_service --dry-run
"""

import asyncio
import csv
import json
import os
import sys
from asyncio import Semaphore
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import aiohttp
import socket
from mxz_utils.logging_config import get_logger

from data_sources.parser import (
    _process_html,
    parse_announcement_fields,
    clean_attachment_orphans,
    reset_attachment_stats,
    get_attachment_stats,
    _download,
)

DATA_DIR = Path(os.environ["DATA_DIR"])
META_FILE = DATA_DIR / "raw" / "announcements" / "announcements_metadata.json"
OUTPUT_FILE = DATA_DIR / "fields_from_announcements.csv"
DAILY_START = os.environ["DAILY_START_DATE"]
LOG_DIR = Path(os.environ["LOG_DIR"])
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger = get_logger(
    name="AnalyseAnnouncementsService",
    level="DEBUG",
    dirpath_logs=str(LOG_DIR),
    logfile_basename="AnalyseAnnouncementsService",
)

HEADER = [
    "announcement_id", "publish_date", "exchange",
    "product_code", "security_id", "field", "value",
    "effective_date", "announcement_title", "evidence", "page_url",
    "needs_review",
]


def load_existing() -> set[str]:
    """从 CSV 加载已处理的 announcement_id 集合。"""
    seen = set()
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is not None:
                for row in reader:
                    aid = row.get("announcement_id", "").strip()
                    if aid:
                        seen.add(aid)
        row_count = sum(1 for _ in open(OUTPUT_FILE, encoding="utf-8")) - 1
        logger.info("已有 CSV: %s 条", row_count)
    else:
        logger.info("CSV 不存在，从头开始")
    return seen


def should_parse(entry: dict) -> bool:
    cat = entry.get("category", "")
    pub_date = entry.get("pub_date", "")
    if cat in ("general", "product"):
        return True
    if cat == "daily" and pub_date >= DAILY_START:
        return True
    return False


def main():

    if not META_FILE.exists():
        logger.error("metadata 不存在: %s", META_FILE)
        sys.exit(1)

    with open(META_FILE, "r", encoding="utf-8") as f:
        meta = json.load(f)

    clean_attachment_orphans(meta)
    reset_attachment_stats()

    seen_ids = load_existing()

    logger.info("metadata 总数: %s 条", len(meta))

    candidates = []
    for aid, entry in meta.items():
        if "_attachment_" in aid:
            continue
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

    dry_run = "--dry-run" in sys.argv
    semaphore = int(os.environ["LLM_CONCURRENCY"])
    sem = Semaphore(semaphore)
    logger.info("并发度: %s", semaphore)

    new_rows: list[dict] = []
    parsed = [0]
    skipped = [0]
    failed: list[str] = []
    lock = asyncio.Lock()

    async def process_one(aid, entry, session, retry_executor):
        sf = entry.get("source_file", "")
        read = lambda: Path(sf).read_text(encoding="utf-8", errors="replace")

        try:
            async with sem:
                loop = asyncio.get_running_loop()
                html = await loop.run_in_executor(retry_executor, read)
                text, links = await loop.run_in_executor(
                    retry_executor,
                    lambda: _process_html(html, entry.get("url", "")),
                )
                items, needs_review = await parse_announcement_fields(
                    text=text,
                    links=links,
                    exchange=entry["exchange"],
                    title=entry["title"],
                    publish_date=entry["pub_date"],
                    category=entry["category"],
                    attachment_dir=Path(sf).parent,
                    session=session,
                    aid=aid,
                )
                review_flag = 1 if needs_review else 0

                async with lock:
                    if not items:
                        skipped[0] += 1
                        new_rows.append({
                            "announcement_id": aid,
                            "publish_date": entry.get("pub_date", ""),
                            "exchange": entry["exchange"],
                            "product_code": "",
                            "security_id": "",
                            "field": "",
                            "value": "",
                            "effective_date": "",
                            "announcement_title": entry["title"],
                            "evidence": "",
                            "page_url": entry["url"],
                            "needs_review": review_flag,
                        })
                        logger.debug(
                            "  → 无 minoq/maxoq 信息 [%s] %s",
                            entry["exchange"], aid
                        )
                        return
                    for item in items:
                        new_rows.append({
                            "announcement_id": aid,
                            "publish_date": entry.get("pub_date", ""),
                            "exchange": entry["exchange"],
                            "product_code": item["product_code"],
                            "security_id": item["security_id"],
                            "field": item["field"],
                            "value": str(item["value"]),
                            "effective_date": item["effective_date"],
                            "announcement_title": entry["title"],
                            "evidence": item["evidence"],
                            "page_url": entry["url"],
                            "needs_review": review_flag,
                        })
                        parsed[0] += 1
                    logger.info(
                        "  + [%s] %s: %s 条",
                        entry["exchange"], aid, len(items),
                    )
        except BaseException as e:
            logger.warning(
                "解析失败 [%s] %s: %s", entry["exchange"], aid, e,
                exc_info=True,
            )
            failed.append(aid)

    async def _main():
        retry_executor = ThreadPoolExecutor(max_workers=semaphore * 2)
        conn = aiohttp.TCPConnector(
            limit=semaphore * 2, limit_per_host=semaphore,
            enable_cleanup_closed=True,
            family=socket.AF_INET,
        )
        async with aiohttp.ClientSession(connector=conn, trust_env=False) as session:
            # 首轮解析
            tasks = [
                process_one(aid, entry, session, retry_executor)
                for aid, entry in candidates
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("首轮完成: +%s 条, 跳过 %s 条, 失败 %s 条", parsed[0], skipped[0], len(failed))

            # 指数退避重试 (最多3轮: 1s, 2s, 4s)
            for retry_i in range(3):
                if not failed:
                    break
                delay = 2 ** retry_i
                logger.warning("=" * 50)
                logger.warning(
                    "第 %s 轮重试: %s 条, 等待 %ss...", retry_i + 1, len(failed), delay
                )
                logger.warning("=" * 50)
                await asyncio.sleep(delay)

                retry_ids = set(failed)
                failed.clear()
                retry_candidates = [
                    (aid, entry) for aid, entry in candidates if aid in retry_ids
                ]
                tasks = [process_one(aid, entry, session, retry_executor)
                         for aid, entry in retry_candidates]
                await asyncio.gather(*tasks, return_exceptions=True)

                logger.info("第 %s 轮重试完成: 仍失败 %s 条", retry_i + 1, len(failed))
        retry_executor.shutdown(wait=False)

    asyncio.run(_main())

    logger.info(
        "解析完成: +%s 条 (跳过 %s 条无信息)", parsed[0], skipped[0],
    )

    # 解析失败汇总
    if failed:
        logger.warning("=" * 50)
        logger.warning("解析失败汇总: %s 条", len(failed))
        for fid in failed:
            logger.warning("  FAIL %s", fid)
        logger.warning("=" * 50)
    else:
        logger.info("解析: 全部成功")

    stats = get_attachment_stats()
    if stats["fail"]:
        logger.warning(
            "附件下载: %s 成功, %s 失败", stats["success"], stats["fail"],
        )
    else:
        logger.info(
            "附件下载: 全部成功 (%s 个附件)", stats["success"],
        )

    if dry_run:
        logger.info("DRY RUN — 不写入文件")
        return

    if not new_rows:
        logger.info("无新增，退出")
        return

    # 读取已有数据，合并后全量排序写入
    existing_rows = []
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_rows.append(row)

    all_rows = existing_rows + new_rows
    all_rows.sort(key=lambda r: (
        r.get("publish_date", ""),
        r.get("exchange", ""),
        r.get("announcement_id", ""),
    ))

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, mode="w", newline="",
              encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER, quoting=csv.QUOTE_NONE, escapechar='\\')
        writer.writeheader()
        writer.writerows(all_rows)

    logger.info(
        "写入完成: +%s 条, 总计 %s 条 → %s",
        len(new_rows), len(all_rows), OUTPUT_FILE,
    )


if __name__ == "__main__":
    main()
