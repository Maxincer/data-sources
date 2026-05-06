#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
历史数据回填脚本 v2。

用法:
  python3 -m data_sources.backfill 20260420 20260429
  python3 -m data_sources.backfill --parallel 3 20260301 20260429
  python3 -m data_sources.backfill --today
"""

import argparse
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

DCE_ENV = {
    "DCE_API_KEY": "ofxc69rpmd59",
    "DCE_API_SECRET": "2UdFW^2G4!4^7#@URqWx",
}


def date_range(start: str, end: str):
    d = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")
    while d <= end_dt:
        yield d.strftime("%Y%m%d")
        d += timedelta(days=1)


def run_date(trade_date: str) -> tuple[str, str]:
    """Run fetch+write for a date, return (date, status)."""
    env = {**os.environ, **DCE_ENV}
    try:
        fetch = subprocess.run(
            [sys.executable, "-m", "data_sources.fetcher", "run", trade_date],
            capture_output=True, text=True, timeout=180, env=env,
        )
        if fetch.returncode != 0:
            return (trade_date, f"FETCH_ERR({fetch.returncode})")

        write = subprocess.run(
            [sys.executable, "-m", "data_sources.writer", "--date", trade_date],
            capture_output=True, text=True, timeout=180, env=env,
        )

        for line in write.stdout.split("\n"):
            line = line.strip()
            if line.startswith("Records written"):
                written = int(line.split(":")[-1].strip())
                if written > 0:
                    return (trade_date, f"OK({written})")
                return (trade_date, f"EMPTY({written})")

        if write.returncode != 0:
            return (trade_date, f"WRITE_ERR({write.returncode})")
        return (trade_date, "NO_OUTPUT")
    except subprocess.TimeoutExpired:
        return (trade_date, "TIMEOUT")
    except Exception as e:
        return (trade_date, f"EXCEPTION({e})")


def main():
    parser = argparse.ArgumentParser(description="回填历史数据")
    parser.add_argument("start", nargs="?", help="起始日期 YYYYMMDD")
    parser.add_argument("end", nargs="?", help="结束日期 YYYYMMDD")
    parser.add_argument("--parallel", type=int, default=1)
    parser.add_argument("--today", action="store_true")
    args = parser.parse_args()

    if args.today:
        today = datetime.now().strftime("%Y%m%d")
        dates = [today]
    elif args.start and args.end:
        dates = list(date_range(args.start, args.end))
    else:
        today = datetime.now()
        dates = list(date_range(
            (today - timedelta(days=7)).strftime("%Y%m%d"),
            today.strftime("%Y%m%d")
        ))

    print(f"回填: {dates[0]} ~ {dates[-1]} ({len(dates)} 天)")
    print(f"并行: {args.parallel}")

    ok, empty, failed = 0, 0, 0
    results = []

    if args.parallel > 1:
        with ThreadPoolExecutor(max_workers=args.parallel) as pool:
            fut_map = {pool.submit(run_date, d): d for d in dates}
            for fut in as_completed(fut_map):
                d, status = fut.result()
                results.append((d, status))
    else:
        for d in dates:
            results.append(run_date(d))

    for d, status in sorted(results, key=lambda x: x[0]):
        if status.startswith("OK"):
            print(f"  {d}: ✅ {status}")
            ok += 1
        elif status.startswith("EMPTY"):
            print(f"  {d}: ⬜ {status}")
            empty += 1
        else:
            print(f"  {d}: ❌ {status}")
            failed += 1

    print(f"\n完成: OK={ok}, EMPTY={empty}, FAIL={failed}")


if __name__ == "__main__":
    main()
