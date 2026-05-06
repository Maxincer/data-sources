#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Data quality verification for downloaded settlement files.

Usage:
    python3 -m data_sources.verifier               # full comparison, all dates
    python3 -m data_sources.verifier 20260429       # comparison for specific date
"""

import json
import sys
from typing import Dict, List, Optional

import pymysql


DB_CONFIG = {
    "host": "192.168.1.202",
    "user": "root",
    "password": "root0808",
    "database": "future_cn",
    "charset": "utf8mb4",
}

ORIGINAL_TABLE = "t_futures_info"
NEW_TABLE = "t_futures_info_exchange"


class CompareResult:
    """Result of field-by-field comparison between two tables."""

    def __init__(self, field: str):
        self.field = field
        self.total_matched = 0
        self.total_diff = 0
        self.total_missing_original = 0
        self.total_missing_new = 0
        self.total_abnormal_missing_new = 0
        self.max_deviation = 0.0
        self.sample_diffs: List[Dict] = []

    @property
    def summary(self) -> Dict:
        # 按偏差降序排列
        sorted_samples = sorted(self.sample_diffs,
                                key=lambda x: x.get("deviation_pct", 0),
                                reverse=True)
        return {
            "field": self.field,
            "matched": self.total_matched,
            "diff": self.total_diff,
            "missing_in_original": self.total_missing_original,
            "missing_in_new": self.total_missing_new,
            "abnormal_missing_new": self.total_abnormal_missing_new,
            "max_deviation_pct": round(self.max_deviation, 4),
            "sample_diffs": sorted_samples[:10],
        }


class Verifier:
    """
    Data quality checks for downloaded settlement files.
    """

    CFFEX_ERROR_SIGNATURE = b"\xcd\xf8\xd2\xb3\xb4\xed\xce\xf3"

    def __init__(self, logger, metadata_file=None):
        self.logger = logger
        self.metadata_file = metadata_file

    def check_min_size(
        self,
        content: bytes,
        min_size: int = 100,
    ) -> tuple[bool, str]:
        """Return (passed, reason) for minimum size gate."""
        size = len(content)
        if size == 0:
            return False, "Empty file (0 bytes)"
        if size < min_size:
            return False, f"File size {size} < minimum {min_size}"
        return True, "OK"

    def check_html_error_page(self, content: bytes) -> tuple[bool, str]:
        """
        Detect HTML error pages that are returned with HTTP 200.
        Currently only known to affect CFFEX.
        """
        preview = content[:500].lower()
        if b"<html" in preview or b"<!doctype html" in preview:
            if self.CFFEX_ERROR_SIGNATURE in content:
                return False, "HTML error page (CFFEX 404)"
            return False, "HTML response instead of expected data"
        return True, "OK"

    def check_size_deviation(
        self,
        current_size: int,
        previous_size: Optional[int],
        threshold_percent: float = 50.0,
    ) -> tuple[bool, str]:
        """
        Check if current size deviates too much from previous.
        Returns (passed, reason).
        """
        if previous_size is None or previous_size == 0:
            return True, "No historical size to compare"
        deviation = abs(current_size - previous_size) / previous_size * 100
        if deviation > threshold_percent:
            return (
                False,
                f"Size deviation {deviation:.1f}% > {threshold_percent}%"
                f" (previous: {previous_size}, current: {current_size})",
            )
        return True, "OK"

    def get_previous_size(self, task) -> Optional[int]:
        """
        Find the file size of the most recent previous fetch
        for the same data product (exchange + description + suffix).
        """
        if not self.metadata_file or not self.metadata_file.exists():
            return None
        match_suffix = (
            f".{task.exchange}.{task.description}.{task.suffix}"
        )
        previous_size = None
        latest_prev_date = ""
        try:
            with open(self.metadata_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    local_fn = record.get("local_filename", "")
                    if not local_fn.endswith(match_suffix):
                        continue
                    date_part = local_fn.split(".")[0]
                    if date_part >= task.trade_date:
                        continue
                    if date_part > latest_prev_date:
                        latest_prev_date = date_part
                        previous_size = record.get("file_size_bytes")
        except Exception as e:
            self.logger.warning(
                "Error reading metadata for previous size: %s", e
            )
        return previous_size

    def verify_response(
        self,
        content: bytes,
        previous_size: Optional[int] = None,
    ) -> tuple[bool, str]:
        """
        Run all fetcher-side checks.
        Returns (passed, combined_reason).
        """
        passed, reason = self.check_html_error_page(content)
        if not passed:
            return False, reason
        passed, reason = self.check_min_size(content)
        if not passed:
            return False, reason
        if previous_size is not None:
            passed, reason = self.check_size_deviation(
                len(content), previous_size
            )
            if not passed:
                return False, reason
        return True, "OK"

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    def _get_conn(self):
        return pymysql.connect(
            **DB_CONFIG, cursorclass=pymysql.cursors.DictCursor
        )

    @staticmethod
    def _format_date(date_val) -> str:
        """Format date value to YYYYMMDD string."""
        if date_val is None:
            return ""
        s = str(date_val)
        return s.replace("-", "")[:8]

    # -----------------------------------------------------------------
    # Cross-table field comparison (legacy, kept for compatibility)
    # -----------------------------------------------------------------

    def compare_fields(
        self,
        fields: Optional[List[str]] = None,
        tolerance: float = 0.01,
        limit_dates: Optional[int] = None,
    ) -> Dict[str, Dict]:
        """
        Compare specified fields between original and new table.

        Args:
            fields: List of field names to compare. Default = all.
            tolerance: Relative tolerance for float comparison.
            limit_dates: If set, only compare the N most recent dates.

        Returns:
            Dict[field_name, CompareResult.summary]
        """
        if fields is None:
            fields = [
                "open", "high", "low", "close", "volume",
                "amt", "oi", "settle", "long_margin", "short_margin",
            ]

        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                # Get date range
                date_filter = ""
                if limit_dates:
                    cur.execute(
                        f"SELECT DISTINCT date FROM `future_cn`.{NEW_TABLE} "
                        f"ORDER BY date DESC LIMIT {limit_dates}"
                    )
                    rows = cur.fetchall()
                    dates = tuple(r["date"] for r in rows)
                    if dates:
                        date_filter = f"AND o.date IN {dates}"

                col_list = ", ".join(
                    [f"o.{f} AS o_{f}" for f in fields]
                    + [f"n.{f} AS n_{f}" for f in fields]
                )
                sql = f"""
                    SELECT o.code, o.date, {col_list}
                    FROM `future_cn`.{ORIGINAL_TABLE} o
                    INNER JOIN `future_cn`.{NEW_TABLE} n
                        ON o.code = n.code AND o.date = n.date
                    WHERE 1=1 {date_filter}
                    ORDER BY o.date DESC
                """
                cur.execute(sql)
                rows = cur.fetchall()

        finally:
            conn.close()

        results = {f: CompareResult(f) for f in fields}

        for row in rows:
            code, date = row["code"], self._format_date(row["date"])
            for field in fields:
                ov = row.get(f"o_{field}")
                nv = row.get(f"n_{field}")
                cr = results[field]

                if ov is None and nv is None:
                    continue
                if ov is None and nv is not None:
                    cr.total_missing_original += 1
                    continue
                if ov is not None and nv is None:
                    cr.total_missing_new += 1
                    continue

                ov = float(ov)
                nv = float(nv)
                diff = abs(ov - nv)
                max_abs = max(abs(ov), abs(nv))
                deviation = diff / max_abs * 100 if max_abs > 0 else 0.0

                if deviation > tolerance:
                    cr.total_diff += 1
                    cr.max_deviation = max(cr.max_deviation, deviation)
                    if len(cr.sample_diffs) < 10:
                        cr.sample_diffs.append({
                            "code": code,
                            "date": date,
                            "original": ov,
                            "new": nv,
                            "deviation_pct": round(deviation, 4),
                        })
                else:
                    cr.total_matched += 1

        return {f: r.summary for f, r in results.items()}

    def get_field_stats(self, target_date: str) -> Dict:
        """
        Get field valid/missing stats for t_futures_info_exchange.

        Returns:
            Dict[exchange][field] = {total, non_null, null_pct}
        """
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                dt = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
                # Get all exchanges
                cur.execute(f"""
                    SELECT DISTINCT SUBSTRING_INDEX(code, '.', -1) AS ex
                    FROM future_cn.t_futures_info_exchange
                    WHERE date = '{dt}'
                    ORDER BY ex
                """)
                exchanges = [r["ex"] for r in cur.fetchall()]

                fields = [
                    "code", "date", "open", "high", "low", "close",
                    "volume", "amt", "oi", "settle", "maxup", "maxdown",
                    "long_margin", "short_margin", "minoq", "maxoq",
                ]

                result = {}
                for ex in exchanges:
                    result[ex] = {}
                    for field in fields:
                        # abnormal_null: 字段缺失且(amt缺失或amt!=0)才计数
                        # amt字段本身缺失则始终计数
                        cur.execute(f"""
                            SELECT COUNT(*) AS total,
                                   SUM(CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END) AS non_null,
                                   SUM(CASE WHEN {field} IS NULL AND (
                                       '{field}' = 'amt' OR amt IS NULL OR amt != 0
                                   ) THEN 1 ELSE 0 END) AS abnormal_null
                            FROM future_cn.t_futures_info_exchange
                            WHERE date = '{dt}'
                              AND SUBSTRING_INDEX(code, '.', -1) = '{ex}'
                        """)
                        row = cur.fetchone()
                        total = row["total"]
                        non_null = row["non_null"]
                        result[ex][field] = {
                            "total": total,
                            "non_null": non_null,
                            "abnormal_null": row["abnormal_null"],
                            "null_pct": round((total - non_null) / total * 100, 1) if total > 0 else 0,
                        }
        finally:
            conn.close()
        return result

    # -----------------------------------------------------------------
    # Abnormal null detail records
    # -----------------------------------------------------------------

    def get_abnormal_nulls(self, target_date: str) -> Dict[str, list]:
        """
        获取异常空值明细：字段缺失且 amt≠0 的记录。

        Returns:
            Dict[exchange] = [{code, open, high, low, close, amt, ...}] 按 amt 降序
        """
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                dt = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
                cur.execute(f"""
                    SELECT DISTINCT SUBSTRING_INDEX(code, '.', -1) AS ex
                    FROM future_cn.t_futures_info_exchange
                    WHERE date = '{dt}'
                    ORDER BY ex
                """)
                exchanges = [r["ex"] for r in cur.fetchall()]

                fields = ["open","high","low","close","volume","amt","oi",
                           "settle","maxup","maxdown","long_margin","short_margin"]
                result = {}
                for ex in exchanges:
                    if ex == "CSI":
                        continue
                    # Find records where ANY field is null but amt is non-zero
                    null_conditions = " OR ".join(
                        f"{f} IS NULL" for f in fields
                    )
                    col_list = "code, " + ", ".join(fields)
                    sql = f"""
                        SELECT {col_list}
                        FROM future_cn.t_futures_info_exchange
                        WHERE date = '{dt}'
                          AND SUBSTRING_INDEX(code, '.', -1) = '{ex}'
                          AND ({null_conditions})
                          AND (amt IS NOT NULL AND amt != 0)
                        ORDER BY amt DESC
                        LIMIT 10
                    """
                    cur.execute(sql)
                    rows = cur.fetchall()
                    if not rows:
                        continue
                    result[ex] = []
                    for row in rows:
                        rec = dict(row)
                        code = rec["code"]
                        # 对比旧表：标记哪些字段旧表也 NULL
                        null_fields = [f for f in fields
                                       if rec.get(f) is None
                                       and f != "amt"]
                        if not null_fields:
                            continue
                        # 查旧表同条数据
                        cur.execute(f"""
                            SELECT {', '.join(null_fields)}
                            FROM future_cn.t_futures_info
                            WHERE code = '{code}' AND date = '{dt}'
                        """)
                        old_row = cur.fetchone()
                        old_null = set()
                        if old_row:
                            for f in null_fields:
                                if old_row.get(f) is None:
                                    old_null.add(f)
                        rec["_old_null"] = old_null
                        rec["_null_fields"] = null_fields
                        rec["_classification"] = self._classify_abnormal_null(
                            rec, old_null
                        )
                        result[ex].append(rec)
        finally:
            conn.close()
        return result

    @staticmethod
    def _classify_abnormal_null(rec: dict, old_null: set = None) -> str:
        """
        判断异常空值是否为可允许异常（无旧表时的启发式规则）。

        规则：
        - OHLC 全部为空、close 有值、volume < 5 → 远月冷门合约 API 限制
        - 其余情况 → 需核查
        """
        nulls = [f for f in ("open", "high", "low") if rec.get(f) is None]
        if len(nulls) == 3 and rec.get("close") is not None:
            vol = rec.get("volume") or 0
            if vol < 5:
                return "✅ 可允许: 远月冷合API无OHLC"
        return "⚠️ 需核查"

    # -----------------------------------------------------------------
    # Comprehensive cross-table comparison
    # -----------------------------------------------------------------

    def compare_all(
        self,
        target_date: Optional[str] = None,
        tolerance: float = 0.01,
    ) -> Dict:
        """
        Comprehensive comparison between t_futures_info (original) and
        t_futures_info_exchange (new). Checks:

        1. Record counts per exchange
        2. Records missing from new table
        3. Records extra in new table
        4. Field-level differences for matching records

        Args:
            target_date: YYYYMMDD date to filter. None = all dates.
            tolerance: Relative tolerance (% deviation) for float fields.

        Returns:
            Dict with keys:
              - exchange_counts: {exchange: {original, new, missing_in_new, extra_in_new}}
              - field_diffs:    {field: {matched, diff, max_deviation_pct, sample_diffs}}
              - missing_records: [{code, date}] (in orig but not new)
              - extra_records:   [{code, date}] (in new but not orig)
              - summary:         overall summary text
        """
        date_where = ""
        if target_date:
            dt = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
            date_where = f"AND a.date = '{dt}'"

        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                # --- (A) Full outer join: matching status for all (code, date) ---
                sql = f"""
                    SELECT a.code, a.date, a.match_status
                    FROM (
                        SELECT o.code, o.date, 'matched' AS match_status
                        FROM `future_cn`.{ORIGINAL_TABLE} o
                        INNER JOIN `future_cn`.{NEW_TABLE} n
                            ON o.code = n.code AND o.date = n.date
                        UNION
                        SELECT o.code, o.date, 'missing_in_new'
                        FROM `future_cn`.{ORIGINAL_TABLE} o
                        LEFT JOIN `future_cn`.{NEW_TABLE} n
                            ON o.code = n.code AND o.date = n.date
                        WHERE n.code IS NULL
                        UNION
                        SELECT n.code, n.date, 'extra_in_new'
                        FROM `future_cn`.{NEW_TABLE} n
                        LEFT JOIN `future_cn`.{ORIGINAL_TABLE} o
                            ON n.code = o.code AND n.date = o.date
                        WHERE o.code IS NULL
                    ) a
                    WHERE 1=1 {date_where}
                    ORDER BY a.date DESC
                """
                cur.execute(sql)
                match_rows = cur.fetchall()

                missing_records = []
                extra_records = []
                matched_set = set()
                for row in match_rows:
                    status = row["match_status"]
                    key = (row["code"], self._format_date(row["date"]))
                    if status == "matched":
                        matched_set.add(key)
                    elif status == "missing_in_new":
                        missing_records.append({
                            "code": row["code"],
                            "date": self._format_date(row["date"]),
                        })
                    elif status == "extra_in_new":
                        extra_records.append({
                            "code": row["code"],
                            "date": self._format_date(row["date"]),
                        })

                # --- (B) Exchange-level count summary ---
                exchange_counts = {}
                for rec in missing_records:
                    ex = rec["code"].rsplit(".", 1)[-1] if "." in rec["code"] else "?"
                    exchange_counts.setdefault(ex, {
                        "original": 0, "new": 0,
                        "missing_in_new": 0, "extra_in_new": 0,
                    })["missing_in_new"] += 1
                for rec in extra_records:
                    ex = rec["code"].rsplit(".", 1)[-1] if "." in rec["code"] else "?"
                    exchange_counts.setdefault(ex, {
                        "original": 0, "new": 0,
                        "missing_in_new": 0, "extra_in_new": 0,
                    })["extra_in_new"] += 1

                # Count originals per exchange
                date_filter_orig = ""
                date_filter_new = ""
                if target_date:
                    dt = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
                    date_filter_orig = f"AND o.date = '{dt}'"
                    date_filter_new = f"AND n.date = '{dt}'"

                sql_orig = f"""
                    SELECT SUBSTRING_INDEX(o.code, '.', -1) AS ex,
                           COUNT(*) AS cnt
                    FROM `future_cn`.{ORIGINAL_TABLE} o
                    WHERE 1=1 {date_filter_orig}
                    GROUP BY ex
                """
                cur.execute(sql_orig)
                for row in cur.fetchall():
                    ex = row["ex"]
                    exchange_counts.setdefault(ex, {
                        "original": 0, "new": 0,
                        "missing_in_new": 0, "extra_in_new": 0,
                    })["original"] = row["cnt"]

                sql_new = f"""
                    SELECT SUBSTRING_INDEX(n.code, '.', -1) AS ex,
                           COUNT(*) AS cnt
                    FROM `future_cn`.{NEW_TABLE} n
                    WHERE 1=1 {date_filter_new}
                    GROUP BY ex
                """
                cur.execute(sql_new)
                for row in cur.fetchall():
                    ex = row["ex"]
                    exchange_counts.setdefault(ex, {
                        "original": 0, "new": 0,
                        "missing_in_new": 0, "extra_in_new": 0,
                    })["new"] = row["cnt"]

                # --- (C) Field-level comparison on matched records,
                #       broken down by exchange ---
                fields = [
                    "open", "high", "low", "close", "volume",
                    "amt", "oi", "settle", "maxup", "maxdown",
                    "long_margin", "short_margin", "minoq", "maxoq",
                ]
                # field_results[exchange][field] = CompareResult
                field_results: Dict[str, Dict[str, CompareResult]] = {}

                if not matched_set:
                    return {
                        "exchange_counts": exchange_counts,
                        "field_diffs": {},
                        "missing_records": missing_records,
                        "extra_records": extra_records,
                        "summary": self._format_summary(
                            exchange_counts, field_results,
                            missing_records, extra_records, target_date,
                        ),
                    }

                code_date_list = list(matched_set)
                for cd_start in range(0, len(code_date_list), 500):
                    batch = code_date_list[cd_start:cd_start + 500]
                    conditions = " OR ".join([
                        f"(o.code = '{c}' AND o.date = '{d[:4]}-{d[4:6]}-{d[6:8]}')"
                        for c, d in batch
                    ])
                    col_list = ", ".join(
                        [f"o.{f} AS o_{f}" for f in fields]
                        + [f"n.{f} AS n_{f}" for f in fields]
                    )
                    sql = f"""
                        SELECT o.code, o.date, {col_list}
                        FROM `future_cn`.{ORIGINAL_TABLE} o
                        INNER JOIN `future_cn`.{NEW_TABLE} n
                            ON o.code = n.code AND o.date = n.date
                        WHERE {conditions}
                    """
                    cur.execute(sql)
                    rows = cur.fetchall()

                    for row in rows:
                        code = row["code"]
                        date = self._format_date(row["date"])
                        ex = code.rsplit(".", 1)[-1] if "." in code else "?"

                        if ex not in field_results:
                            field_results[ex] = {
                                f: CompareResult(f) for f in fields
                            }

                        for field in fields:
                            ov = row.get(f"o_{field}")
                            nv = row.get(f"n_{field}")
                            cr = field_results[ex][field]

                            if ov is None and nv is None:
                                continue
                            if ov is None and nv is not None:
                                cr.total_missing_original += 1
                                continue
                            if ov is not None and nv is None:
                                cr.total_missing_new += 1
                                # 异常缺失：同一条的 amt(旧表) 不为 0 或 amt 本身缺失
                                oa = row.get("o_amt")
                                if field == "amt" or oa is None or float(oa) != 0:
                                    cr.total_abnormal_missing_new += 1
                                continue

                            ov = float(ov)
                            nv = float(nv)
                            diff_val = abs(ov - nv)
                            max_abs = max(abs(ov), abs(nv))
                            deviation = diff_val / max_abs * 100 if max_abs > 0 else 0.0

                            if deviation > tolerance:
                                cr.total_diff += 1
                                cr.max_deviation = max(cr.max_deviation, deviation)
                                if len(cr.sample_diffs) < 10:
                                    cr.sample_diffs.append({
                                        "code": code,
                                        "date": date,
                                        "original": round(ov, 4),
                                        "new": round(nv, 4),
                                        "deviation_pct": round(deviation, 4),
                                    })
                            else:
                                cr.total_matched += 1

        finally:
            conn.close()

        result = {
            "exchange_counts": exchange_counts,
            "field_diffs": {
                ex: {f: r.summary for f, r in ex_results.items()}
                for ex, ex_results in sorted(field_results.items())
            },
            "missing_records": missing_records,
            "extra_records": extra_records,
            "summary": self._format_summary(
                exchange_counts, field_results,
                missing_records, extra_records, target_date,
            ),
        }
        return result

    def _format_summary(
        self,
        exchange_counts: Dict,
        field_results: Dict[str, Dict[str, CompareResult]],
        missing_records: List,
        extra_records: List,
        target_date: Optional[str] = None,
    ) -> str:
        """Format a human-readable comparison summary."""
        lines = []
        title = f"对比报告: {target_date}" if target_date else "对比报告: 全量数据"
        lines.append(f"{'=' * 80}")
        lines.append(f"  {title}")
        lines.append(f"{'=' * 80}")

        # Exchange counts
        lines.append("")
        lines.append("【交易所合约数对比】")
        lines.append(
            f"{'交易所':>8s} | {'目标(原表)':>10s} | {'新表':>10s} | "
            f"{'缺漏':>8s} | {'多余':>8s}"
        )
        lines.append("-" * 56)
        for ex in sorted(exchange_counts.keys()):
            ec = exchange_counts[ex]
            lines.append(
                f"{ex:>8s} | {ec['original']:>10d} | {ec['new']:>10d} | "
                f"{ec['missing_in_new']:>8d} | {ec['extra_in_new']:>8d}"
            )

        # Missing records sample
        if missing_records:
            lines.append("")
            lines.append(
                f"【原表有但新表无】共 {len(missing_records)} 条"
            )
            for rec in missing_records[:15]:
                lines.append(f"  ❌ {rec['code']} @ {rec['date']}")
            if len(missing_records) > 15:
                lines.append(f"  ... 还有 {len(missing_records) - 15} 条")

        # Extra records sample
        if extra_records:
            lines.append("")
            lines.append(
                f"【新表有但原表无】共 {len(extra_records)} 条"
            )
            for rec in extra_records[:15]:
                lines.append(f"  ➕ {rec['code']} @ {rec['date']}")
            if len(extra_records) > 15:
                lines.append(f"  ... 还有 {len(extra_records) - 15} 条")

        # Field diffs by exchange
        lines.append("")
        lines.append("【分交易所 × 分字段差异明细】")
        lines.append(f"{'=' * 80}")

        all_fields = [
            "open", "high", "low", "close", "volume",
            "amt", "oi", "settle", "maxup", "maxdown",
            "long_margin", "short_margin", "minoq", "maxoq",
        ]

        for ex in sorted(field_results.keys()):
            ex_res = field_results[ex]
            has_any_diff = any(
                r.summary["diff"] > 0
                or r.summary["missing_in_original"] > 0
                or r.summary["missing_in_new"] > 0
                for r in ex_res.values()
            )

            lines.append(f"")
            lines.append(f"  ── {ex} ──")

            # Header row
            header = f"  {'字段':<14s} | {'匹配':>6s} | {'差异':>4s} | {'原表缺':>5s} | {'新表缺':>5s} | {'缺异':>5s} | {'最大偏差':>8s}"
            lines.append(header)
            lines.append(f"  {'-' * len(header)}")

            if not has_any_diff:
                lines.append(f"  ✅ 全部字段完全一致")
                continue

            for field in all_fields:
                s = ex_res[field].summary
                if s["diff"] == 0 and s["missing_in_original"] == 0 and s["missing_in_new"] == 0:
                    continue
                icon = "🔴" if s["diff"] > max(1, s["matched"] * 0.05) else "🟡"
                if s["diff"] == 0:
                    icon = "  "
                lines.append(
                    f"  {icon} {field:<12s} | {s['matched']:>6d} | {s['diff']:>4d} | "
                    f"{s['missing_in_original']:>5d} | {s['missing_in_new']:>5d} | "
                    f"{s.get('abnormal_missing_new', 0):>5d} | "
                    f"{s['max_deviation_pct']:>7.2f}%"
                )
                limit = None if s["diff"] < 10 else 10
                for sd in s["sample_diffs"][:limit]:
                    lines.append(
                        f"      → {sd['code']}: "
                        f"原表={sd['original']} 新表={sd['new']} "
                        f"偏差={sd['deviation_pct']:.2f}%"
                    )

        lines.append(f"{'=' * 80}")
        return "\n".join(lines)


if __name__ == "__main__":
    class PrintLogger:
        def info(self, msg):
            print(f"[INFO] {msg}")

        def warning(self, msg):
            print(f"[WARN] {msg}")

        def error(self, msg):
            print(f"[ERROR] {msg}")

        def alert(self, msg):
            print(f"[ALERT] {msg}")

    v = Verifier(PrintLogger())
    target = sys.argv[1] if len(sys.argv) > 1 else None
    if target:
        result = v.compare_all(target_date=target)
        print(result["summary"])
