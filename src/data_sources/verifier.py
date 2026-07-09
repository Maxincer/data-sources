#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import pymysql.cursors

from data_sources.db import get_connection, resolve_config


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
        self.sample_diffs: list[dict] = []
        self.sample_missing_in_new: list[dict] = []
        self.sample_missing_in_original: list[dict] = []

    @property
    def summary(self) -> dict:
        # 按偏差降序排列
        sorted_samples = sorted(self.sample_diffs,
                                key=lambda x: x.get("ratio", 0),
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
            "sample_missing_in_new": self.sample_missing_in_new[:10],
            "sample_missing_in_original": self.sample_missing_in_original[:10],
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
        previous_size: int | None,
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

    def get_previous_size(self, task) -> int | None:
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
        previous_size: int | None = None,
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

    def _get_conn(self, config_override: dict | None = None):
        """DB connection via env-configured db.py."""
        conn = get_connection(config_override)
        conn.cursorclass = pymysql.cursors.DictCursor
        return conn

    def _get_db(self, config_override: dict | None = None) -> str:
        """Return current database name."""
        return resolve_config(config_override)["database"]

    @staticmethod
    def _format_date(date_val) -> str:
        """Format date value to YYYYMMDD string."""
        if date_val is None:
            return ""
        s = str(date_val)
        return s.replace("-", "")[:8]

    # -----------------------------------------------------------------
    # Cross-table field comparison (legacy, kept for compatibility)

    def get_field_stats(self, target_date: str,
                        table: str = "t_futures_info_exchange",
                        config_override: dict | None = None) -> dict:
        """
        Get field valid/missing stats for a given table.

        Args:
            target_date: YYYYMMDD
            table: MySQL table name
            config_override: optional dict to override DB host/user/password

        Returns:
            dict[exchange][field] = {total, non_null, null_pct}
        """
        conn = self._get_conn(config_override)
        db = self._get_db(config_override)
        try:
            with conn.cursor() as cur:
                dt = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
                cur.execute(f"""
                    SELECT DISTINCT SUBSTRING_INDEX(code, '.', -1) AS ex
                    FROM `{db}`.`{table}`
                    WHERE date = '{dt}'
                    ORDER BY ex
                """)
                exchanges = [r["ex"] for r in cur.fetchall()]

                fields = [
                    "code", "date", "open", "high", "low", "close", "volume",
                    "amt", "oi", "settle", "maxup", "maxdown", "if_basis",
                    "long_margin", "short_margin", "minoq", "maxoq",
                ]

                result = {}
                for ex in exchanges:
                    result[ex] = {}
                    for field in fields:
                        extra_abn = ""
                        if field == "if_basis":
                            if ex != "CFE":
                                extra_abn = " AND 1=0"
                            else:
                                extra_abn = " AND code REGEXP '^(IF|IC|IH|IM)'"
                        cur.execute(f"""
                            SELECT COUNT(*) AS total,
                                   SUM(CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END) AS non_null,
                                   SUM(CASE WHEN {field} IS NULL AND (
                                       '{field}' = 'amt' OR (amt IS NOT NULL AND amt != 0)
                                   ){extra_abn} THEN 1 ELSE 0 END) AS abnormal_null
                            FROM `{db}`.`{table}`
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

    def get_abnormal_nulls(self, target_date: str,
                           table: str = "t_futures_info_exchange",
                           config_override: dict | None = None,
                           ref_config_override: dict | None = None) -> dict[str, list]:
        """
        获取异常空值明细：字段缺失且 amt≠0 的记录。

        Args:
            target_date: YYYYMMDD
            table: MySQL table name
            config_override: 新表 DB 配置 (默认走 env)
            ref_config_override: 旧表 DB 配置 (默认同新表)

        Returns:
            dict[exchange] = [{code, open, high, low, close, amt, ...}] 按 amt 降序
        """
        conn = self._get_conn(config_override)
        db = self._get_db(config_override)
        try:
            with conn.cursor() as cur:
                dt = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
                cur.execute(f"""
                    SELECT DISTINCT SUBSTRING_INDEX(code, '.', -1) AS ex
                    FROM `{db}`.`{table}`
                    WHERE date = '{dt}'
                    ORDER BY ex
                """)
                exchanges = [r["ex"] for r in cur.fetchall()]

                fields = ["open","high","low","close","volume","amt","oi",
                           "settle","maxup","maxdown","long_margin",
                           "short_margin","minoq","maxoq"]
                result = {}
                for ex in exchanges:
                    if ex == "CSI":
                        continue
                    null_conditions = " OR ".join(
                        f"{f} IS NULL" for f in fields
                    )
                    col_list = "code, " + ", ".join(fields)
                    sql = f"""
                        SELECT {col_list}
                        FROM `{db}`.`{table}`
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
                        null_fields = [f for f in fields
                                       if rec.get(f) is None
                                       and f != "amt"]
                        if not null_fields:
                            continue
                        # 查旧表同条数据
                        ref_db = self._get_db(ref_config_override)
                        orig_conn = self._get_conn(ref_config_override)
                        try:
                            with orig_conn.cursor() as orig_cur:
                                orig_cur.execute(f"""
                                    SELECT {', '.join(null_fields)}
                                    FROM `{ref_db}`.t_futures_info
                                    WHERE code = '{code}' AND date = '{dt}'
                                """)
                                old_row = orig_cur.fetchone()
                        finally:
                            orig_conn.close()
                        old_null = set()
                        if old_row:
                            for f in null_fields:
                                if old_row.get(f) is None:
                                    old_null.add(f)
                        rec["_old_null"] = old_null
                        rec["_null_fields"] = null_fields
                        rec["_classification"] = self._classify_abnormal_null(rec)
                        result[ex].append(rec)
        finally:
            conn.close()
        return result

    @staticmethod
    def _classify_abnormal_null(rec: dict) -> str:
        """
        判断异常空值是否为可允许异常（无旧表时的启发式规则）。

        规则：
        - OHLC 全部为空、close 有值、volume < 300 → 冷门/临期合约无OHLC
        - 其余情况 → 需核查
        """
        nulls = [f for f in ("open", "high", "low") if rec.get(f) is None]
        if len(nulls) == 3 and rec.get("close") is not None:
            vol = rec.get("volume") or 0
            if vol < 500:
                return "✅ 可允许: 量小无OHLC"
        return "⚠️ 需核查"

    def compare_all(
        self,
        target_date: str,
        data_a: list[dict],
        data_b: list[dict],
    ) -> dict:
        """Compare two record lists field-by-field for a given date.

        Each data source is a list of records:
          {"code": "CU2606.SHF", "date": "20260526", "open": 105720.0, ...}

        Returns:
            dict with keys: exchange_counts, field_diffs, missing_records,
            extra_records, summary
        """
        fields = [
            "open", "high", "low", "close", "volume",
            "amt", "oi", "settle", "maxup", "maxdown",
            "if_basis", "long_margin", "short_margin", "minoq", "maxoq",
        ]

        # Per-field decimal precision (default 4)
        _PRECISION = {
            "long_margin": 0, "short_margin": 0,
            "volume": 0, "oi": 0, "minoq": 0, "maxoq": 0,
        }

        # ---- Build lookups {(code, date_str): record} ----
        lookup_a: dict = {}
        for r in data_a:
            code = r.get("code", "")
            if not code:
                continue
            date_str = r.get("date", target_date)
            if isinstance(date_str, str) and len(date_str) == 8:
                pass  # already YYYYMMDD
            lookup_a[(code, date_str)] = r

        lookup_b: dict = {}
        for r in data_b:
            code = r.get("code", "")
            if not code:
                continue
            date_str = r.get("date", target_date)
            lookup_b[(code, date_str)] = r

        # ---- Matching / missing / extra ----
        codes_a = {k[0] for k in lookup_a}
        codes_b = {k[0] for k in lookup_b}
        matched = codes_a & codes_b
        missing = codes_a - codes_b
        extra = codes_b - codes_a

        exchange_counts = {}
        for c in codes_a:
            ex = c.rsplit(".", 1)[-1] if "." in c else "?"
            exchange_counts.setdefault(ex, {
                "source_a": 0, "source_b": 0, "missing": 0, "extra": 0,
            })["source_a"] += 1
        for c in codes_b:
            ex = c.rsplit(".", 1)[-1] if "." in c else "?"
            exchange_counts.setdefault(ex, {
                "source_a": 0, "source_b": 0, "missing": 0, "extra": 0,
            })["source_b"] += 1
        for c in missing:
            ex = c.rsplit(".", 1)[-1] if "." in c else "?"
            exchange_counts.setdefault(ex, {
                "source_a": 0, "source_b": 0, "missing": 0, "extra": 0,
            })["missing"] += 1
        for c in extra:
            ex = c.rsplit(".", 1)[-1] if "." in c else "?"
            exchange_counts.setdefault(ex, {
                "source_a": 0, "source_b": 0, "missing": 0, "extra": 0,
            })["extra"] += 1

        # Missing/extra records with full data from source
        missing_records = []
        for c in sorted(missing):
            rec = lookup_a.get((c, target_date))
            if rec is None:
                # try any date for this code
                for k in lookup_a:
                    if k[0] == c:
                        rec = lookup_a[k]
                        break
            missing_records.append(rec if rec else {"code": c})

        extra_records = []
        for c in sorted(extra):
            rec = lookup_b.get((c, target_date))
            if rec is None:
                for k in lookup_b:
                    if k[0] == c:
                        rec = lookup_b[k]
                        break
            extra_records.append(rec if rec else {"code": c})

        # ---- Field-level comparison on matched codes ----
        field_results = {}

        for code in sorted(matched):
            a_row = lookup_a.get((code, target_date))
            if a_row is None:
                for k in lookup_a:
                    if k[0] == code:
                        a_row = lookup_a[k]
                        break
            b_row = lookup_b.get((code, target_date))
            if b_row is None:
                for k in lookup_b:
                    if k[0] == code:
                        b_row = lookup_b[k]
                        break
            if not a_row or not b_row:
                continue

            ex = code.rsplit(".", 1)[-1] if "." in code else "?"
            if ex not in field_results:
                field_results[ex] = {f: CompareResult(f) for f in fields}

            for field in fields:
                av = a_row.get(field)
                bv = b_row.get(field)
                cr = field_results[ex][field]

                if av is None and bv is None:
                    continue
                if av is None and bv is not None:
                    # TAS 合约：交易所 API 不提供，豁免条件 a
                    if "TAS" in code:
                        cr.total_matched += 1
                        continue
                    cr.total_missing_original += 1
                    bvf = round(float(bv), _PRECISION.get(field, 4)) if isinstance(bv, (int, float)) else bv
                    if len(cr.sample_missing_in_original) < 5:
                        cr.sample_missing_in_original.append({
                            "code": code, "date": target_date,
                            "original": None, "new": bvf,
                        })
                    continue
                if av is not None and bv is None:
                    # minoq=1 且 b_source 为空 → 视为一致，不列示
                    if field == "minoq" and av == 1:
                        cr.total_matched += 1
                        continue
                    cr.total_missing_new += 1
                    avf = round(float(av), _PRECISION.get(field, 4)) if isinstance(av, (int, float)) else av
                    if len(cr.sample_missing_in_new) < 5:
                        cr.sample_missing_in_new.append({
                            "code": code, "date": target_date,
                            "original": avf, "new": None,
                        })
                    continue

                try:
                    avf = round(float(av), 4)
                    bvf = round(float(bv), 4)
                except (TypeError, ValueError):
                    avf = av
                    bvf = bv

                if avf != bvf:
                    abs_diff = abs(avf - bvf) if isinstance(avf, (int, float)) and isinstance(bvf, (int, float)) else 0
                    ratio = abs_diff / abs(bvf) if bvf else 0
                    # vol/amt/oi 差异 ≤ 千分之一视为匹配（大数值浮点误差）
                    if field in ("volume", "amt", "oi") and ratio <= 0.001:
                        cr.total_matched += 1
                        continue
                    # if_basis 绝对值 < 0.001 视为匹配（基差接近 0 时比例不稳定）
                    if field == "if_basis" and abs_diff < 0.001:
                        cr.total_matched += 1
                        continue
                    cr.total_diff += 1
                    cr.max_deviation = max(cr.max_deviation, ratio)
                    if len(cr.sample_diffs) < 10:
                        cr.sample_diffs.append({
                            "code": code, "date": target_date,
                            "original": avf, "new": bvf,
                            "a-b": round(avf - bvf, 4),
                            "ratio": round(ratio, 6),
                        })
                else:
                    cr.total_matched += 1

        # ---- Summary aggregations ----
        field_summary = {}
        for ex, ex_results in field_results.items():
            for fname, cr in ex_results.items():
                if fname not in field_summary:
                    field_summary[fname] = {
                        "matched": 0, "diff": 0,
                        "missing_a": 0, "missing_b": 0,
                    }
                s = cr.summary
                field_summary[fname]["matched"] += s["matched"]
                field_summary[fname]["diff"] += s["diff"]
                field_summary[fname]["missing_a"] += s["missing_in_original"]
                field_summary[fname]["missing_b"] += s["missing_in_new"]

        result = {
            "exchange_counts": exchange_counts,
            "field_diffs": {
                ex: {f: r.summary for f, r in ex_results.items()}
                for ex, ex_results in sorted(field_results.items())
            },
            "field_summary": field_summary,
            "missing_records": missing_records,
            "extra_records": extra_records,
            "missing_count": len(missing_records),
            "extra_count": len(extra_records),
            "matched_count": len(codes_a & codes_b),
            "total_source_a": len(codes_a),
            "total_source_b": len(codes_b),
            "summary": self._format_summary(
                exchange_counts, field_results,
                missing_records, extra_records, target_date,
            ),
        }
        return result

    def _format_summary(
        self,
        exchange_counts: dict,
        field_results: dict[str, dict[str, CompareResult]],
        missing_records: list,
        extra_records: list,
        target_date: str | None = None,
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
            f"{'交易所':>8s} | {'source_a':>10s} | {'source_b':>10s} | "
            f"{'缺漏':>8s} | {'多余':>8s}"
        )
        lines.append("-" * 56)
        for ex in sorted(exchange_counts.keys()):
            ec = exchange_counts[ex]
            sa = ec.get("source_a", ec.get("original", 0))
            sb = ec.get("source_b", ec.get("new", 0))
            ms = ec.get("missing", ec.get("missing_in_new", 0))
            ex_c = ec.get("extra", ec.get("extra_in_new", 0))
            lines.append(
                f"{ex:>8s} | {sa:>10d} | {sb:>10d} | "
                f"{ms:>8d} | {ex_c:>8d}"
            )

        # Missing records sample
        if missing_records:
            lines.append("")
            lines.append(
                f"【原表有但新表无】共 {len(missing_records)} 条"
            )
            for rec in missing_records[:15]:
                d = rec.get("date", "")
                dt = f" @ {d}" if d else ""
                lines.append(f"  ❌ {rec['code']}{dt}")
            if len(missing_records) > 15:
                lines.append(f"  ... 还有 {len(missing_records) - 15} 条")

        # Extra records sample
        if extra_records:
            lines.append("")
            lines.append(
                f"【新表有但原表无】共 {len(extra_records)} 条"
            )
            for rec in extra_records[:15]:
                d = rec.get("date", "")
                dt = f" @ {d}" if d else ""
                lines.append(f"  ➕ {rec['code']}{dt}")
            if len(extra_records) > 15:
                lines.append(f"  ... 还有 {len(extra_records) - 15} 条")

        # Field diffs by exchange
        lines.append("")
        lines.append("【分交易所 × 分字段差异明细】")
        lines.append(f"{'=' * 80}")

        all_fields = [
            "open", "high", "low", "close", "volume",
            "amt", "oi", "settle", "maxup", "maxdown",
            "if_basis", "long_margin", "short_margin", "minoq", "maxoq",
        ]

        for ex in sorted(field_results.keys()):
            ex_res = field_results[ex]
            has_any_diff = any(
                r.summary["diff"] > 0
                or r.summary["missing_in_original"] > 0
                or r.summary["missing_in_new"] > 0
                for r in ex_res.values()
            )

            lines.append("")
            lines.append(f"  ── {ex} ──")

            # Header row
            ab_label = "|a-b|/b"
            header = f"  {'字段':<14s} | {'匹配':>6s} | {'差异':>4s} | {'a缺':>5s} | {'b缺':>5s} | {'缺异':>5s} | {ab_label:>10s}"
            lines.append(header)
            lines.append(f"  {'-' * len(header)}")

            if not has_any_diff:
                lines.append("  ✅ 全部字段完全一致")
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
                    f"{s['max_deviation_pct']:>10.6f}"
                )
                limit = None if s["diff"] < 10 else 10
                for sd in s["sample_diffs"][:limit]:
                    lines.append(
                        f"      → {sd['code']}: "
                        f"原表={sd['original']} 新表={sd['new']} "
                        f"a-b={sd.get('a-b','—')} |a-b|/b={sd.get('ratio', 0):.6f}"
                    )

        lines.append(f"{'=' * 80}")
        return "\n".join(lines)
