#!/usr/bin/env python3
"""Reporter: all pipeline reporting in one place."""

import argparse
import json
import logging
import os
import smtplib
import ssl
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import List, Optional


import requests

from mxz_utils.logging_config import get_logger
from data_sources.task import Task
from data_sources.verifier import Verifier
from data_sources.db import fetch_table
from data_sources.wind_client import fetch_wind_data



_FULL_FIELDS = [
    "open", "high", "low", "close", "volume", "amt", "oi",
    "settle", "maxup", "maxdown", "if_basis", "long_margin", "short_margin",
    "minoq", "maxoq",
]

_FEISHU_WEBHOOK = os.environ["FEISHU_WEBHOOK"]


def _parse_date_safe(date_str: str):
    """Parse YYYYMMDD or YYYY-like string to datetime, None on failure."""
    import re as _re
    if not date_str:
        return None
    d = date_str.strip().replace("-", "")
    d = _re.sub(r'[A-Z]+', lambda m: '01' * (len(m.group()) // 2), d)
    if len(d) >= 8 and d[:8].isdigit():
        return datetime.strptime(d[:8], "%Y%m%d")
    return None


class Reporter:
    """All pipeline reporting methods."""
    def __init__(self, logger=None):
        self.logger = logger or get_logger(
            "reporter", logging.DEBUG,
            os.environ["LOG_DIR"], "Reporter",
        )

    def task_report(self, tasks: List[Task], trade_date: str) -> None:
        """Send a Markdown table of file size changes."""
        if not tasks:
            return
        tasks = [t for t in tasks if t.filepath is not None]
        if not tasks:
            return
        lines = [
            f"📊 文件尺寸检查报告 {trade_date}",
            "| 文件名 | 本次大小(bytes) | 上次大小(bytes) | 变化(%) |",
            "| :--- | ---: | ---: | ---: |",
        ]
        total_current = 0
        for task in sorted(tasks, key=lambda t: t.filepath.name):
            fn = task.filepath.name
            cur_size = task.size
            prev_size = task.previous_size
            change = task.change_percent
            prev_str = str(prev_size) if prev_size else "N/A"
            change_str = f"{change}%" if change else "N/A"
            alert = " ⚠️" if change and abs(change) > 5 else ""
            lines.append(
                f"| {fn} | {cur_size} | {prev_str}"
                f" | {change_str}{alert} |"
            )
            total_current += cur_size
        lines.append(f"总大小: {total_current:,} 字节")
        self.logger.alert("\n".join(lines))

    def generate_daily(
        self, date_str: str, *,
        skip_table_compare: bool = False,
        skip_wind: bool = False,
        sender: Optional[str] = None,
        email_recipients: Optional[list[str]] = None,
    ) -> None:
        """Generate daily data verification report.

        Args:
            date_str: 交易日 YYYYMMDD
            skip_table_compare: True=阶段二，跳过两表对比，仅做 WSS 交叉验证
            skip_wind: True=跳过 Wind WSS 交叉验证（开发/无 cpp_py 环境）
        """
        # 比对库 (旧表 t_futures_info) 配置，通过环境变量传入
        _ref_db = {
            k: v for k, v in {
                "host": os.environ["REF_DB_HOST"],
                "user": os.environ["REF_DB_USER"],
                "password": os.environ["REF_DB_PASSWORD"],
                "database": os.environ["REF_DB_DATABASE"]
            }.items() if v is not None
        } or None

        v = Verifier(self.logger)
        stats = v.get_field_stats(date_str)
        abnormal = v.get_abnormal_nulls(date_str, ref_config_override=_ref_db)

        feishu_sections = [
            self._build_file_size_section(date_str),
            self._build_field_stats_section(stats, abnormal),
        ]

        # 公告人工处理提醒
        review_section = self._build_announcement_review_section(date_str)
        if review_section:
            feishu_sections.append(review_section)

        # Build comparison results (shared by Feishu and email)
        comparisons: list[tuple[str, dict]] = []
        ours = fetch_table("t_futures_info_exchange", date_str)

        if not skip_table_compare:
            # Phase 1a: 两表对比 (旧表 t_futures_info 可能在独立 DB)
            wind_tbl = fetch_table("t_futures_info", date_str, _ref_db)
            comp = v.compare_all(date_str, ours, wind_tbl)
            comparisons.append((
                "cross-validation: t_futures_info_exchange vs t_futures_info",
                comp,
            ))

        # Phase 1b / Phase 2: Wind WSS 交叉验证
        if not skip_wind:
            wind_data = fetch_wind_data(date_str)
            wind_comp = v.compare_all(date_str, ours, wind_data)
            title = (
                "cross-validation: t_futures_info vs wss"
                if skip_table_compare
                else "cross-validation: t_futures_info_exchange vs wss"
            )
            comparisons.append((title, wind_comp))

        # Feishu
        for title, comp in comparisons:
            feishu_sections.append(
                self._build_comparison_section(title, comp, date_str)
            )
        self._send_feishu_markdown(
            f"📊 数据验证报告 {date_str}",
            "\n\n---\n\n".join(feishu_sections),
        )
        self._smtp_send(
            date_str, stats, abnormal, comparisons,
            sender, email_recipients
        )

        self.logger.info("Report for %s sent.", date_str)

    def send_email(
        self, date_str: str,
        sender: Optional[str] = None,
        recipients: Optional[list[str]] = None,
    ) -> None:
        """快捷方式：只发邮件。"""
        self.generate_daily(date_str,sender=sender,email_recipients=recipients)

    def _smtp_send(self, date_str, stats, abnormal, comparisons,
                   sender=None, recipients=None):
        """发送邮件（SMTP）。"""
        password = os.environ["SMTP_PASSWORD"]
        if not password:
            self.logger.warning("SMTP_PASSWORD 未设置，跳过邮件发送")
            return

        if not sender or not recipients:
            self.logger.warning("发件人/收件人未设置，跳过邮件发送")
            return

        html_parts = [
            self._email_header(date_str),
            self._email_file_size_section(date_str),
        ]
        html_parts.append(self._email_field_stats(stats, abnormal))
        for title, comp in comparisons:
            html_parts.append(
                self._email_comparison_section(title, comp, date_str)
            )
        html_parts.append(self._email_footer())

        msg = MIMEMultipart("alternative")
        msg["Subject"] = (
            f"📊 期货数据验证报告 "
            f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        )
        msg["From"] = sender
        msg["To"] = ", ".join(recipients)
        msg.attach(MIMEText("\n".join(html_parts), "html", "utf-8"))

        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(
            os.environ["SMTP_HOST"],
            int(os.environ["SMTP_PORT"]),
            context=ctx) as server:
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_string())
        self.logger.info("邮件已发送至: %s", ", ".join(recipients))

    @staticmethod
    def _send_feishu_markdown(title: str, content: str):
        """Send a markdown message to the Feishu webhook."""
        if len(content) > 25000:
            content = content[:25000] + "\n\n... (truncated)"

        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": title},
                    "template": "blue",
                },
                "elements": [
                    {"tag": "markdown", "content": content},
                ],
            },
        }
        try:
            resp = requests.post(_FEISHU_WEBHOOK, json=payload, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            print(f"[WARN] Failed to send Feishu message: {e}")

    @staticmethod
    def _build_announcement_review_section(date_str: str) -> str | None:
        """检查需要人工处理的公告, 返回飞书消息或 None.

        条件:
          1. needs_review = 1
          2. publish_date <= today <= effective_date + 7 days
             (或 effective_date 为空时取 publish_date)
        """
        import csv


        csv_path = (
            Path(os.environ["DATA_DIR"])
            / "fields_from_announcements.csv"
        )
        if not csv_path.exists():
            return None

        today = datetime.strptime(date_str, "%Y%m%d")
        review_items: list[dict] = []

        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if str(row.get("needs_review", "")).strip() != "1":
                    continue
                pd_str = row.get("publish_date", "").strip()
                eff_str = row.get("effective_date", "").strip()
                # effective_date 含占位字母时取 publish_date 作为窗口起点
                window_start = (
                    _parse_date_safe(eff_str) or _parse_date_safe(pd_str)
                )
                if not window_start:
                    continue
                window_end = window_start + timedelta(days=7)
                if not (window_start <= today <= window_end):
                    continue
                review_items.append({
                    "title": row.get("announcement_title", ""),
                    "url": row.get("page_url", ""),
                    "exchange": row.get("exchange", ""),
                    "publish_date": pd_str,
                    "effective_date": eff_str,
                })

        if not review_items:
            return None

        # 去重 (同一公告可能多行, 按 url 去重)
        seen_urls = set()
        unique_items = []
        for item in review_items:
            url = item["url"]
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_items.append(item)

        lines = [
            "**🔔 需人工处理的公告 (生效后一周内)**",
            "",
        ]
        for item in unique_items[:20]:
            ex = item["exchange"]
            title = item["title"][:80]
            url = item["url"]
            eff = item["effective_date"]
            lines.append(f"- [{ex}] [{title}]({url}) 生效: {eff}")
        if len(unique_items) > 20:
            lines.append(f"  ... 还有 {len(unique_items) - 20} 条")
        return "\n".join(lines)

    @staticmethod
    def _build_file_size_section(date_str: str) -> str:
        """Section 1: file size changes from metadata."""
        metadata_file = (
            Path(os.environ["DATA_DIR"])
            / "raw" / "structured" / ".metadata.jsonl"
        )
        lines = [f"**交易日期**: {date_str}", ""]
        if metadata_file.exists():
            lines.append("**数据文件尺寸变化**:")
            lines.append("| 文件 | 本次(bytes) | 上次(bytes) | 变化率 |")
            lines.append("| :--- | ---: | ---: | ---: |")
            total_size = 0
            # 去重：每个文件只取最后一次记录
            records: dict[str, dict] = {}
            with open(metadata_file, encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        lfn = rec.get("local_filename", "")
                        if date_str in lfn:
                            records[lfn] = rec
                    except Exception:
                        pass
            for fn, rec in sorted(records.items()):
                sz = rec.get("file_size_bytes", 0)
                prev = rec.get("previous_size_bytes")
                chg = rec.get("size_change_percent")
                prev_str = f"{prev:,}" if prev else "N/A"
                chg_str = f"{chg:+.2f}%" if chg else "N/A"
                flag = " ⚠️" if chg and abs(chg) > 5 else ""
                lines.append(
                    f"| {fn} | {sz:,} | {prev_str} |"
                    f" {chg_str}{flag} |"
                )
                total_size += sz
            lines.append(f"| **合计** | **{total_size:,}** | | |")
        else:
            lines.append("⚠️ 无元数据文件")
        return "\n".join(lines)

    @staticmethod
    def _build_field_stats_section(stats: dict, abnormal: dict) -> str:
        """Section 2: field coverage + abnormal nulls."""
        lines = ["**字段覆盖率统计** (t_futures_info_exchange):", ""]
        for ex in sorted(stats.keys()):
            if ex == "CSI":
                continue
            total = (stats[ex].get("code", {}) or {}).get("total", 0) or 0
            lines.append(f"**{ex}** ({total} 条):")
            lines.append("| 字段 | 非空 | 缺失率 | 异常空值 |")
            lines.append("| :--- | ---: | ---: | ---: |")
            for field in _FULL_FIELDS:
                s = stats[ex].get(field, {}) or {}
                nn = s.get("non_null", 0) or 0
                to = s.get("total", 0) or 0
                pct = s.get("null_pct", 0) or 0
                abn = s.get("abnormal_null", 0) or 0
                ok_rate = (to - abn) / to * 100 if to > 0 else 100
                icon = "✅" if abn == 0 else ("🟡" if ok_rate >= 90 else "🔴")
                lines.append(
                    f"| {icon} {field} | {nn}/{to} | {pct}% | {abn} |"
                )

            if ex in abnormal and abnormal[ex]:
                lines.append("")
                lines.append("异常空值明细 (按金额降序, 最多20条):")
                lines.append("| 合约 | 空值字段 | amt | 判定 |")
                lines.append("| :--- | :--- | ---: | :--- |")
                for rec in abnormal[ex]:
                    code = (rec.get("code", "") or "").split(".")[0] or "?"
                    nulls = rec.get("_null_fields", []) or []
                    old_nulls = rec.get("_old_null", set()) or set()
                    cls = rec.get("_classification", "") or ""
                    tags = [
                        f"{f}(旧表同)" if f in old_nulls else f
                        for f in nulls
                    ]
                    null_str = ", ".join(tags)
                    amt = rec.get("amt")
                    amt_str = f"{amt:>,.0f}" if amt else "—"
                    lines.append(
                        f"| {code} | {null_str} | {amt_str} |"
                        f" {cls} |"
                    )
            lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _build_comparison_section(
        title: str, comp: dict, _date_str: str
    ) -> str:
        """Unified comparison report section.

        Structure:
          ## {title}
          ### contracts count
            totals + per-exchange table + 缺漏/多余明细
          ### groupby ex, field
            field summary table + per-exchange per-field details
        """
        lines = [f"## {title}", ""]

        ec = comp.get("exchange_counts", {})

        # ---- contracts count ----
        lines.append("### contracts count")
        lines.append("")
        lines.append(
            f"source_a: {comp.get('total_source_a',0)} 条, "
            f"source_b: {comp.get('total_source_b',0)} 条, "
            f"匹配: {comp.get('matched_count',0)} 条"
        )
        lines.append("")
        lines.append("| 交易所 | source_a | source_b | 多余 | 缺漏 |")
        lines.append("| :--- | ---: | ---: | ---: | ---: |")
        for ex in sorted(ec.keys()):
            if ex == "CSI":
                continue
            d = ec[ex]
            lines.append(
                f"| {ex} | {d['source_a']} | {d['source_b']} | "
                f"{d['missing']} | {d['extra']} |"
            )
        lines.append("")

        # Missing / extra records
        missing_records = comp.get("missing_records", [])
        extra_records = comp.get("extra_records", [])
        mc = comp.get("missing_count", len(missing_records))
        ecnt = comp.get("extra_count", len(extra_records))
        if mc:
            lines.append(f"**多余** (source_a有/source_b无, 共{mc}条, 最多10条):")
            for rec in missing_records[:10]:
                code = rec.get("code", "?")
                open_v = rec.get("open", "—")
                close_v = rec.get("close", "—")
                settle_v = rec.get("settle", "—")
                lines.append(
                    f"  ❌ code={code}, open={open_v}, "
                    f"close={close_v}, settle={settle_v}"
                )
            lines.append("")
        if ecnt:
            lines.append(f"**缺漏** (source_b有/source_a无, 共{ecnt}条, 最多10条):")
            for rec in extra_records[:10]:
                code = rec.get("code", "?")
                open_v = rec.get("open", "—")
                close_v = rec.get("close", "—")
                settle_v = rec.get("settle", "—")
                lines.append(
                    f"  ➕ code={code}, open={open_v}, "
                    f"close={close_v}, settle={settle_v}"
                )
            lines.append("")

        # ---- groupby ex, field ----
        lines.append("### groupby ex, field")
        lines.append("")

        fs = comp.get("field_summary", {})
        if fs:
            lines.append("**字段差异汇总**:")
            lines.append("")
            lines.append("| 字段 | 匹配 | 差异 | a缺 | b缺 |")
            lines.append("| :--- | ---: | ---: | ---: | ---: |")
            for fname in _FULL_FIELDS:
                s = fs.get(fname, {})
                icon = (
                    "✅"
                    if s.get("diff", 0) == 0
                    and s.get("missing_a", 0) == 0
                    and s.get("missing_b", 0) == 0
                    else ""
                )
                lines.append(
                    (
                        f"| {icon}{fname} "
                        f"| {s.get('matched',0)} "
                        f"| {s.get('diff',0)} "
                        f"| {s.get('missing_a',0)} "
                        f"| {s.get('missing_b',0)} |"
                    )
                )
            lines.append("")

        fd = comp.get("field_diffs", {})
        has_diff = False
        for ex in sorted(fd.keys()):
            if ex == "CSI":
                continue
            ex_diffs = fd[ex]
            has_any = any(
                s["diff"] > 0
                or s["missing_in_original"] > 0
                or s["missing_in_new"] > 0
                for s in ex_diffs.values()
            )
            if not has_any:
                continue
            has_diff = True
            for field in _FULL_FIELDS:
                s = ex_diffs.get(field, {})
                if (
                    s["diff"] == 0
                    and s["missing_in_original"] == 0
                    and s["missing_in_new"] == 0
                ):
                    continue
                diff_count = s.get("diff", 0)
                limit = None if diff_count < 10 else 10
                samples = sorted(
                    s.get("sample_diffs", []),
                    key=lambda x: x.get("ratio", 0), reverse=True,
                )
                missing_b = s.get("sample_missing_in_new", [])
                missing_a = s.get("sample_missing_in_original", [])
                total = diff_count + len(missing_a) + len(missing_b)
                lines.append(f"  **{ex} {field}** (共{total}条):")

                # if_basis: 差异极小 (<0.001) 时只显示条数, 不逐条列示
                if field == "if_basis":
                    tiny_diffs = [
                        sd for sd in samples
                        if isinstance(sd.get('a-b'), (int, float))
                        and abs(sd['a-b']) < 0.001
                    ]
                    if (
                        tiny_diffs
                        and len(tiny_diffs) == len(samples)
                        and not missing_a
                        and not missing_b
                    ):
                        lines.append(
                            f"    {len(tiny_diffs)} 条差异均 < 0.001 (可忽略)"
                        )
                        continue

                for sd in samples[:limit]:
                    a_b = sd.get('a-b', '—')
                    ratio = sd.get('ratio', '-')
                    ratio_str = (
                        f" |a-b|/b={ratio:.6f}"
                        if isinstance(ratio, (int, float))
                        else ""
                    )
                    lines.append(
                        f"    {sd['code']}:"
                        f" a={sd['original']}"
                        f" b={sd['new']}"
                        f" a-b={a_b}"
                        f"{ratio_str}"
                    )
                for sd in missing_b:
                    lines.append(
                        f"    {sd['code']}:"
                        f" a={sd['original']} b=— a-b=—"
                    )
                for sd in missing_a:
                    lines.append(
                        f"    {sd['code']}:"
                        f" a=— b={sd['new']} a-b=—"
                    )

        if not has_diff and not mc:
            lines.append("✅ 全部字段完全一致")

        return "\n".join(lines)

    @staticmethod
    def _email_file_size_section(date_str: str) -> str:
        """File size section for email, matching Feishu format."""
        metadata_file = (
            Path(os.environ["DATA_DIR"])
            / "raw" / "structured" / ".metadata.jsonl"
        )
        parts = ['<h2>📁 数据文件尺寸变化</h2>']
        if metadata_file.exists():
            parts.append(
                '<table border="1" cellpadding="8" cellspacing="0"'
                ' style="border-collapse:collapse;width:100%;">'
            )
            parts.append(
                '<tr style="background:#f5f5f5;">'
                '<th>文件</th><th>本次(bytes)</th><th>上次(bytes)</th>'
                '<th>变化率</th></tr>'
            )
            total_size = 0
            # 去重：每个文件只取最后一次记录
            records: dict[str, dict] = {}
            with open(metadata_file, encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        lfn = rec.get("local_filename", "")
                        if date_str in lfn:
                            records[lfn] = rec
                    except Exception:
                        pass
            for fn, rec in sorted(records.items()):
                sz = rec.get("file_size_bytes", 0)
                prev = rec.get("previous_size_bytes")
                chg = rec.get("size_change_percent")
                prev_str = f"{prev:,}" if prev else "N/A"
                chg_str = f"{chg:+.2f}%" if chg else "N/A"
                flag = " 🔴" if chg and abs(chg) > 5 else ""
                parts.append(
                    f'<tr><td>{fn}</td>'
                    f'<td style="text-align:right">{sz:,}</td>'
                    f'<td style="text-align:right">{prev_str}</td>'
                    f'<td style="text-align:right">{chg_str}{flag}</td></tr>'
                )
                total_size += sz
            parts.append(
                f'<tr style="background:#f0f0f0;"><td><b>合计</b></td>'
                f'<td style="text-align:right"><b>{total_size:,}</b></td>'
                f'<td></td><td></td></tr>'
            )
            parts.append('</table>')
        else:
            parts.append('<p>⚠️ 无元数据文件</p>')
        return "\n".join(parts)

    @staticmethod
    def _email_header(date_str: str) -> str:
        return (
            '<html><body style="font-family:-apple-system,'
            'BlinkMacSystemFont,\'Segoe UI\',Roboto,sans-serif;'
            'color:#333;max-width:900px;margin:0 auto;padding:20px;">'
            '<h1 style="color:#1a73e8;border-bottom:2px solid #1a73e8;'
            'padding-bottom:10px;">'
            f"📊 数据验证报告 "
            f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            "</h1>"
        )

    @staticmethod
    def _email_comparison_section(
        title: str, comp: dict, _date_str: str
    ) -> str:
        """Unified comparison section for email (HTML)."""
        parts = [f'<h2>{title}</h2>']

        ec = comp.get("exchange_counts", {})
        total_a = comp.get("total_source_a", 0)
        total_b = comp.get("total_source_b", 0)
        matched = comp.get("matched_count", 0)
        missing_count = comp.get("missing_count", 0)
        extra_count = comp.get("extra_count", 0)

        parts.append(
            f'<p>source_a: <b>{total_a}</b> 条, '
            f'source_b: <b>{total_b}</b> 条, '
            f'匹配: <b>{matched}</b> 条</p>'
        )

        # Contracts count table
        parts.append('<h3>contracts count</h3>')
        parts.append(
            '<table border="1" cellpadding="6" cellspacing="0"'
            ' style="border-collapse:collapse;">'
        )
        parts.append(
            '<tr style="background:#f5f5f5;">'
            '<th>交易所</th><th>source_a</th><th>source_b</th>'
            '<th>多余</th><th>缺漏</th></tr>'
        )
        for ex in sorted(ec.keys()):
            if ex == "CSI":
                continue
            d = ec[ex]
            color = "red" if d["missing"] > 0 else "green"
            parts.append(
                f'<tr><td><b>{ex}</b></td>'
                f'<td style="text-align:center">'
                f'{d["source_a"]}</td>'
                f'<td style="text-align:center">'
                f'{d["source_b"]}</td>'
                f'<td style="text-align:center;color:{color}">'
                f'{d["missing"]}</td>'
                f'<td style="text-align:center">'
                f'{d["extra"]}</td></tr>'
            )
        parts.append('</table>')

        # Missing / extra
        missing_records = comp.get("missing_records", [])
        extra_records = comp.get("extra_records", [])
        if missing_count:
            parts.append(
                f'<p>➕ 多余: {missing_count} 条 (source_a有/source_b无)</p>'
            )
            parts.append('<ul>')
            for rec in missing_records[:10]:
                code = rec.get("code", "?")
                close_v = rec.get("close", "—")
                parts.append(f'<li>❌ {code} close={close_v}</li>')
            if missing_count > 10:
                parts.append(f'<li>... 还有 {missing_count - 10} 条</li>')
            parts.append('</ul>')
        if extra_count:
            parts.append(
                f'<p>⚠️ 缺漏: {extra_count} 条 (source_b有/source_a无)</p>'
            )
            parts.append('<ul>')
            for rec in extra_records[:10]:
                code = rec.get("code", "?")
                parts.append(f'<li>➕ {code}</li>')
            parts.append('</ul>')

        # Field summary
        fs = comp.get("field_summary", {})
        if fs:
            parts.append('<h3>字段差异汇总</h3>')
            parts.append(
                '<table border="1" cellpadding="6" cellspacing="0"'
                ' style="border-collapse:collapse;">'
            )
            parts.append(
                '<tr style="background:#f5f5f5;">'
                '<th>字段</th><th>匹配</th><th>差异</th>'
                '<th>缺a</th><th>缺b</th></tr>'
            )
            for fname in _FULL_FIELDS:
                s = fs.get(fname, {})
                if (
                    s.get("diff", 0) == 0
                    and s.get("missing_a", 0) == 0
                    and s.get("missing_b", 0) == 0
                ):
                    continue
                color = "red" if s.get("diff", 0) > 0 else "green"
                parts.append(
                    f'<tr style="color:{color}"><td><b>{fname}</b></td>'
                    f'<td style="text-align:right">'
                    f'{s.get("matched", 0)}</td>'
                    f'<td style="text-align:right">'
                    f'{s.get("diff", 0)}</td>'
                    f'<td style="text-align:right">'
                    f'{s.get("missing_a", 0)}</td>'
                    f'<td style="text-align:right">'
                    f'{s.get("missing_b", 0)}</td></tr>'
                )
            parts.append('</table>')

        # Per-exchange per-field details
        fd = comp.get("field_diffs", {})
        if fd:
            parts.append('<h3>groupby ex, field</h3>')
            for ex in sorted(fd.keys()):
                if ex == "CSI":
                    continue
                ex_diffs = fd[ex]
                for field in _FULL_FIELDS:
                    s = ex_diffs.get(field, {})
                    if (
                        s.get("diff", 0) == 0
                        and s.get("missing_in_original", 0) == 0
                        and s.get("missing_in_new", 0) == 0
                    ):
                        continue
                    diff_count = s.get("diff", 0)
                    limit = None if diff_count < 10 else 10
                    samples = sorted(
                    s.get("sample_diffs", []),
                    key=lambda x: x.get("ratio", 0), reverse=True,
                )
                    missing_b = s.get("sample_missing_in_new", [])
                    missing_a = s.get("sample_missing_in_original", [])
                    parts.append(f'<p><b>{ex} {field}</b></p>')
                    
                    # if_basis: 差异极小 (<0.001) 时只显示条数, 不逐条列示
                    if field == "if_basis":
                        tiny_diffs = [
                            sd for sd in samples
                            if isinstance(sd.get('a-b'), (int, float))
                            and abs(sd['a-b']) < 0.001
                        ]
                        if (
                        tiny_diffs
                        and len(tiny_diffs) == len(samples)
                        and not missing_a
                        and not missing_b
                    ):
                            parts.append(
                                (
                                    f'<ul><li>{len(tiny_diffs)} 条差异均 '
                                    f'&lt; 0.001 (可忽略)</li></ul>'
                                )
                            )
                            continue

                    parts.append('<ul>')
                    for sd in samples[:limit]:
                        a_b = sd.get('a-b', '—')
                        ratio = sd.get('ratio', '-')
                        ratio_str = (
                        f" |a-b|/b={ratio:.6f}"
                        if isinstance(ratio, (int, float))
                        else ""
                    )
                        parts.append(
                            (
                                f'<li>{sd["code"]}: '
                                f'a={sd["original"]} '
                                f'b={sd["new"]} '
                                f'a-b={a_b}{ratio_str}</li>'
                            )
                        )
                    for sd in missing_b:
                        parts.append(
                            f'<li>{sd["code"]}: '
                            f'a={sd["original"]} b=—</li>'
                        )
                    for sd in missing_a:
                        parts.append(
                            f'<li>{sd["code"]}: a=— '
                            f'b={sd["new"]}</li>'
                        )
                    parts.append('</ul>')

        return "\n".join(parts)

    @staticmethod
    def _email_field_stats(stats: dict, _abnormal: dict) -> str:
        parts = ['<h2>📈 字段覆盖率统计</h2>']
        for ex in sorted(stats.keys()):
            if ex == "CSI":
                continue
            total = (stats[ex].get("code", {}) or {}).get("total", 0) or 0
            parts.append(f'<h3>{ex} ({total} 个合约)</h3>')
            parts.append(
                '<table border="1" cellpadding="6" cellspacing="0"'
                ' style="border-collapse:collapse;width:100%;">'
            )
            parts.append(
                '<tr style="background:#f5f5f5;">'
                '<th>字段</th><th>非空</th><th>缺失率</th>'
                '<th>异常空值</th></tr>'
            )
            for f in _FULL_FIELDS:
                s = stats[ex].get(f, {}) or {}
                nn = s.get("non_null", 0) or 0
                to = s.get("total", 0) or 0
                pct = s.get("null_pct", 0) or 0
                abn = s.get("abnormal_null", 0) or 0
                ok_rate = (to - abn) / to * 100 if to > 0 else 100
                icon = "✅" if abn == 0 else ("⚠️" if ok_rate >= 90 else "❌")
                parts.append(
                    f'<tr><td>{icon} {f}</td>'
                    f'<td style="text-align:center">{nn}/{to}</td>'
                    f'<td style="text-align:center">{pct}%</td>'
                    f'<td style="text-align:center">{abn}</td></tr>'
                )
            parts.append('</table>')
        return "\n".join(parts)

    @staticmethod
    def _email_footer() -> str:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return (
            f'<p style="color:#999;font-size:12px;margin-top:30px;">'
            f'报告生成: {now}'
            f'</p></body></html>'
        )


def main():
    """CLI entry point. Supports python -m data_sources.reporter."""
    parser = argparse.ArgumentParser(
        description="Data sources reporter"
    )
    parser.add_argument("date", nargs="?", default=None,
                        help="Trade date YYYYMMDD, default today")
    parser.add_argument("--sender", default=None,
                        help="Sender email address")
    parser.add_argument("--recipients", default=None,
                        help="Comma-separated recipient email addresses")
    parser.add_argument(
        "--skip-table-compare", action="store_true",
        help="Skip comparison with Wind original table (phase 2 only)",
    )
    parser.add_argument(
        "--skip-wind", action="store_true",
        help="Skip Wind WSS cross-validation (dev / no cpp_py)",
    )
    args = parser.parse_args()

    date_str = (
        args.date
        if args.date
        else datetime.now().strftime("%Y%m%d")
    )
    recipients = (
        args.recipients.split(",")
        if args.recipients
        else None
    )
    r = Reporter()
    r.generate_daily(
        date_str,
        skip_table_compare=args.skip_table_compare,
        skip_wind=args.skip_wind,
        sender=args.sender,
        email_recipients=recipients,
    )


if __name__ == "__main__":
    main()
