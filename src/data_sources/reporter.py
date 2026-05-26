#!/usr/bin/env python3
"""Reporter: all pipeline reporting in one place."""

import argparse
import json
import os
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import List, Optional

import logging

import pymysql
import requests
from pymysql.cursors import DictCursor

from data_sources.task import Task
from data_sources.verifier import DB_CONFIG, DB_CONFIG_ORIG, Verifier
from mxz_utils.logging_config import get_logger


_FULL_FIELDS = [
    "open", "high", "low", "close", "volume", "amt", "oi",
    "settle", "maxup", "maxdown", "if_basis", "long_margin", "short_margin",
    "minoq", "maxoq",
]

_FEISHU_WEBHOOK = (
    "https://open.feishu.cn/open-apis/bot/v2/hook/"
    "7f1c49ef-6e6b-4c19-8152-8e25dfb8d688"
)


class Reporter:
    """All pipeline reporting methods."""

    _DEFAULT_SENDER = "mxz@wendao.fund"
    _DEFAULT_RECIPIENTS = [
        "fisher@wendao.fund",
        "chendingzhong@wendao.fund",
    ]

    def __init__(self, logger=None):
        self.logger = logger or get_logger(
            "data_sources.reporter", logging.DEBUG, "./logs", "reporter",
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
        email: bool = False,
        sender: Optional[str] = None,
        email_recipients: Optional[list[str]] = None,
    ) -> None:
        """Generate daily data verification report.

        Args:
            date_str: 交易日 YYYYMMDD
            skip_table_compare: True=阶段二，跳过两表对比，仅做 Wind API 交叉验证
            email: 是否同时发送邮件

        一次计算，两个渠道：
          1. 飞书（默认）
          2. 邮件（email=True 时同时发送）
        """
        v = Verifier(self.logger)
        stats = v.get_field_stats(date_str)
        abnormal = v.get_abnormal_nulls(date_str)

        feishu_sections = [
            self._build_file_size_section(date_str),
            self._build_field_stats_section(stats, abnormal),
        ]

        ec = {}
        fd = {}
        comp = {}

        if not skip_table_compare:
            # Phase 1: 两表对比 + Wind API
            comp = v.compare_all(target_date=date_str)
            ec = comp.get("exchange_counts", {})
            fd = comp.get("field_diffs", {})
            feishu_sections.append(
                self._build_comparison_section(comp, date_str)
            )
        else:
            # Phase 2: 仅 Wind API 交叉验证
            self.logger.info("skip_table_compare=True, skipping table comparison")

        # Always run Wind API cross-validation
        wind_comp = v.compare_with_wind_api(date_str)
        if wind_comp.get("wind_available"):
            feishu_sections.append(
                self._build_wind_comparison_section(wind_comp)
            )
            if email:
                self.logger.info(
                    "Wind cross-validation: %d/%d matched, %d diffs",
                    wind_comp.get("matched_contracts", 0),
                    wind_comp.get("total_contracts", 0),
                    len(wind_comp.get("contract_diffs", [])),
                )

        self._send_feishu_markdown(
            f"📊 数据验证报告 {date_str}",
            "\n\n---\n\n".join(feishu_sections),
        )

        if email:
            recipients = email_recipients or self._DEFAULT_RECIPIENTS
            sender_addr = sender or self._DEFAULT_SENDER
            self._smtp_send(
                date_str, stats, abnormal, ec, fd, comp,
                sender=sender_addr, recipients=recipients,
                wind_comp=wind_comp,
            )

        self.logger.info("Report for %s sent.", date_str)

    def send_email(
        self, date_str: str,
        sender: Optional[str] = None,
        recipients: Optional[list[str]] = None,
    ) -> None:
        """快捷方式：只发邮件。"""
        self.generate_daily(
            date_str, email=True,
            sender=sender, email_recipients=recipients,
        )

    def _smtp_send(self, date_str, stats, abnormal, ec, fd, comp,
                   sender=None, recipients=None,
                   wind_comp: Optional[dict] = None):
        """发送邮件（SMTP），使用已计算好的数据。"""
        password = os.environ.get("SMTP_PASSWORD")
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
        # Phase 1: table comparison
        if ec or comp:
            html_parts.append(self._email_coverage(ec, comp, date_str))
            html_parts.append(self._email_field_diffs(fd))
        # Always: field stats
        html_parts.append(self._email_field_stats(stats, abnormal))
        # Always: Wind cross-validation
        if wind_comp and wind_comp.get("wind_available"):
            html_parts.append(
                self._email_wind_section(wind_comp)
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
        with smtplib.SMTP_SSL("smtp.exmail.qq.com", 465,
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
    def _build_file_size_section(date_str: str) -> str:
        """Section 1: file size changes from metadata."""
        metadata_file = Path("./data/raw/.metadata.jsonl")
        lines = [f"**交易日期**: {date_str}", ""]
        if metadata_file.exists():
            lines.append("**数据文件尺寸变化**:")
            lines.append("| 文件 | 本次(bytes) | 上次(bytes) | 变化率 |")
            lines.append("| :--- | ---: | ---: | ---: |")
            total_size = 0
            # 去重：每个文件只取最后一次记录
            records: dict[str, dict] = {}
            with open(metadata_file) as f:
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
                lines.append("异常空值明细 (按金额降序, 最多10条):")
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
    def _build_comparison_section(comp: dict, date_str: str) -> str:
        """Section 3: table comparison differences."""
        lines = [
            "**合约覆盖率: t_futures_info vs t_futures_info_exchange "
            "(个合约)**",
            "",
        ]
        try:
            ec = comp.get("exchange_counts", {})
            lines.append("| 交易所 | 原表 | 新表 | 缺漏 | 多余 |")
            lines.append("| :--- | ---: | ---: | ---: | ---: |")
            for ex in sorted(ec.keys()):
                if ex == "CSI":
                    continue
                lines.append(
                    f"| {ex} | {ec[ex]['original']} | {ec[ex]['new']} | "
                    f"{ec[ex]['missing_in_new']} | {ec[ex]['extra_in_new']} |"
                )

            original_table = "t_futures_info"
            new_table = "t_futures_info_exchange"
            missing_records = comp.get("missing_records", [])
            extra_records = comp.get("extra_records", [])
            if missing_records:
                lines.append("")
                lines.append("缺漏明细 (原表有/新表无, 最多10条):")
                for rec in _fetch_records_details(
                    missing_records[:10], original_table, date_str
                ):
                    lines.append(f"  ❌ {_fmt_rec_line(rec)}")
            if extra_records:
                lines.append("")
                lines.append("多余明细 (新表有/原表无, 最多10条):")
                for rec in _fetch_records_details(
                    extra_records[:10], new_table, date_str
                ):
                    lines.append(f"  ➕ {_fmt_rec_line(rec)}")
            lines.append("")

            lines.append("**字段差异明细**:")
            lines.append("")

            has_diff = False
            fd = comp.get("field_diffs", {})
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
                    lines.append(f"  **{ex} {field}**:")
                    limit = None if s["diff"] < 10 else 10
                    for sd in s["sample_diffs"][:limit]:
                        diff = round(sd['original'] - sd['new'], 4)
                        lines.append(
                            f"    {sd['code']}:"
                            f" 原={sd['original']}"
                            f" 新={sd['new']}"
                            f" 差={diff}"
                        )
                    for sd in s.get("sample_missing_in_new", []):
                        lines.append(
                            f"    {sd['code']}:"
                            f" 原={sd['original']} 新=— 差=—"
                        )
                    for sd in s.get("sample_missing_in_original", []):
                        lines.append(
                            f"    {sd['code']}:"
                            f" 原=— 新={sd['new']} 差=—"
                        )

            if not has_diff:
                lines.append("✅ 字段无差异")
        except Exception as e:
            lines.append(f"**对比失败**: {e}")

        return "\n".join(lines)

    @staticmethod
    def _build_wind_comparison_section(wind_comp: dict) -> str:
        """Wind API cross-validation section for Feishu."""
        if not wind_comp.get("wind_available"):
            return "⚠️ Wind API 不可用，跳过交叉验证"

        lines = []
        lines.append(
            f"**Wind API 交叉验证** "
            f"({wind_comp['matched_contracts']}/"
            f"{wind_comp['total_contracts']} 合约)"
        )
        lines.append("")

        field_stats = wind_comp.get("field_stats", {})
        has_diff = any(s["diff"] > 0 for s in field_stats.values())

        if not has_diff and not wind_comp.get("wind_unavailable"):
            lines.append("✅ 全部字段完全一致")
            return "\n".join(lines)

        lines.append("**分字段差异统计**:")
        lines.append("")
        lines.append(
            "| 字段 | 匹配 | 差异 | 最大偏差 | 均值偏差 |"
        )
        lines.append(
            "| :--- | ---: | ---: | ---: | ---: |"
        )
        for fname, s in field_stats.items():
            avg_dev = (
                s["sum_deviation"] / s["diff"]
                if s["diff"] > 0 else 0.0
            )
            icon = "🔴" if s["diff"] > 0 else "✅"
            lines.append(
                f"| {icon} {fname} | {s['matched']} | {s['diff']} |"
                f" {s['max_deviation']:.2f}% |"
                f" {avg_dev:.2f}% |"
            )

        contract_diffs = wind_comp.get("contract_diffs", [])
        if contract_diffs:
            lines.append("")
            lines.append("**差异明细** (最多 20 条):")
            for d in contract_diffs[:20]:
                lines.append(
                    f"  → {d['code']}.{d['field']}: "
                    f"ours={d['ours']} wind={d['wind']}"
                    f" 偏差 {d['deviation_pct']:.2f}%"
                )
            if len(contract_diffs) > 20:
                lines.append(f"  ... 还有 {len(contract_diffs) - 20} 条")

        unavailable = wind_comp.get("wind_unavailable", [])
        if unavailable:
            lines.append("")
            lines.append(
                f"**Wind 无数据** ({len(unavailable)} 个):"
            )
            for c in unavailable[:10]:
                lines.append(f"  ⚠️ {c}")
            if len(unavailable) > 10:
                lines.append(f"  ... 还有 {len(unavailable) - 10} 个")

        return "\n".join(lines)

    @staticmethod
    def _email_file_size_section(date_str: str) -> str:
        """File size section for email, matching Feishu format."""
        metadata_file = Path("./data/raw/.metadata.jsonl")
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
            with open(metadata_file) as f:
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
    def _email_coverage(ec: dict, comp: dict, date_str: str) -> str:
        parts = ['<h2>📋 合约覆盖率</h2>']
        parts.append(
            '<table border="1" cellpadding="8" cellspacing="0"'
            ' style="border-collapse:collapse;width:100%;">'
        )
        parts.append(
            '<tr style="background:#f5f5f5;">'
            '<th>交易所</th><th>原表</th><th>新表</th>'
            '<th>缺漏</th><th>多余</th></tr>'
        )
        for ex in sorted(ec.keys()):
            if ex == "CSI":
                continue
            c = ec[ex]
            color = "red" if c["missing_in_new"] > 0 else "green"
            parts.append(
                f'<tr><td><b>{ex}</b></td>'
                f'<td style="text-align:center">{c["original"]}</td>'
                f'<td style="text-align:center">{c["new"]}</td>'
                f'<td style="text-align:center;color:{color}">'
                f'{c["missing_in_new"]}</td>'
                f'<td style="text-align:center">'
                f'{c["extra_in_new"]}</td></tr>'
            )
        parts.append('</table>')

        missing_records = comp.get("missing_records", [])
        extra_records = comp.get("extra_records", [])
        if missing_records:
            parts.append(
                '<h3>⚠️ 缺漏明细 (原表有/新表无, 最多10条)</h3>'
            )
            parts.append(
                '<table border="1" cellpadding="4" cellspacing="0"'
                ' style="border-collapse:collapse;font-size:12px;">'
            )
            parts.append(
                '<tr style="background:#f5f5f5;"><th>合约</th>' +
                ''.join(f'<th>{f}</th>' for f in _FULL_FIELDS) +
                '</tr>'
            )
            for rec in _fetch_records_details(
                missing_records[:10], "t_futures_info", date_str
            ):
                parts.append(
                    '<tr><td>'
                    + ((rec.get("code", "") or ""))
                    + '</td>' + ''.join(_td(rec.get(f))
                                        for f in _FULL_FIELDS)
                    + '</tr>'
                )
            parts.append('</table>')

        if extra_records:
            parts.append(
                '<h3>➕ 多余明细 (新表有/原表无, 最多10条)</h3>'
            )
            parts.append(
                '<table border="1" cellpadding="4" cellspacing="0"'
                ' style="border-collapse:collapse;font-size:12px;">'
            )
            parts.append(
                '<tr style="background:#f5f5f5;"><th>合约</th>' +
                ''.join(f'<th>{f}</th>' for f in _FULL_FIELDS) +
                '</tr>'
            )
            for rec in _fetch_records_details(
                extra_records[:10], "t_futures_info_exchange", date_str
            ):
                parts.append(
                    '<tr><td>'
                    + ((rec.get("code", "") or ""))
                    + '</td>' + ''.join(_td(rec.get(f))
                                        for f in _FULL_FIELDS)
                    + '</tr>'
                )
            parts.append('</table>')
        return "\n".join(parts)

    @staticmethod
    def _email_field_stats(stats: dict, abnormal: dict) -> str:
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

            if ex in abnormal and abnormal[ex]:
                parts.append('<p><b>异常空值明细:</b></p>')
                parts.append(
                    '<table border="1" cellpadding="4" cellspacing="0"'
                    ' style="border-collapse:collapse;font-size:13px;">'
                )
                parts.append(
                    '<tr style="background:#f5f5f5;">'
                    '<th>合约</th><th>空值字段</th><th>amt</th>'
                    '<th>判定</th></tr>'
                )
                for rec in abnormal[ex]:
                    code = (
                        (rec.get("code", "") or "").split(".")[0] or "?"
                    )
                    nulls = rec.get("_null_fields", []) or []
                    cls = rec.get("_classification", "") or ""
                    amt = rec.get("amt")
                    amt_str = f"{amt:>,.0f}" if amt else "—"
                    parts.append(
                        f'<tr><td>{code}</td>'
                        f'<td>{", ".join(nulls)}</td>'
                        f'<td style="text-align:right">{amt_str}</td>'
                        f'<td>{cls}</td></tr>'
                    )
                parts.append('</table>')
        return "\n".join(parts)

    @staticmethod
    def _email_field_diffs(fd: dict) -> str:
        parts = ['<h2>🔍 字段差异明细</h2>']
        has_diff = False
        for ex in sorted(fd.keys()):
            if ex == "CSI":
                continue
            ex_d = fd[ex]
            rows = []
            for f in _FULL_FIELDS:
                s = ex_d.get(f, {}) or {}
                if (
                    s.get("diff", 0) == 0
                    and s.get("missing_in_original", 0) == 0
                    and s.get("missing_in_new", 0) == 0
                ):
                    continue
                has_diff = True
                samples = []
                for sd in s.get("sample_diffs", [])[:10]:
                    diff = round(sd['original'] - sd['new'], 4)
                    samples.append(f"{sd['code']}: 旧={sd['original']} 新={sd['new']} 差={diff}")
                for sd in s.get("sample_missing_in_new", []):
                    samples.append(f"{sd['code']}: 旧={sd['original']} 新=— 差=—")
                for sd in s.get("sample_missing_in_original", []):
                    samples.append(f"{sd['code']}: 旧=— 新={sd['new']} 差=—")
                rows.append((f, samples))
            if not rows:
                continue
            parts.append(f'<h3>{ex}</h3>')
            parts.append(
                '<table border="1" cellpadding="6" cellspacing="0"'
                ' style="border-collapse:collapse;">'
            )
            parts.append(
                '<tr style="background:#f5f5f5;">'
                '<th>字段</th><th>差异明细</th></tr>'
            )
            for fname, samples in rows:
                sample_text = "<br>".join(samples) if samples else "—"
                parts.append(
                    f'<tr><td><b>{fname}</b></td>'
                    f'<td style="font-size:12px;color:#666">'
                    f'{sample_text}</td></tr>'
                )
            parts.append('</table>')
        if not has_diff:
            parts.append('<p>✅ 全部字段无差异</p>')
        return "\n".join(parts)

    @staticmethod
    def _email_wind_section(wind_comp: dict) -> str:
        """Wind API 交叉验证邮件章节 (HTML)."""
        parts = ['<h2>🌐 Wind API 交叉验证</h2>']
        mc = wind_comp.get("matched_contracts", 0)
        tc = wind_comp.get("total_contracts", 0)
        parts.append(
            f'<p>匹配: {mc}/{tc} 合约</p>'
        )

        field_stats = wind_comp.get("field_stats", {})
        has_diff = any(s["diff"] > 0 for s in field_stats.values())
        if not has_diff and not wind_comp.get("wind_unavailable"):
            parts.append('<p>✅ 全部字段完全一致</p>')
            return "\n".join(parts)

        parts.append('<h3>分字段差异统计</h3>')
        parts.append(
            '<table border="1" cellpadding="6" cellspacing="0"'
            ' style="border-collapse:collapse;">'
        )
        parts.append(
            '<tr style="background:#f5f5f5;">'
            '<th>字段</th><th>匹配</th><th>差异</th>'
            '<th>最大偏差</th><th>均值偏差</th></tr>'
        )
        for fname, s in field_stats.items():
            avg_dev = (
                s["sum_deviation"] / s["diff"]
                if s["diff"] > 0 else 0.0
            )
            icon = "🔴" if s["diff"] > 0 else "✅"
            parts.append(
                f'<tr><td>{icon} {fname}</td>'
                f'<td style="text-align:right">{s["matched"]}</td>'
                f'<td style="text-align:right">{s["diff"]}</td>'
                f'<td style="text-align:right">{s["max_deviation"]:.2f}%</td>'
                f'<td style="text-align:right">{avg_dev:.2f}%</td></tr>'
            )
        parts.append('</table>')

        contract_diffs = wind_comp.get("contract_diffs", [])
        if contract_diffs:
            parts.append('<h3>差异明细 (最多 20 条)</h3>')
            parts.append('<ul>')
            for d in contract_diffs[:20]:
                parts.append(
                    f'<li>{d["code"]}.{d["field"]}: '
                    f'ours={d["ours"]} wind={d["wind"]} '
                    f'偏差 {d["deviation_pct"]:.2f}%</li>'
                )
            if len(contract_diffs) > 20:
                parts.append(
                    f'<li>... 还有 {len(contract_diffs) - 20} 条</li>'
                )
            parts.append('</ul>')

        unavailable = wind_comp.get("wind_unavailable", [])
        if unavailable:
            parts.append(
                f'<p>Wind 无数据 ({len(unavailable)} 个)</p>'
            )
            parts.append('<ul>')
            for c in unavailable[:10]:
                parts.append(f'<li>⚠️ {c}</li>')
            if len(unavailable) > 10:
                parts.append(
                    f'<li>... 还有 {len(unavailable) - 10} 个</li>'
                )
            parts.append('</ul>')

        return "\n".join(parts)

    @staticmethod
    def _email_footer() -> str:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return (
            f'<p style="color:#999;font-size:12px;margin-top:30px;">'
            f'报告生成: {now}'
            f'</p></body></html>'
        )


def _fetch_records_details(records: list, table: str,
                           date_str: str) -> list:
    """查询记录的所有字段值。"""
    cfg = DB_CONFIG if table == "t_futures_info_exchange" else DB_CONFIG_ORIG
    conn = pymysql.connect(**cfg, cursorclass=DictCursor)
    try:
        with conn.cursor() as cur:
            dt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            cols = ", ".join(["code"] + _FULL_FIELDS)
            results = []
            for rec in records:
                code = rec["code"]
                cur.execute(
                    f"SELECT {cols} FROM future_cn.{table} "
                    f"WHERE code = %s AND date = %s",
                    (code, dt),
                )
                row = cur.fetchone()
                if row:
                    results.append(dict(row))
            return results
    finally:
        conn.close()


def _fmt_rec_line(rec: dict) -> str:
    """格式化一条记录的所有字段为一行（用于飞书 Markdown）。"""
    def v(f):
        val = rec.get(f)
        if val is None:
            return "—"
        try:
            fv = float(val)
            if f == "amt":
                return f"{fv:,.0f}"
            if f in ("volume", "oi"):
                return f"{int(fv)}"
            return f"{fv:g}"
        except Exception:
            return str(val)
    parts = [rec.get("code", "?")]
    for f in _FULL_FIELDS:
        parts.append(f"{f}={v(f)}")
    return " | ".join(parts)


def _td(val) -> str:
    """格式化表格单元格为 HTML <td>。"""
    if val is None:
        return '<td style="text-align:center">—</td>'
    try:
        fv = float(val)
        if abs(fv) >= 1_000_000:
            return f'<td style="text-align:right">{fv:,.0f}</td>'
        if fv == int(fv):
            return f'<td style="text-align:right">{int(fv)}</td>'
        return f'<td style="text-align:right">{fv:g}</td>'
    except Exception:
        return f'<td>{val}</td>'


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
    send_email = bool(args.sender and recipients)

    r = Reporter()
    r.generate_daily(
        date_str,
        skip_table_compare=args.skip_table_compare,
        email=send_email,
        sender=args.sender,
        email_recipients=recipients,
    )


if __name__ == "__main__":
    main()
