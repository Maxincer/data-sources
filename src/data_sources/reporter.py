#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Reporter: send pipeline statistics as Feishu cards.
Provides helper functions and the Reporter class for Fetcher.
"""

from typing import List

from data_sources.models import Task


class Reporter:
    """File-size change reporter used by Fetcher."""

    def __init__(self, logger):
        self.logger = logger

    def task_report(self, tasks: List[Task], trade_date: str) -> None:
        """Send a Markdown table of file size changes."""
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





# =====================================================================
# Comprehensive daily verification report
# =====================================================================

FEISHU_WEBHOOK = (
    "https://open.feishu.cn/open-apis/bot/v2/hook/"
    "7f1c49ef-6e6b-4c19-8152-8e25dfb8d688"
)


def _send_feishu_markdown(title: str, content: str):
    """Send a markdown message to the configured Feishu webhook."""
    import requests

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
        resp = requests.post(FEISHU_WEBHOOK, json=payload, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"[WARN] Failed to send Feishu message: {e}")


def generate_daily_report(date_str: str):
    """
    Generate complete daily data verification report and send to Feishu.

    Three sections:
    1. File size tracking from Fetcher metadata
    2. Field valid/missing stats for t_futures_info_exchange
    3. Field comparison differences with t_futures_info
    """
    import json
    from pathlib import Path
    from data_sources.verifier import Verifier

    class ReportLogger:
        def info(self, msg): pass
        def warning(self, msg): pass
        def error(self, msg): pass
        def alert(self, msg): pass

    v = Verifier(ReportLogger())

    sections = []

    # ---- Section 1: File size tracking ----
    metadata_file = Path("./data/raw/.metadata.jsonl")
    file_lines = [f"**交易日期**: {date_str}", ""]
    if metadata_file.exists():
        file_lines.append("**数据文件尺寸变化**:")
        file_lines.append("| 文件 | 本次(bytes) | 上次(bytes) | 变化率 |")
        file_lines.append("| :--- | ---: | ---: | ---: |")
        total_size = 0
        with open(metadata_file) as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    if date_str in rec.get("local_filename", ""):
                        fn = rec["local_filename"]
                        sz = rec.get("file_size_bytes", 0)
                        prev = rec.get("previous_size_bytes")
                        chg = rec.get("size_change_percent")
                        prev_str = f"{prev:,}" if prev else "N/A"
                        chg_str = f"{chg:+.2f}%" if chg else "N/A"
                        alert = " ⚠️" if chg and abs(chg) > 5 else ""
                        file_lines.append(f"| {fn} | {sz:,} | {prev_str} | {chg_str}{alert} |")
                        total_size += sz
                except Exception:
                    pass
        file_lines.append(f"| **合计** | **{total_size:,}** | | |")
    else:
        file_lines.append("⚠️ 无元数据文件")
    sections.append("\n".join(file_lines))

    # ---- Section 2: Field stats ----
    stats = v.get_field_stats(date_str)
    abnormal = v.get_abnormal_nulls(date_str)
    stats_lines = ["**字段覆盖率统计** (t_futures_info_exchange):", ""]
    for ex in sorted(stats.keys()):
        if ex == "CSI":
            continue
        stats_lines.append(f"**{ex}** ({stats[ex].get('code', {}).get('total', 0)} 条):")
        stats_lines.append("| 字段 | 非空 | 缺失率 | 异常空值 |")
        stats_lines.append("| :--- | ---: | ---: | ---: |")
        for field in ["open","high","low","close","volume","amt","oi",
                       "settle","maxup","maxdown","long_margin","short_margin"]:
            s = stats[ex].get(field, {})
            total = s.get("total", 0)
            nn = s.get("non_null", 0)
            pct = s.get("null_pct", 0)
            abn = s.get("abnormal_null", 0)
            icon = "✅" if pct == 0 else ("🟡" if pct < 50 else "🔴")
            stats_lines.append(f"| {icon} {field} | {nn}/{total} | {pct}% | {abn} |")

        # 异常空值明细
        if ex in abnormal and abnormal[ex]:
            stats_lines.append("")
            stats_lines.append("异常空值明细 (按金额降序, 最多10条):")
            stats_lines.append("| 合约 | 空值字段 | amt | 判定 |")
            stats_lines.append("| :--- | :--- | ---: | :--- |")
            for rec in abnormal[ex]:
                code = rec.get("code", "")
                parts = code.split(".")[0] if code else "?"
                nulls = rec.get("_null_fields", [])
                old_nulls = rec.get("_old_null", set())
                classification = rec.get("_classification", "")
                tags = []
                for f in nulls:
                    if f in old_nulls:
                        tags.append(f"{f}(旧表同)")
                    else:
                        tags.append(f"{f}")
                null_str = ", ".join(tags)
                amt = rec.get("amt")
                amt_str = f"{amt:>,.0f}" if amt else "—"
                stats_lines.append(f"| {parts} | {null_str} | {amt_str} | {classification} |")

        stats_lines.append("")
    sections.append("\n".join(stats_lines))

    # ---- Section 3: Comparison differences ----
    try:
        comp = v.compare_all(target_date=date_str)
        comp_lines = ["**合约覆盖率: t_futures_info vs t_futures_info_exchange (个合约)**", ""]
        comp_lines.append("| 交易所 | 原表 | 新表 | 缺漏 | 多余 |")
        comp_lines.append("| :--- | ---: | ---: | ---: | ---: |")
        for ex, ec in sorted(comp.get("exchange_counts", {}).items()):
            if ex == "CSI":
                continue
            comp_lines.append(
                f"| {ex} | {ec['original']} | {ec['new']} | "
                f"{ec['missing_in_new']} | {ec['extra_in_new']} |"
            )

        # 缺漏/多余明细
        missing_records = comp.get("missing_records", [])
        extra_records = comp.get("extra_records", [])
        if missing_records:
            comp_lines.append("")
            comp_lines.append(f"缺漏明细 (原表有/新表无):")
            for rec in missing_records:
                comp_lines.append(f"  ❌ {rec['code']} @ {rec['date']}")
        if extra_records:
            comp_lines.append("")
            comp_lines.append(f"多余明细 (新表有/原表无):")
            for rec in extra_records:
                comp_lines.append(f"  ➕ {rec['code']} @ {rec['date']}")
        comp_lines.append("")

        has_field_diff = False
        for ex in sorted(comp.get("field_diffs", {}).keys()):
            if ex == "CSI":
                continue
            ex_diffs = comp["field_diffs"][ex]
            has_any = any(
                s["diff"] > 0 or s["missing_in_original"] > 0
                or s["missing_in_new"] > 0
                for s in ex_diffs.values()
            )
            if not has_any:
                continue
            has_field_diff = True
            for field in ["open","high","low","close","volume","amt","oi",
                           "settle","maxup","maxdown","long_margin","short_margin"]:
                s = ex_diffs.get(field, {})
                if s["diff"] == 0 and s["missing_in_original"] == 0 and s["missing_in_new"] == 0:
                    continue
                meta = f"{ex} {field}: 差{s['diff']}"
                if s["missing_in_original"]: meta += f" 旧缺{s['missing_in_original']}"
                if s["missing_in_new"]:     meta += f" 新缺{s['missing_in_new']}"
                if s.get("abnormal_missing_new", 0):
                    meta += f" 缺异{s['abnormal_missing_new']}"
                if s["max_deviation_pct"] > 0.001: meta += f" 最大{s['max_deviation_pct']}%"
                comp_lines.append(f"  {meta}")
                # 差异<10条全列, ≥10条列前10
                limit = None if s["diff"] < 10 else 10
                for sd in s["sample_diffs"][:limit]:
                    comp_lines.append(f"    {sd['code']}: 原={sd['original']} 新={sd['new']}")

        if not has_field_diff:
            comp_lines.append("✅ 字段无差异")

        sections.append("\n".join(comp_lines))
    except Exception as e:
        sections.append(f"**对比失败**: {e}")

    full_report = "\n\n---\n\n".join(sections)
    _send_feishu_markdown(f"📊 数据验证报告 {date_str}", full_report)
    print(f"Report for {date_str} sent.")


def send_email_report(date_str: str, recipients: list[str] = None):
    """
    通过企业微信邮箱发送数据验证报告。

    SMTP_PASSWORD 从环境变量读取，否则跳过发送。
    """
    import os
    import smtplib
    import ssl
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    password = os.environ.get("SMTP_PASSWORD")
    if not password:
        print("⚠️ SMTP_PASSWORD 未设置，跳过邮件发送")
        return

    if recipients is None:
        recipients = ["wanghaibo@wendao.fund", "chendingzhong@wendao.fund"]

    from data_sources.verifier import Verifier

    sender = "mxz@wendao.fund"

    class _FakeLogger:
        def info(self, *a, **kw): pass
        def warning(self, *a, **kw): pass
        def error(self, *a, **kw): pass

    v = Verifier(_FakeLogger())

    # 生成报告内容
    comp = v.compare_all(target_date=date_str)
    ec = comp.get("exchange_counts", {})
    fd = comp.get("field_diffs", {})
    stats = v.get_field_stats(date_str)
    abnormal = v.get_abnormal_nulls(date_str)

    FIELDS = ["open","high","low","close","volume","amt","oi",
               "settle","maxup","maxdown","long_margin","short_margin"]

    html_parts = []
    html_parts.append(f"""
<html><body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #333; max-width: 900px; margin: 0 auto; padding: 20px;">
<h1 style="color: #1a73e8; border-bottom: 2px solid #1a73e8; padding-bottom: 10px;">📊 数据验证报告 {date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}</h1>
""")

    # 合约覆盖率
    html_parts.append('<h2>📋 合约覆盖率</h2>')
    html_parts.append('<table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%;">')
    html_parts.append('<tr style="background: #f5f5f5;"><th>交易所</th><th>原表</th><th>新表</th><th>缺漏</th><th>多余</th></tr>')
    for ex in sorted(ec.keys()):
        if ex == "CSI": continue
        c = ec[ex]
        html_parts.append(
            f'<tr><td><b>{ex}</b></td><td style="text-align:center">{c["original"]}</td>'
            f'<td style="text-align:center">{c["new"]}</td>'
            f'<td style="text-align:center;color:{"red" if c["missing_in_new"]>0 else "green"}">{c["missing_in_new"]}</td>'
            f'<td style="text-align:center">{c["extra_in_new"]}</td></tr>')
    html_parts.append('</table>')

    # 缺漏/多余明细
    missing_records = comp.get("missing_records", [])
    extra_records = comp.get("extra_records", [])
    if missing_records:
        html_parts.append('<h3>⚠️ 缺漏明细 (原表有/新表无)</h3>')
        html_parts.append('<table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse;">')
        html_parts.append('<tr style="background:#f5f5f5;"><th>合约</th><th>日期</th></tr>')
        for rec in missing_records:
            html_parts.append(f'<tr><td>{rec["code"]}</td><td>{rec["date"]}</td></tr>')
        html_parts.append('</table>')
    if extra_records:
        html_parts.append('<h3>➕ 多余明细 (新表有/原表无)</h3>')
        html_parts.append('<table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse;">')
        html_parts.append('<tr style="background:#f5f5f5;"><th>合约</th><th>日期</th></tr>')
        for rec in extra_records:
            html_parts.append(f'<tr><td>{rec["code"]}</td><td>{rec["date"]}</td></tr>')
        html_parts.append('</table>')

    # 字段覆盖率
    html_parts.append('<h2>📈 字段覆盖率统计</h2>')
    for ex in sorted(stats.keys()):
        if ex == "CSI": continue
        total = (stats[ex].get("code", {}) or {}).get("total", 0) or 0
        html_parts.append(f'<h3>{ex} ({total} 个合约)</h3>')
        html_parts.append('<table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse; width: 100%;">')
        html_parts.append('<tr style="background:#f5f5f5;"><th>字段</th><th>非空</th><th>缺失率</th><th>异常空值</th></tr>')
        for f in FIELDS:
            s = stats[ex].get(f, {}) or {}
            nn = s.get("non_null",0) or 0
            to = s.get("total",0) or 0
            pct = s.get("null_pct",0) or 0
            abn = s.get("abnormal_null",0) or 0
            icon = "✅" if pct==0 else ("⚠️" if pct<10 else "❌")
            html_parts.append(f'<tr><td>{icon} {f}</td><td style="text-align:center">{nn}/{to}</td>'
                              f'<td style="text-align:center">{pct}%</td>'
                              f'<td style="text-align:center">{abn}</td></tr>')
        html_parts.append('</table>')

        # 异常空值明细
        if ex in abnormal and abnormal[ex]:
            html_parts.append('<p><b>异常空值明细:</b></p>')
            html_parts.append('<table border="1" cellpadding="4" cellspacing="0" style="border-collapse: collapse; font-size: 13px;">')
            html_parts.append('<tr style="background:#f5f5f5;"><th>合约</th><th>空值字段</th><th>amt</th><th>判定</th></tr>')
            for rec in abnormal[ex]:
                code = (rec.get("code","") or "").split(".")[0] or "?"
                nulls = rec.get("_null_fields",[]) or []
                classification = rec.get("_classification","") or ""
                amt = rec.get("amt")
                amt_str = f"{amt:>,.0f}" if amt else "—"
                html_parts.append(f'<tr><td>{code}</td><td>{", ".join(nulls)}</td>'
                                  f'<td style="text-align:right">{amt_str}</td>'
                                  f'<td>{classification}</td></tr>')
            html_parts.append('</table>')

    # 字段差异
    html_parts.append('<h2>🔍 字段差异明细</h2>')
    has_diff = False
    for ex in sorted(fd.keys()):
        if ex == "CSI": continue
        ex_d = fd[ex]
        rows = []
        for f in FIELDS:
            s = ex_d.get(f, {}) or {}
            if s.get("diff",0)==0 and s.get("missing_in_original",0)==0 and s.get("missing_in_new",0)==0:
                continue
            has_diff = True
            meta = f"diff={s['diff']}"
            if s.get("missing_in_original"): meta += f" 旧缺{s['missing_in_original']}"
            if s.get("missing_in_new"): meta += f" 新缺{s['missing_in_new']}"
            if s.get("max_deviation_pct",0) > 0.001: meta += f" max={s['max_deviation_pct']}%"
            samples = []
            for sd in s.get("sample_diffs",[])[:3]:
                samples.append(f"{sd['code']}: 旧={sd['original']} 新={sd['new']}")
            rows.append((f, meta, samples))
        if not rows: continue
        html_parts.append(f'<h3>{ex}</h3>')
        html_parts.append('<table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse;">')
        html_parts.append('<tr style="background:#f5f5f5;"><th>字段</th><th>差异</th><th>示例</th></tr>')
        for fname, meta, samples in rows:
            sample_text = "<br>".join(samples) if samples else "—"
            html_parts.append(f'<tr><td><b>{fname}</b></td><td>{meta}</td><td style="font-size:12px;color:#666">{sample_text}</td></tr>')
        html_parts.append('</table>')
    if not has_diff:
        html_parts.append('<p>✅ 全部字段无差异</p>')

    from datetime import datetime
    html_parts.append(f'<p style="color:#999;font-size:12px;margin-top:30px;">报告生成: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>')
    html_parts.append('</body></html>')

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"📊 期货数据验证报告 {date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText("\n".join(html_parts), "html", "utf-8"))

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.exmail.qq.com", 465, context=ctx) as server:
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())
    print(f"✅ 邮件报告已发送至: {', '.join(recipients)}")


if __name__ == "__main__":
    import sys
    date_str = sys.argv[1] if len(sys.argv) > 1 else \
        __import__("datetime").datetime.now().strftime("%Y%m%d")
    generate_daily_report(date_str)


def fetcher_card(logger, trade_date: str, task_results: list) -> None:
    """Send a Feishu card summarizing fetcher results."""
    success = [
        r for r in task_results
        if r.get("success") and not r.get("no_data")
    ]
    no_data = [r for r in task_results if r.get("no_data")]
    failed = [r for r in task_results if not r.get("success")]

    elements = [
        {"tag": "div", "text": {
            "tag": "lark_md",
            "content": (
                f"**交易日期**: {trade_date}\n"
                f"**成功**: {len(success)}"
                f"  |  **无数据**: {len(no_data)}"
                f"  |  **失败**: {len(failed)}"
            )
        }},
        {"tag": "hr"},
    ]

    if success:
        lines = "\n".join([f"✅ {r['exchange']:10s} {r.get('description', '')}"
                          for r in success[:20]])
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"**成功下载**:\n{lines}"}
        })

    if failed:
        lines = "\n".join([f"❌ {r['exchange']:10s} {r.get('error', '')}"
                          for r in failed[:10]])
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"**下载失败**:\n{lines}"}
        })

    logger.feishu_card(f"📥 Fetcher 下载报告 {trade_date}", elements)


def parser_card(logger, trade_date: str, stats_list: list) -> None:
    """Send a Feishu card summarizing parser results."""
    total_records = sum(s.get("total_records", 0) for s in stats_list)
    total_errors = sum(s.get("errors", 0) for s in stats_list)

    lines = []
    for s in stats_list:
        exc = s.get("exchange", "?")
        dt = s.get("data_type", "?")
        tr = s.get("total_records", 0)
        err = s.get("errors", 0)
        missing = s.get("missing", {})
        missing_str = ", ".join(
            [f"{k}={v}" for k, v in missing.items() if v > 0 and v == tr]
        )
        part_missing = ", ".join(
            [f"{k}={v}" for k, v in missing.items() if v > 0 and v < tr]
        )
        parts = [f"{exc:10s} {dt:20s} total={tr:5d}"]
        if err:
            parts.append(f"err={err}")
        if missing_str:
            parts.append(f"全缺=[{missing_str}]")
        if part_missing:
            parts.append(f"部分缺=[{part_missing}]")
        lines.append("  ".join(parts))

    elements = [
        {"tag": "div", "text": {
            "tag": "lark_md",
            "content": f"**交易日期**: {trade_date}\n"
                       f"**总记录数**: {total_records}  |  "
                       f"**解析错误**: {total_errors}"
        }},
        {"tag": "hr"},
        {"tag": "div", "text": {
            "tag": "lark_md",
            "content": "```\n" + "\n".join(lines[:20]) + "\n```"
        }},
    ]

    logger.feishu_card(f"🔍 Parser 解析报告 {trade_date}", elements)


def verifier_card(logger, trade_date: str, compare_results: dict) -> None:
    """Send a Feishu card summarizing field comparison results."""
    lines = []
    total_matched = 0
    total_diff = 0
    for field, summary in compare_results.items():
        m = summary.get("matched", 0)
        d = summary.get("diff", 0)
        max_dev = summary.get("max_deviation_pct", 0)
        om = summary.get("missing_in_original", 0)
        nm = summary.get("missing_in_new", 0)
        total_matched += m
        total_diff += d
        status = "✅" if d == 0 and nm == 0 else "⚠️"
        extra = []
        if om:
            extra.append(f"原表缺={om}")
        if nm:
            extra.append(f"新表缺={nm}")
        if d:
            extra.append(f"最大偏差={max_dev:.2f}%")
        extra_str = f" ({', '.join(extra)})" if extra else ""
        lines.append(
            f"{status} {field:12s}: 匹配={m:5d} 差异={d:3d}{extra_str}"
        )

    elements = [
        {"tag": "div", "text": {
            "tag": "lark_md",
            "content": f"**对比日期**: {trade_date}\n"
                       f"**总匹配**: {total_matched}  |  "
                       f"**总差异**: {total_diff}"
        }},
        {"tag": "hr"},
        {"tag": "div", "text": {
            "tag": "lark_md",
            "content": "```\n" + "\n".join(lines[:20]) + "\n```"
        }},
    ]

    # Add sample diffs if any
    sample_shown = False
    for field, summary in compare_results.items():
        samples = summary.get("sample_diffs", [])
        if samples and not sample_shown:
            sample_lines = [
                f"- {s['code']} {s['date']}: "
                f"原={s['original']:.2f}, 新={s['new']:.2f}, "
                f"偏差={s['deviation_pct']:.2f}%"
                for s in samples[:3]
            ]
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**差异样例 ({field})**:\n"
                               + "\n".join(sample_lines)
                }
            })
            sample_shown = True

    logger.feishu_card("✅ Verifier 字段比对报告", elements)
