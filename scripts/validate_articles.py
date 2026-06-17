#!/usr/bin/env python3
"""
公告内容质量检测脚本 (探索版)

扫描 metadata 中的全部 HTML 文件, 检测以下问题:
  1. WAF/验证码页面
  2. 404 错误页
  3. 导航壳（正文只有附件链接, 无实际内容）
  4. "系统暂时无法提供服务" 等应用层错误
  5. 疑似空白/截断（纯文本 < xx 字符 且 无 WAF/404/附件）

输出: 按交易所汇总的问题统计 + 异常清单
"""

import json
import re
import sys
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path("data/raw/announcements")
META_FILE = DATA_DIR / "announcements_metadata.json"

# ── 检测规则 ──────────────────────────────────────
MIN_BODY_TEXT_LEN = 150  # 低于此值视为异常 (排除导航壳/WAF/404 后)


def _body_text(html: str) -> str:
    """剥掉 <script> <style> <tag> 后的纯文本."""
    text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _has_inline_content(html: str) -> bool:
    """判断是否有内联公告正文.

    支持:
    - TRS_Editor / article-content 等专用内容容器
    - 普通 <p> 标签内嵌正文 (CFFEX/GFEX 常见格式): 统计 >30 字符的有意义段落数
    """
    # 1. 专用内容容器
    if bool(
        re.search(r'class=["\']TRS_Editor["\']', html)
        or re.search(r'class=["\']article-content["\']', html)
        or re.search(r'class=["\']articleContent["\']', html)
    ):
        return True

    # 2. 法规条款特征 (跨标签通用: 第X章/第Y条)
    if re.search(r'第[一二三四五六七八九十百千\d]+(章|条|节|款)', html):
        return True

    # 3. 普通 <p> 标签内容 (CFFEX notification pages)
    meaningful_p_count = 0
    for m in re.finditer(r'<p[^>]*>(.*?)</p>', html, re.DOTALL):
        text = re.sub(r'<[^>]+>', '', m.group(1))
        text = text.replace('&nbsp;', ' ').strip()
        skip = ('首页', '返回', '关闭', '附件下载', '＞', '>',
                '沪ICP', '沪公网', '支持IPv', '本网站', '当前位置')
        if any(text.startswith(s) for s in skip):
            continue
        if len(text) > 30:
            meaningful_p_count += 1

    return meaningful_p_count >= 1


def _has_attachment(html: str) -> bool:
    """判断是否有 PDF/DOC/DOCX 附件链接."""
    return bool(re.search(r'\.(pdf|docx?)(\?|")', html, re.IGNORECASE))


def classify_file(filepath: Path, note: str = "") -> dict:
    """
    分类单个 HTML 文件.
    返回: {"status": "ok|waf|404|shell|error|empty", "detail": str, "text_len": int}
    """
    if not filepath.exists():
        return {"status": "missing", "detail": "文件不存在", "text_len": 0}

    try:
        html = filepath.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return {"status": "error", "detail": "文件读取失败", "text_len": 0}

    text = _body_text(html)
    text_len = len(text)
    title = re.search(r"<title>([^<]+)</title>", html)

    # 1. WAF
    waf_keywords = ["safeline", "waf", "验证码", "滑块验证",
                    "安全验证", "bot", "cf-browser-verify"]
    for kw in waf_keywords:
        if kw.lower() in text.lower():
            return {"status": "waf", "detail": f"检测到 WAF 关键词: {kw}",
                    "text_len": text_len}

    # 2. 404
    if title and ("404" in title.group(1) or "错误" in title.group(1)):
        return {"status": "404", "detail": f"标题: {title.group(1)[:60]}",
                "text_len": text_len}

    # 3. 应用层错误
    error_patterns = [
        "系统暂时无法提供服务",
        "服务暂不可用",
        "请求太频繁",
        "请稍后再试",
    ]
    for pat in error_patterns:
        if pat in text:
            return {"status": "error", "detail": f"应用错误: {pat}",
                    "text_len": text_len}

    # 4. 附件型导航壳 (有 PDF/DOC 链接, 无内联合约正文)
    #     导航菜单撑大 text_len (~20KB), 不能用阈值判断
    if _has_attachment(html) and not _has_inline_content(html):
        return {"status": "shell",
                "detail": "仅有附件链接, 无 TRS_Editor/article-content 正文",
                "text_len": text_len}

    # 5. Tavily 提取的内容 (note 中包含 Tavily 标记)
    if "Tavily" in note:
        # Tavily 提取的是文本而非 HTML, 正文就是全部内容
        if text_len < MIN_BODY_TEXT_LEN:
            return {"status": "short",
                    "detail": f"Tavily 提取文本过短 ({text_len} chars)",
                    "text_len": text_len}

    # 6. 纯文本过短
    if text_len < MIN_BODY_TEXT_LEN:
        return {"status": "short",
                "detail": f"正文过短 ({text_len} chars)",
                "text_len": text_len}

    return {"status": "ok", "detail": f"{text_len} chars",
            "text_len": text_len, "has_inline": _has_inline_content(html),
            "has_attach": _has_attachment(html)}


def main():
    if not META_FILE.exists():
        print(f"错误: metadata 文件不存在: {META_FILE}")
        sys.exit(1)

    meta = json.loads(META_FILE.read_text(encoding="utf-8"))

    # 按交易所汇总
    stats = defaultdict(lambda: defaultdict(int))
    anomalies = []

    for k, v in meta.items():
        fpath = Path(v.get("source_file", ""))
        note = v.get("note", "")
        result = classify_file(fpath, note)

        ex = v.get("exchange", "?")
        cat = v.get("category", "?")
        stats[ex][result["status"]] += 1

        if result["status"] != "ok":
            anomalies.append({
                "id": k,
                "exchange": ex,
                "category": cat,
                "title": v.get("title", "")[:80],
                "file": v.get("source_file", ""),
                "status": result["status"],
                "detail": result["detail"],
                "text_len": result["text_len"],
            })

    # ── 输出 ──
    print("=" * 70)
    print("公告内容质量报告")
    print("=" * 70)

    # 按交易所输出
    all_exchanges = ["CFFEX", "DCE", "GFEX", "INE", "SHFE"]
    total_stats = defaultdict(int)
    for ex in all_exchanges:
        ex_stats = stats.get(ex, {})
        total = sum(ex_stats.values())
        ok = ex_stats.get("ok", 0)
        bad = total - ok
        total_stats["total"] += total
        total_stats["ok"] += ok
        total_stats["bad"] += bad

        badges = []
        for status in ["waf", "404", "shell", "error", "short", "missing"]:
            cnt = ex_stats.get(status, 0)
            if cnt:
                badges.append(f"{status}={cnt}")
                total_stats[status] += cnt

        print(f"\n{ex}: {total} 条 | OK: {ok} | 异常: {bad}",
              "(" + ", ".join(badges) + ")" if badges else "")

    print(f"\n{'─' * 40}")
    total_all = total_stats["total"]
    total_ok = total_stats["ok"]
    total_bad = total_stats["bad"]
    print(f"总计: {total_all} | 正常: {total_ok} | 异常: {total_bad}")
    print(f"通过率: {total_ok / total_all * 100:.1f}%" if total_all else "")

    # 异常清单
    if anomalies:
        print(f"\n{'=' * 70}")
        print(f"异常清单 ({len(anomalies)} 条)")
        print(f"{'=' * 70}")
        for a in sorted(anomalies, key=lambda x: (x["exchange"], x["status"])):
            print(f"  [{a['status']:7s}] {a['exchange']}/{a['category']} "
                  f"{a['id'][:24]:26s} | {a['title'][:60]}")
            print(f"              → {a['detail']}")

if __name__ == "__main__":
    main()
