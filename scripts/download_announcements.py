#!/usr/bin/env python3
"""
下载公告原始内容（Playwright + 系统 Chrome headless）。
从 metadata.jsonl 读取，下载 -> data/raw/announcements/。
"""
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ANNOUNCEMENTS_DIR = PROJECT_ROOT / "data" / "raw" / "announcements"
METADATA_FILE = ANNOUNCEMENTS_DIR / "metadata.jsonl"
CHROME_BIN = "/tmp/chrome-extract/opt/google/chrome/chrome"
CHROME_DEPS = "/tmp/chrome-deps/usr/lib/x86_64-linux-gnu"

os.environ["LD_LIBRARY_PATH"] = CHROME_DEPS + ":" + os.environ.get("LD_LIBRARY_PATH", "")


def load_metadata() -> list[dict]:
    records = []
    with open(METADATA_FILE) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def save_metadata(records: list[dict]):
    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(METADATA_FILE, "w") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def parse_html_to_text(html: str) -> str:
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    return text.strip()


def main():
    from playwright.sync_api import sync_playwright

    if not Path(CHROME_BIN).exists():
        print(f"[ERROR] Chrome 未找到: {CHROME_BIN}")
        return

    records = load_metadata()
    pending = [r for r in records if not r.get("crawled_content")]

    if not pending:
        print("所有公告已下载完毕。")
        return

    print(f"待下载: {len(pending)} 条")
    print(f"浏览器: {CHROME_BIN}")
    ANNOUNCEMENTS_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            executable_path=CHROME_BIN,
            args=[
                "--no-sandbox",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
                "--window-size=1920,1080",
                "--disable-dev-shm-usage",
            ],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
        )

        for i, rec in enumerate(pending, 1):
            rid = rec["id"]
            url = rec["url"]
            title = rec.get("title", "?")
            print(f"[{i}/{len(pending)}] {title[:60]}...")

            html_file = ANNOUNCEMENTS_DIR / f"{rid}.html"

            try:
                page = context.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                # 等正文加载
                try:
                    page.wait_for_selector(
                        "article, .content, .main, .article, .detail, "
                        ".news-content, .TRS_Editor, .Custom_UnionStyle, #zoom",
                        timeout=5_000,
                    )
                except Exception:
                    pass

                html = page.content()
                html_file.write_text(html, encoding="utf-8")
                page.close()

                # 纯文本版
                text = parse_html_to_text(html)
                txt_file = ANNOUNCEMENTS_DIR / f"{rid}.txt"
                txt_file.write_text(text, encoding="utf-8")

                rec["source_file"] = str(html_file.relative_to(PROJECT_ROOT))
                rec["source_txt"] = str(txt_file.relative_to(PROJECT_ROOT))
                rec["crawled_content"] = True
                rec["crawl_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                print(f"  ✓ HTML ({len(html):,} bytes) | 文本 ({len(text):,} chars)")

            except Exception as e:
                print(f"  ✗ 失败: {e}")
                rec["_download_error"] = str(e)

        browser.close()

    save_metadata(records)
    print(f"\n完成 → {METADATA_FILE}")


if __name__ == "__main__":
    main()
