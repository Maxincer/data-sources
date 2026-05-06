"""
Scrape SHFE/INE product specification HTML pages for tick size data.

Uses non-headless Playwright (headless=False) to bypass WAF JS challenge
by presenting a real browser fingerprint.

Workflow:
1. Visit product listing page, discover all product links dynamically
2. For each product, save the raw HTML to data/raw/product_configs/
3. Parser (parser.py) later reads these HTML files and extracts tick sizes

Directory structure:
  data/raw/product_configs/
    YYYYMMDD.SHFE.au_f.html
    YYYYMMDD.SHFE.cu_f.html
    ...
"""

import json, re, sys, time
from pathlib import Path
from datetime import date
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

RAW_DIR = Path("data/raw")
CONFIG_DIR = RAW_DIR / "product_configs"
TODAY = date.today().strftime("%Y%m%d")

EXCHANGES = [
    {
        "name": "SHFE",
        "nick": "shfe",
        "listing_url": "https://www.shfe.com.cn/products/",
        "domain": "www.shfe.com.cn",
    },
    {
        "name": "INE",
        "nick": "ine",
        "listing_url": "https://www.ine.com.cn/products/",
        "domain": "www.ine.com.cn",
    },
]


def discover_product_urls(browser, exchange_info) -> list:
    """Scrape product listing page to discover all individual product URLs."""
    name = exchange_info["name"]
    nick = exchange_info["nick"]
    listing_url = exchange_info["listing_url"]

    page = browser.new_page()
    product_urls = set()

    try:
        page.goto(listing_url, wait_until="commit", timeout=30000)
        page.wait_for_timeout(8000)  # Wait for WAF JS challenge

        html = page.content()
        if "safeline" in html.lower() or "人机识别" in html:
            print(f"  ⚠ {name}: WAF still active, waiting...")
            page.wait_for_timeout(8000)
            html = page.content()

        if "safeline" in html.lower() or "人机识别" in html:
            print(f"  ✗ {name}: BLOCKED by WAF")
            return []

        soup = BeautifulSoup(html, "html.parser")
        domain = exchange_info["domain"]

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # Match product detail pages: /products/futures/<path>/<code>_f/
            # Path can be multi-level (e.g. metal/nonferrousmetal/)
            m = re.search(r"(/products/futures/.*/\w+_f/)$", href)
            if m:
                full_url = f"https://{domain}{m.group(1)}"
                code_match = re.search(r"/(\w+_f)/$", href)
                if code_match:
                    product_urls.add((code_match.group(1), full_url))

    except Exception as e:
        print(f"  ✗ {name}: Error discovering products: {e}")
    finally:
        page.close()

    return [{"code": code, "url": url, "exchange": name} for code, url in sorted(product_urls)]


def fetch_product_html(browser, product_info) -> str:
    """Fetch a single product page and return its HTML content."""
    page = browser.new_page()

    try:
        page.goto(product_info["url"], wait_until="commit", timeout=30000)
        page.wait_for_timeout(6000)

        html = page.content()
        if "safeline" in html.lower() or "人机识别" in html:
            page.wait_for_timeout(6000)
            html = page.content()
        if "safeline" in html.lower() or "人机识别" in html:
            return None

        return html
    except Exception:
        return None
    finally:
        page.close()


def save_html(html: str, product_info: dict):
    """Save raw HTML to data/raw/product_configs/."""
    filename = f"{TODAY}.{product_info['exchange']}.{product_info['code']}.html"
    fpath = CONFIG_DIR / filename
    fpath.write_text(html, encoding="utf-8")
    return fpath


def main():
    print("=" * 60)
    print("SHFE/INE 合约规格 HTML 爬取")
    print(f"日期: {TODAY}")
    print("模式: 非无头浏览器 (headless=False)")
    print("=" * 60)

    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,  # Real browser window for WAF bypass
            args=["--no-sandbox"],
        )

        for exch in EXCHANGES:
            name = exch["name"]
            print(f"\n--- {name}: 发现产品 ---")
            products = discover_product_urls(browser, exch)
            if not products:
                print(f"  {name}: 未发现产品")
                continue

            print(f"  发现 {len(products)} 个产品")

            print(f"\n--- {name}: 下载 HTML ---")
            for i, prod in enumerate(products):
                code = prod["code"]
                print(f"  [{i+1}/{len(products)}] {code}...", end=" ")
                sys.stdout.flush()

                html = fetch_product_html(browser, prod)
                if html:
                    fpath = save_html(html, prod)
                    print(f"✅ {fpath.name} ({len(html)} bytes)")
                else:
                    print("❌")

                time.sleep(1)

        browser.close()

    # Summary
    saved = list(CONFIG_DIR.glob(f"{TODAY}.*.html"))
    print(f"\n{'=' * 60}")
    print(f"完成: {len(saved)} 个 HTML 文件")
    print(f"目录: {CONFIG_DIR}/")
    for f in saved:
        print(f"  {f.name}")


if __name__ == "__main__":
    main()
