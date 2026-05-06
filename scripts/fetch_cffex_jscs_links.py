#!/usr/bin/env python3
"""Fetch CFFEX jscs.html and extract settlement data file links."""
import re
import requests


def fetch_cffex_jscs_links():
    """Fetch CFFEX settlement page and return all <a> links."""
    url = "http://www.cffex.com.cn/cn/jscs.html"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }

    resp = requests.get(url, headers=headers, timeout=15)
    resp.encoding = "utf-8"
    html = resp.text

    base = "http://www.cffex.com.cn"
    links = []

    # Match <a ... href="..."...>TEXT</a>
    pattern = re.compile(
        r'<a\s[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(html):
        href = match.group(1).strip()
        raw_text = match.group(2).strip()
        # Strip inner HTML tags from text
        text = re.sub(r"<[^>]+>", "", raw_text).strip()
        if not href or href.startswith("#") or href.startswith("javascript"):
            continue
        full_url = href if href.startswith("http") else f"{base}/{href.lstrip('/')}"
        links.append({"url": full_url, "text": text or href})

    return links


if __name__ == "__main__":
    links = fetch_cffex_jscs_links()
    print(f"Total links found: {len(links)}\n")
    for i, link in enumerate(links, 1):
        print(f"  [{i:3d}] {link['text'][:60]:60s}  {link['url']}")
