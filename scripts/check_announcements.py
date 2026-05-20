#!/usr/bin/env python3
"""
快速公告检查 — pipeline Step 4。

检查交易所是否有新公告发布，输出到日志供人工/LLM后续分析。
"""

import json
import urllib.request
import re
from datetime import datetime
from pathlib import Path

STATE_FILE = Path("data/announcement_state.json")

# 各交易所可直达的单个公告页（用于验证连通性）
ANNOUNCEMENT_URLS = {
    "INE": "https://www.ine.cn/publicnotice/notice/202601/t20260116_830122.html",
    "SHFE": "https://www.shfe.com.cn/publicnotice/notice/202605/t20260515_831713.html",
    "CFFEX": "http://www.cffex.com.cn/cn/jysgg.html",
    "GFEX": "http://www.gfex.com.cn/gfex/tzts/list_yw.shtml",
}

# 影响数据管线的关键词
KEYWORDS = [
    "最小变动价位", "涨跌停板幅度", "涨停板幅度", "跌停板幅度",
    "合约修订", "合约月份", "保证金",
]


def main():
    state = load_state()
    prev_ids = set(state.get("known_ids", []))
    new_ids = set()
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 公告检查开始")
    print()
    
    # 1. 检查已知公告页连通性
    for name, url in ANNOUNCEMENT_URLS.items():
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                html = resp.read().decode("utf-8", errors="replace")
                size = len(html)
                
                # 检查是否包含相关关键词
                matched = [kw for kw in KEYWORDS if kw in html]
                tags = f"命中: {', '.join(matched)}" if matched else "无相关关键词"
                
                rid = f"{name}:{size}"
                new_ids.add(rid)
                
                if rid not in prev_ids:
                    print(f"  🆕 [{name}] 公告页可达 ({size:,} bytes) {tags}")
                else:
                    print(f"     [{name}] 公告页可达 ({size:,} bytes) {tags}")
        except Exception as e:
            print(f"  ❌ [{name}] 不可达: {e}")
    
    # 2. 检查需要手动搜索的交易所
    print()
    print("  需要人工搜索确认的交易所（页面JS渲染）:")
    for name in ["DCE", "CZCE"]:
        print(f"    🔍 {name}: 需 web_search")
    
    # 3. 输出供LLM分析的查询
    print()
    print("--- 以下为LLM分析搜索查询 ---")
    queries = [
        "上海国际能源交易中心 公告 最小变动价位 site:ine.cn",
        "上海期货交易所 公告 涨跌停板 site:shfe.com.cn",
        "大连商品交易所 调整 涨跌停板 保证金 site:dce.com.cn",
        "郑州商品交易所 涨跌停板 调整 site:czce.com.cn",
        "中国金融期货交易所 保证金 调整 site:cffex.com.cn",
        "广州期货交易所 涨跌停板 调整 site:gfex.com.cn",
    ]
    for q in queries:
        print(f"  🔍 {q}")
    print()
    
    # Save state
    state["known_ids"] = list(new_ids | prev_ids)
    state["last_check"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    save_state(state)


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
