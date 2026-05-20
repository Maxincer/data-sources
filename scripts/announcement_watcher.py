#!/usr/bin/env python3
"""
交易所公告监控脚本。

思路：交易所公告列表页多为 JS 动态渲染，难以直接爬取。
本脚本通过搜索引擎 API 检索各交易所近期公告，再调用 LLM 判断是否影响数据管线。

用法:
  python scripts/announcement_watcher.py              # 检查近7天公告
  python scripts/announcement_watcher.py --days 30    # 检查近30天

依赖:
  pip install requests  # 如需飞书通知
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from typing import List, Optional

# ===================================================================
# 交易所公告搜索配置
# ===================================================================

EXCHANGES = [
    {
        "name": "INE (上海国际能源交易中心)",
        "search_queries": [
            "上海国际能源交易中心 公告 最小变动价位 site:ine.cn",
            "上海国际能源交易中心 涨跌停板 调整 site:ine.cn",
            "上海国际能源交易中心 保证金 调整 site:ine.cn",
        ],
        "url_pattern": "https://www.ine.cn/publicnotice/notice/{date}/t{article_id}.html",
        "doc_ids": [],  # 由搜索填充
    },
    {
        "name": "SHFE (上海期货交易所)",
        "search_queries": [
            "上海期货交易所 公告 涨跌停板 site:shfe.com.cn",
            "上海期货交易所 保证金 调整 site:shfe.com.cn",
        ],
    },
    {
        "name": "DCE (大连商品交易所)",
        "search_queries": [
            "大连商品交易所 调整 涨跌停板 公告 site:dce.com.cn",
            "大连商品交易所 最小变动价位 site:dce.com.cn",
        ],
    },
    {
        "name": "CZCE (郑州商品交易所)",
        "search_queries": [
            "郑州商品交易所 涨跌停板 调整 site:czce.com.cn",
            "郑州商品交易所 保证金 调整 site:czce.com.cn",
        ],
    },
    {
        "name": "CFFEX (中国金融期货交易所)",
        "search_queries": [
            "中国金融期货交易所 涨跌停板 调整 site:cffex.com.cn",
            "中国金融期货交易所 保证金 调整 site:cffex.com.cn",
        ],
    },
    {
        "name": "GFEX (广州期货交易所)",
        "search_queries": [
            "广州期货交易所 涨跌停板 调整 site:gfex.com.cn",
        ],
    },
]

# 我们关心的参数关键词
KEYWORDS = [
    "最小变动价位",
    "涨跌停板幅度",
    "涨停板幅度",
    "跌停板幅度",
    "保证金比例",
    "交易保证金",
    "合约修订",
    "合约调整",
    "交割",
    "涨跌停",
    "合约乘数",
]

# 不关心的关键词（过滤掉）
SKIP_KEYWORDS = [
    "仿真交易",
    "测试",
    "会员",
    "从业人员",
    "投诉",
    "党建",
]

# 各品种的计算逻辑描述（供LLM上下文参考）
CALCULATION_LOGIC = """
## 我们的涨跌停计算逻辑

### 公式
  maxup   = floor(前结算价 × (1 + 涨停幅度) / tick) × tick
  maxdown = floor(前结算价 × (1 - 跌停幅度) / tick) × tick

### 数据来源
  - 前结算价: DailyMarketData.dat (交易所API, 每日盘后)
  - 涨跌停幅度: TradingParameters.dat → UPPER_VALUE / LOWER_VALUE (交易所API, 每日)
  - tick(最小变动价位): product_configs/*.html (交易所官网合约规格页)
  - 舍入规则: SHFE/INE 用 FLOOR, CZCE/DCE/GFEX用各自规则

### 我们关心的字段
  - maxup (涨停价)
  - maxdown (跌停价)
  - long_margin (多头保证金)
  - short_margin (空头保证金)
  - minoq / maxoq (最小/最大下单量)
  - settle (结算价, 当合约月份调整时会受影响)

### 潜在影响
  如果交易所公告涉及以下变更，我们的数据管线需要相应调整:
  1. 最小变动价位变更 → 涨跌停计算需要改tick
  2. 涨跌停板幅度变更 → 自动生效(TradingParameters每日更新)
  3. 合约月份调整 → 期货代码映射需要更新
  4. 新合约上市 → 新增代码映射
  5. 保证金比例变更 → 自动生效(TradingParameters每日更新)
"""


def fetch_page(url: str) -> Optional[str]:
    """获取网页内容。"""
    try:
        import requests
        resp = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
            },
            timeout=15,
        )
        resp.raise_for_status()
        # 检测编码
        if resp.encoding and resp.encoding.lower() != "utf-8":
            resp.encoding = resp.apparent_encoding
        return resp.text
    except Exception as e:
        return None


def extract_text_from_html(html: str) -> str:
    """从HTML中提取纯文本。"""
    # 移除 script/style
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    # 移除 HTML 标签
    text = re.sub(r'<[^>]+>', '', text)
    # 移除多余空白
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def search_announcements(exchange: dict, days_back: int = 7) -> List[dict]:
    """
    对每个交易所的搜索查询，返回匹配的公告。
    注意: 本函数需要 web_search 工具支持。
    在自动化运行时，可以通过搜索引擎API或调用主系统工具来实现。
    
    当前返回结构化搜索结果占位符。
    """
    results = []
    for query in exchange["search_queries"]:
        # 这里将输出搜索参数供外部工具使用
        results.append({
            "exchange": exchange["name"],
            "query": query,
            "days_back": days_back,
        })
    return results


def check_known_urls() -> List[dict]:
    """检查已知的公告URL模式（直连交易所页面）。"""
    results = []
    
    # INE 公告已知URL模式
    ine_patterns = [
        ("https://www.ine.cn/publicnotice/notice/{year}{month:02d}/", "INE公告列表"),
    ]
    
    # 部分公告可以直接通过已知ID访问
    known_articles = [
        # EC 最小变动价位调整系列
        {
            "url": "https://www.ine.cn/publicnotice/notice/202601/t20260116_830122.html",
            "title": "关于发布《上海国际能源交易中心集运指数（欧线）期货标准合约》（修订案）的公告",
        },
        {
            "url": "https://www.ine.cn/publicnotice/notice/202601/t20260116_830126.html",
            "title": "关于集运指数（欧线）期货合约修订实施有关事项的通知",
        },
    ]
    
    for article in known_articles:
        html = fetch_page(article["url"])
        if html:
            text = extract_text_from_html(html)
            results.append({
                "url": article["url"],
                "title": article["title"],
                "content": text[:2000],
                "accessible": True,
            })
        else:
            results.append({
                "url": article["url"],
                "title": article["title"],
                "accessible": False,
            })
    
    return results


def filter_relevant(text: str) -> bool:
    """判断公告内容是否与我们相关。"""
    matched = [kw for kw in KEYWORDS if kw in text]
    skipped = [kw for kw in SKIP_KEYWORDS if kw in text]
    
    # 如果包含跳过关键词，不相关
    if skipped and not matched:
        return False
    
    return len(matched) > 0


def llm_prompt(exchange: str, announcements: List[dict]) -> str:
    """生成 LLM prompt。"""
    lines = [
        "你是一个期货市场数据系统的专家。",
        "以下是各交易所近期的公告，请分析哪些公告可能影响我们的数据管线。\n",
        CALCULATION_LOGIC,
        "\n## 近期公告\n",
    ]
    
    for ann in announcements:
        lines.append(f"### [{ann.get('exchange','?')}] {ann.get('title','?')}")
        lines.append(f"URL: {ann.get('url','?')}")
        lines.append(f"内容摘要: {ann.get('content','(无法获取)')[:1500]}")
        lines.append("")
    
    lines.extend([
        "\n## 任务",
        "1. 逐条判断每个公告是否可能影响我们的数据管线",
        "2. 如有影响，描述影响的字段、时间点和需要做的调整",
        "3. 如无影响，注明'无影响'",
        "\n## 输出格式",
        "每个公告一行: [交易所] 标题 | 影响: [有/无] | 影响字段: [...] | 生效日期: [...] | 需要调整: [...]",
    ])
    
    return "\n".join(lines)


def generate_report(announcements: List[dict]) -> str:
    """生成包含LLM prompt的报告。"""
    lines = []
    lines.append("=" * 70)
    lines.append(f"交易所公告监控报告")
    lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 70)
    lines.append("")
    
    # 统计
    total = len(announcements)
    relevant = [a for a in announcements if filter_relevant(a.get("content", ""))]
    
    lines.append(f"检查公告数: {total}")
    lines.append(f"命中关键词: {len(relevant)}")
    lines.append("")
    
    if relevant:
        lines.append("### 命中关键词的公告")
        for a in relevant:
            kw = [k for k in KEYWORDS if k in a.get("content", "")]
            lines.append(f"  - [{a.get('exchange','?')}] {a.get('title','?')}")
            lines.append(f"    URL: {a.get('url','?')}")
            lines.append(f"    命中: {', '.join(kw)}")
            lines.append("")
    
    # LLM Prompt
    lines.append("=" * 70)
    lines.append("以下内容供LLM分析")
    lines.append("=" * 70)
    lines.append("")
    lines.append(llm_prompt("全部", announcements))
    
    return "\n".join(lines)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="交易所公告监控")
    parser.add_argument("--days", type=int, default=7, help="回溯天数")
    parser.add_argument("--output", help="输出文件路径")
    args = parser.parse_args()
    
    print(f"检查近 {args.days} 天的交易所公告...")
    print()
    
    all_announcements = []
    
    # 1. 尝试直连已知URL
    print("> 检查已知公告URL...")
    known = check_known_urls()
    all_announcements.extend(known)
    for a in known:
        status = "✅" if a.get("accessible") else "❌"
        print(f"  {status} {a['title']}")
    
    # 2. 输出搜索参数（供web_search工具使用）
    print()
    print("> 需要搜索的查询（共 {} 条）:".format(
        sum(len(ex["search_queries"]) for ex in EXCHANGES)
    ))
    for ex in EXCHANGES:
        for q in ex["search_queries"]:
            print(f"  🔍 {q}")
    
    # 3. 生成报告
    print()
    report = generate_report(all_announcements)
    
    if args.output:
        with open(args.output, "w") as f:
            f.write(report)
        print(f"报告已保存: {args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()
