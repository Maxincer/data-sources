#!/usr/bin/env python3
"""
交易所公告监控脚本 — NotifierWatchdog

工作流:
  1. 搜索引擎(Tavily)检索各交易所近期公告
  2. 直连已知公告页获取全文
  3. 关键词过滤 + LLM(DeepSeek)分析影响
  4. 有影响的公告 → 飞书告警 + 邮件通知
  5. 无新公告 → 静默退出

用法:
  python scripts/announcement_watcher.py                        # 默认近7天
  python scripts/announcement_watcher.py --days 30              # 近30天
  python scripts/announcement_watcher.py --force                # 强制重新检查已见过的公告
  python scripts/announcement_watcher.py --dry-run              # 只输出不告警

定时调度 (rutask):
  00 30 16 * * 1-5  → 工作日16:30，与data-sources pipeline同步
"""

import argparse
import json
import os
import re
import smtplib
import ssl
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path
from typing import List, Optional

# ── 依赖检查 ──────────────────────────────────────────────────────
try:
    import requests
except ImportError:
    print("ERROR: 需要安装 requests: pip install requests")
    sys.exit(1)

# ── 项目路径 ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = PROJECT_ROOT / "data" / "announcement_watcher_state.json"
DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR = PROJECT_ROOT / "logs"

# ── order_limits.csv 自动更新（可选依赖）────────────────────────────
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
try:
    from order_limits_updater import process_minoq_maxoq_change
    HAS_ORDER_LIMITS_UPDATER = True
except ImportError as e:
    HAS_ORDER_LIMITS_UPDATER = False
    print(f"  ⚠ order_limits_updater 未加载: {e}")

# ── 从环境变量读取敏感信息 ─────────────────────────────────────────
TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY", "")
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
FEISHU_WEBHOOK = os.environ.get("FEISHU_WEBHOOK", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.exmail.qq.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SENDER = os.environ.get("SENDER", "robot@wendao.fund")
RECIPIENTS = os.environ.get("RECIPIENTS", "mxz@wendao.fund")

# ── 交易所公告搜索配置 ────────────────────────────────────────────
EXCHANGES = [
    {
        "name": "INE (上海国际能源交易中心)",
        "short": "INE",
        "search_queries": [
            ("上海国际能源交易中心 公告 最小变动价位 site:ine.cn", 7),
            ("上海国际能源交易中心 涨跌停板 调整 site:ine.cn", 7),
            ("上海国际能源交易中心 保证金 调整 site:ine.cn", 7),
            ("上海国际能源交易中心 合约 修订 site:ine.cn", 14),
            ("上海国际能源交易中心 交易限额 调整 site:ine.cn", 7),
            ("上海国际能源交易中心 最大开仓 调整 site:ine.cn", 14),
        ],
    },
    {
        "name": "SHFE (上海期货交易所)",
        "short": "SHFE",
        "search_queries": [
            ("上海期货交易所 公告 涨跌停板 site:shfe.com.cn", 7),
            ("上海期货交易所 保证金 调整 site:shfe.com.cn", 7),
            ("上海期货交易所 最小变动价位 site:shfe.com.cn", 14),
            ("上海期货交易所 交易限额 调整 site:shfe.com.cn", 7),
        ],
    },
    {
        "name": "DCE (大连商品交易所)",
        "short": "DCE",
        "search_queries": [
            ("大连商品交易所 调整 涨跌停板 公告 site:dce.com.cn", 7),
            ("大连商品交易所 最小变动价位 site:dce.com.cn", 14),
            ("大连商品交易所 保证金 调整 site:dce.com.cn", 7),
            ("大连商品交易所 最大下单 调整 site:dce.com.cn", 14),
        ],
    },
    {
        "name": "CZCE (郑州商品交易所)",
        "short": "CZCE",
        "search_queries": [
            ("郑州商品交易所 涨跌停板 调整 site:czce.com.cn", 7),
            ("郑州商品交易所 保证金 调整 site:czce.com.cn", 7),
            ("郑州商品交易所 开仓 手数 调整 site:czce.com.cn", 14),
        ],
    },
    {
        "name": "CFFEX (中国金融期货交易所)",
        "short": "CFFEX",
        "search_queries": [
            ("中国金融期货交易所 涨跌停板 调整 site:cffex.com.cn", 7),
            ("中国金融期货交易所 保证金 调整 site:cffex.com.cn", 7),
        ],
    },
    {
        "name": "GFEX (广州期货交易所)",
        "short": "GFEX",
        "search_queries": [
            ("广州期货交易所 涨跌停板 调整 site:gfex.com.cn", 7),
            ("广州期货交易所 最小变动价位 site:gfex.com.cn", 14),
            ("广州期货交易所 最小开仓 调整 site:gfex.com.cn", 14),
        ],
    },
]

# 我们关心的参数关键词
KEYWORDS = [
    "最小变动价位", "涨跌停板幅度", "涨停板幅度", "跌停板幅度",
    "保证金比例", "交易保证金", "合约修订", "合约调整",
    "合约乘数", "最小开仓", "最大开仓", "持仓限额",
    "交割", "涨跌停", "上市", "交易参数", "风控参数",
    "mini_order_qty", "max_order_qty",
    "tick", "涨停", "跌停",
    "交易限额", "下单量", "开仓手数", "持仓限额",
    "交易手数", "限价", "市价",
]

# 不关心的关键词（过滤掉）
SKIP_KEYWORDS = [
    "仿真交易", "测试",
    "投诉", "党建", "党史", "征文", "招聘", "从业人员",
    "期货日报", "交割库", "培训", "服务",
    "社会责任", "ESG",
    "限制开仓监管措施",  # 风控个案，不是规则变更
    "违规处理决定",  # 风控个案
    "建发浆纸",  # 仓库/厂库扩容，不影响数据管线
    "增加纸浆厂库",
    "增加启用库容",
    "指定交割仓库",
    "成为我所",
    "交割库落地",
]

# 排除的URL模式（导航页、产品页、数据页等）
EXCLUDED_URL_PATTERNS = [
    r"/products/",
    r"/services",
    r"/reports(?:/|$)",
    r"query_params=",
    r"/option",
    r"spotlight/",
    r"sspz/",  # CZCE 品种介绍页
]

# 公告URL白名单模式（只有匹配这些模式的才认为是正式公告）
ANNOUNCEMENT_URL_PATTERNS = [
    r"(ine|shfe)\.(cn|com\.cn)/publicnotice/notice/",
    r"gfex\.com\.cn/gfex/tzts/",
    r"cffex\.com\.cn/cn/jysgg",
    r"dce\.com\.cn/dce/content/",
    r"czce\.com\.cn/cn/.*(?:公告|通知|调整)",
]

# 各品种的计算逻辑描述（供 LLM 上下文参考）
CALCULATION_LOGIC = """
我们是一个期货行情数据系统，以下字段是我们自行计算的：

## 涨跌停价计算

### SHFE / INE
  maxup   = floor(前结算价 × (1 + 涨停幅度) / tick) × tick
  maxdown = floor(前结算价 × (1 - 跌停幅度) / tick) × tick

### CZCE
  maxup   = ceil(前结算价 × (1 + 涨停幅度) / tick) × tick
  maxdown = floor(前结算价 × (1 - 跌停幅度) / tick) × tick

### 计算所需的依赖数据
  - 前结算价: DailyMarketData API (交易所每日盘后提供)
  - 涨跌停幅度: TradingParameters API → UPPER_VALUE / LOWER_VALUE
  - tick(最小变动价位): 交易所官网合约规格页 (Playwright爬取)
  - 舍入规则: SHFE/INE用FLOOR, CZCE用CEIL

## 我们关心的字段
  - maxup (涨停价)
  - maxdown (跌停价)
  - long_margin / short_margin (保证金)
  - minoq / maxoq (最小/最大下单量)
  - settle (结算价)

## 潜在影响场景
  1. 最小变动价位变更 → 涨跌停计算需要改tick
  2. 涨跌停板幅度变更 → 自动生效(TradingParameters每日更新)
  3. 合约月份调整 → 期货代码映射需要更新
  4. 新合约上市 → 新增代码映射
  5. 保证金比例变更 → 自动生效(TradingParameters每日更新)
  6. 最小/最大开仓手数变更 → order_limits.csv需要手动更新

## 当前 order_limits.csv 维护的值（部分示例）
  - CZCE MA: minoq=1(部分合约调整为8), maxoq_limit=1000, maxoq_market=200
  - GFEX lc(碳酸锂): minoq=5(原为1,品种公告调整)
  - DCE bz(纯苯): minoq=4(大商所发〔2026〕74号)
  - INE/SHFE: 全部期货 maxoq=500, minoq=1, 无市价指令
  - CFFEX IF: maxoq_limit=100, maxoq_market=50, minoq=1
"""


# ═══════════════════════════════════════════════════════════════════
#  核心模块
# ═══════════════════════════════════════════════════════════════════

def log(msg: str):
    """时间戳日志。"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def load_state() -> dict:
    """加载已见公告状态。"""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
    return {"known_ids": [], "last_check": None, "announcements": {}}


def save_state(state: dict):
    """保存公告状态。"""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(
        json.dumps(state, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )


import hashlib


def compute_id(url: str, title: str = "") -> str:
    """生成公告唯一ID（确定性哈希，跨进程一致）。"""
    raw = f"{url}|{title[:80]}".encode("utf-8")
    return hashlib.md5(raw).hexdigest()[:12]


def fetch_page(url: str, timeout: int = 15) -> Optional[str]:
    """获取网页内容。返回 None 若遇到 WAF 或非正常页面。"""
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        text = resp.text
        pg = resp.text[:200].lower()
        # 检测 WAF 防火墙（任意位置包含防护关键词）
        if any(kw in pg for kw in ["防火墙", "captcha", "js-challenge", "access denied"]):
            log(f"  ↺ WAF 拦截 {url[:60]}...")
            return None
        # 检测极短页面（<200字符有效HTML通常不是正常页面）
        clean = extract_text_from_html(text)
        if len(clean) < 100:
            log(f"  ↺ 页面内容过短({len(clean)}字符) {url[:60]}...")
            return None
        return text
    except Exception as e:
        log(f"  ⚠ 获取失败 {url}: {e}")
        return None


def extract_text_from_html(html: str) -> str:
    """从HTML中提取纯文本。"""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def filter_keywords(text: str) -> tuple:
    """关键词过滤: (命中的关键词列表, 跳过的关键词列表, 是否应跳过)。"""
    matched = [kw for kw in KEYWORDS if kw in text]
    skipped = [kw for kw in SKIP_KEYWORDS if kw in text]
    # 有跳过关键词 → 无论是否命中关键词都跳过
    should_skip = len(skipped) > 0
    return matched, skipped, should_skip


def tavily_extract_page(url: str) -> Optional[str]:
    """通过 Tavily extract API 获取页面内容（绕过 WAF）。"""
    if not TAVILY_API_KEY:
        return None
    try:
        resp = requests.post(
            "https://api.tavily.com/extract",
            json={"api_key": TAVILY_API_KEY, "urls": [url]},
            timeout=20,
        )
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("results", [])
            if results and results[0].get("raw_content"):
                raw = results[0]["raw_content"]
                if len(raw) > 100:
                    log(f"  ✓ Tavily extract 获取内容 ({len(raw)} chars)")
                    return raw
        return None
    except Exception as e:
        log(f"  ⚠ Tavily extract 失败 {url[:50]}: {e}")
        return None


def search_tavily(query: str, days_back: int = 7) -> List[dict]:
    """
    通过 Tavily API 搜索公告。
    返回搜索结果列表，每项含 title、url、content。
    """
    if not TAVILY_API_KEY:
        log("  ⚠ TAVILY_API_KEY 未设置，跳过搜索")
        return []

    url = "https://api.tavily.com/search"
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": "basic",
        "include_answer": False,
        "include_raw_content": False,
        "max_results": 10,
        "include_domains": [],
        "exclude_domains": [],
    }
    # Tavily 支持 time_range: day / week / month / year
    if days_back <= 1:
        payload["time_range"] = "day"
    elif days_back <= 7:
        payload["time_range"] = "week"
    elif days_back <= 30:
        payload["time_range"] = "month"
    else:
        payload["time_range"] = "year"

    try:
        resp = requests.post(url, json=payload, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        log(f"  ✓ 搜索 '{query[:40]}...' → {len(results)} 条结果")
        return results
    except Exception as e:
        log(f"  ✗ Tavily 搜索失败: {e}")
        return []


def analyze_with_llm(announcements: List[dict]) -> List[dict]:
    """
    调用 DeepSeek API 分析公告是否影响数据管线。
    返回公告列表，每条附加 "impact" 字段。
    """
    if not DEEPSEEK_API_KEY:
        log("  ⚠ DEEPSEEK_API_KEY 未设置，跳过 LLM 分析")
        for a in announcements:
            a["impact"] = "未分析(无API Key)"
        return announcements

    if not announcements:
        return announcements

    # 构建 LLM prompt
    lines = [CALCULATION_LOGIC, "\n## 近期公告\n"]
    for i, ann in enumerate(announcements, 1):
        ex = ann.get("exchange_short", ann.get("exchange", "?"))
        title = ann.get("title", "?")
        url = ann.get("url", "?")
        content = ann.get("content", ann.get("snippet", ""))[:1500]
        lines.append(f"### [{i}] [{ex}] {title}")
        lines.append(f"URL: {url}")
        lines.append(f"内容: {content}")
        lines.append("")

    prompt = "\n".join(lines)
    system_prompt = """你是一个期货市场数据系统专家。每一条公告请按以下三级分类判断：

## 分类标准

### A. 需手动处理（标"有/手动"）
需要我们的代码或配置文件手动更新的情况：
- 最小变动价位(tick)变更 → 需更新涨跌停计算逻辑中的tick值
- 最小/最大开仓手数(minoq/maxoq)变更 → 需手动更新order_limits.csv
- 合约乘数变更 → 影响计算逻辑
- 新合约上市 → 需新增代码映射
- 合约月份调整 → 需更新代码映射

### B. 自动生效但需关注（标"有/自动"）
交易所API已自动推送，不须手动改代码，但作为信息同步：
- 涨跌停板幅度调整 → TradingParameters API每日自动获取
- 保证金比例调整 → TradingParameters API每日自动获取
- 交割标准调整 → 不影响数据管线

### C. 无影响（标"无"）
与我们数据管线完全无关：
- 监管个案（限制开仓、违规处理）
- 仓库/厂库扩容
- 培训、党建、社会活动
- 期货日报等新闻报道

## 关键规则：必须提取数值变化
**无论分类为 手动/自动/无，每条公告都必须提取和报告涉及的数值变化！**

例如公告说"涨跌停板幅度调整为7%，保证金调整为8%"，你必须输出：
变更: [涨跌停板幅度: →7%, 套保保证金: →8%, 投机保证金: →9%]

如果公告涉及**最小开仓手数(minoq)** 或 **最大开仓手数(maxoq)** 的调整：
- 需明确标注影响的**品种代码**（如 lc, MA, sc 等）
- 需提取**变更前后的具体数值**（如旧: 1手, 新: 8手）
- 需注明**生效日期**和**合约范围**（全部合约还是特定合约月份）

输出格式（每行一个公告）：
[序号] [交易所简称] 标题 | 影响: 手动/自动/无 | 字段: [影响字段] | 品种: [品种代码] | 变更: [旧值→新值] | 生效日期: [YYYY-MM-DD] | 说明: [一句话]

**变更字段不可为空！** 即使无法获取旧值也要写明新值。"""

    payload = {
        "model": "deepseek-v4-flash",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": 2048,
    }

    try:
        resp = requests.post(
            "https://api.deepseek.com/chat/completions",
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        result = resp.json()
        analysis_text = result["choices"][0]["message"]["content"]

        # 解析 LLM 输出：按行分割，匹配 [N] 序号
        # LLM 输出示例:
        #   [1] SHFE ... | 影响: 有 | 字段: [maxup] | ...
        #   [2] INE ... | 影响: 无 | ...
        parsed_by_index = {}
        for line in analysis_text.split("\n"):
            idx_match = re.match(r"^\s*\[(\d+)\]", line)
            if idx_match:
                idx = int(idx_match.group(1)) - 1  # 1-based → 0-based
                parsed_by_index[idx] = line

        for i, ann in enumerate(announcements):
            ann["llm_analysis_raw"] = analysis_text
            if i in parsed_by_index:
                ann["_llm_line"] = parsed_by_index[i]
            ann["impact_assessed"] = True

        log(f"  ✓ LLM 分析完成 ({len(announcements)} 条, {len(parsed_by_index)} 行解析)")
        # 打印分类结果摘要
        manual_count = sum(1 for a in announcements if '手动' in a.get('_llm_line', ''))
        auto_count = sum(1 for a in announcements if '自动' in a.get('_llm_line', ''))
        log(f"  ├ 手动: {manual_count} | 自动: {auto_count} | 无影响: {len(announcements) - manual_count - auto_count}")
        return announcements

    except Exception as e:
        log(f"  ✗ LLM 分析失败: {e}")
        for a in announcements:
            a["impact"] = f"分析失败: {e}"
        return announcements


def _parse_llm_impact(ann: dict) -> dict:
    """解析LLM输出，提取结构化影响信息。
    仅使用逐条解析行 `_llm_line`，无反回退（避免误匹配其他公告）。
    """
    raw = ann.get("_llm_line", "")
    result = {
        "has_impact": False,
        "impact_level": "无",
        "fields": [],
        "variety": "",
        "change": "",
        "effective_date": "",
        "summary": "",
    }

    if not raw:
        return result

    # 解析标准格式的 LLM 输出
    # 格式: [N] EX 标题 | 影响: 有/无 | 字段: [...] | 品种: [...] | 变更: [...] | 生效日期: [...] | 说明: [...]
    # 三级分类: 手动/自动/无
    impact_match = re.search(r"影响:\s*(手动|自动|无)", raw)
    fields_match = re.search(r"字段:\s*\[?([^|]+?)\]?(?=\s*\||\s*$)", raw)
    variety_match = re.search(r"品种:\s*\[?([^|]+?)\]?(?=\s*\||\s*$)", raw)
    change_match = re.search(r"变更:\s*\[?([^|]+?)\]?(?=\s*\||\s*$)", raw)
    date_match = re.search(r"生效日期:\s*\[?([^|]+?)\]?(?=\s*\||\s*$)", raw)
    summary_match = re.search(r"说明:\s*\[?([^|]+?)\]?(?=\s*\||\s*$)", raw)

    if impact_match:
        level = impact_match.group(1)
        result["has_impact"] = (level in ("手动", "自动"))
        result["impact_level"] = level
    if fields_match:
        val = fields_match.group(1).strip().strip("[]")
    if variety_match:
        result["fields"] = [f.strip() for f in val.split(",") if f.strip() and f.strip() != "]"]
    if variety_match:
        result["variety"] = variety_match.group(1).strip().strip("[]")
    if change_match:
        result["change"] = change_match.group(1).strip()
    if date_match:
        result["effective_date"] = date_match.group(1).strip().strip("[]")
    if summary_match:
        result["summary"] = summary_match.group(1).strip()
    elif not result["has_impact"] and "无影响" in raw:
        result["summary"] = "无影响"

    return result


def send_feishu_alert(announcements: List[dict], order_limit_changes: List[str] = None):
    """通过飞书 Webhook 发送告警（快速扫描格式）。

    Args:
        announcements: 公告列表
        order_limit_changes: order_limits.csv 自动更新记录（标准格式行）
    """
    if not FEISHU_WEBHOOK:
        log("  ⚠ FEISHU_WEBHOOK 未设置，跳过飞书告警")
        return

    if not announcements:
        return

    # 解析并三级分类
    manual = []      # A. 需手动处理
    auto = []        # B. 自动生效
    no_impact = []   # C. 无影响
    for ann in announcements:
        # 没有 LLM 分析结果的行 → 归为无影响
        if not ann.get("_llm_line"):
            info = {"has_impact": False, "impact_level": "无",
                    "fields": [], "variety": "", "change": "",
                    "effective_date": "", "summary": ""}
        else:
            info = _parse_llm_impact(ann)
        ann["_parsed"] = info
        level = info["impact_level"]
        if level == "手动":
            manual.append(ann)
        elif level == "自动":
            auto.append(ann)
        else:
            no_impact.append(ann)

    elements = []

    # ── 统计头部 ──
    header_text = (
        f"**共发现 {len(announcements)} 条公告**\n"
        f"🔴 {len(manual)} 条需手动处理　🟡 {len(auto)} 条自动生效　⚪ {len(no_impact)} 条无影响"
    )
    # 如果是首次运行且没有 state 文件，标记一下
    elements.append({"tag": "markdown", "content": header_text})
    elements.append({"tag": "hr"})

    # ── A. 需手动处理 ──
    if manual:
        lines = ["**🔴 需手动处理**"]
        for ann in manual:
            info = ann["_parsed"]
            ex = ann.get("exchange_short", "?")
            title = ann.get("title", "?")[:45]
            date_str = f"[{info['effective_date']}]" if info["effective_date"] else ""
            change_str = info.get("change", "")[:100]
            variety_str = info.get("variety", "")
            conclusion = info.get("summary", "")[:80] if info.get("summary") else ""

            lines.append(f"🔴 **[{ex}]** {title}")
            # 第一行：变更内容（最醒目）
            if change_str:
                lines.append(f"   📈 {change_str}")
            if variety_str:
                lines.append(f"   品种: {variety_str} {date_str}")
            if conclusion and not change_str:
                lines.append(f"   📊 {conclusion}")
        lines.append("")
        elements.append({"tag": "markdown", "content": "\n".join(lines)})

    # ── B. 自动生效（需关注）──
    if auto:
        lines = ["**🟡 已自动生效（仅供参考）**"]
        for ann in auto:
            info = ann["_parsed"]
            ex = ann.get("exchange_short", "?")
            title = ann.get("title", "?")[:45]
            date_str = f"[{info['effective_date']}]" if info["effective_date"] else ""
            change_str = info.get("change", "")[:100]
            variety_str = info.get("variety", "")
            conclusion = info.get("summary", "")[:80] if info.get("summary") else ""

            lines.append(f"🟡 **[{ex}]** {title}")
            # 变更内容优先
            if change_str:
                lines.append(f"   📈 {change_str}")
            if variety_str:
                lines.append(f"   品种: {variety_str} {date_str}")
            if conclusion and not change_str:
                lines.append(f"   {conclusion}")
        lines.append("")
        elements.append({"tag": "markdown", "content": "\n".join(lines)})

    # ── 无影响的公告（仅标题，折叠）──
    if no_impact:
        no_impact_lines = ["**⚪ 其他公告（已确认无影响）**"]
        for ann in no_impact:
            ex = ann.get("exchange_short", "?")
            title = ann.get("title", "?")[:60]
            no_impact_lines.append(f"[{ex}] {title}")
        no_impact_lines.append("")
        elements.append({"tag": "markdown", "content": "\n".join(no_impact_lines)})

    # ── order_limits.csv 自动更新记录 ──
    if order_limit_changes:
        elements.append({"tag": "hr"})
        ol_lines = ["**📝 order_limits.csv 已自动更新**"]
        for line in order_limit_changes:
            ol_lines.append(f"`{line}`")
        elements.append({"tag": "markdown", "content": "\n".join(ol_lines)})

    # ── URL 列表（可点）──
    url_lines = ["**📎 原文链接**"]
    for ann in announcements:
        info = ann["_parsed"]
        ex = ann.get("exchange_short", "?")
        url = ann.get("url", "?")
        level = info.get("impact_level", "无")
        marker = {"手动": "🔴", "自动": "🟡", "无": "⚪"}.get(level, "⚪")
        short_title = ann.get("title", "?")[:30]
        url_lines.append(f"{marker} [{ex}] [{short_title}]({url})")
    elements.append({"tag": "markdown", "content": "\n".join(url_lines)})

    # ── 脚注 ──
    elements.append({
        "tag": "note",
        "elements": [
            {"tag": "plain_text", "content": f"NotifierWatchdog · {datetime.now().strftime('%Y-%m-%d %H:%M')}"}
        ],
    })

    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"📢 期货公告监控 · {len(manual)} 条需处理"},
                "template": "red" if manual else "orange" if auto else "blue",
            },
            "elements": elements,
        },
    }

    try:
        resp = requests.post(FEISHU_WEBHOOK, json=payload, timeout=10)
        resp.raise_for_status()
        log(f"  ✅ 飞书告警已发送 ({len(announcements)} 条, 手动{len(manual)} 自动{len(auto)})")
    except Exception as e:
        log(f"  ❌ 飞书告警发送失败: {e}")


def send_email_alert(announcements: List[dict]):
    """通过邮件发送告警（备用通道）。"""
    if not SMTP_PASSWORD:
        log("  ⚠ SMTP_PASSWORD 未设置，跳过邮件告警")
        return

    if not announcements:
        return

    lines = ["<h2>期货交易所公告监控告警</h2>", "<hr>"]
    for ann in announcements:
        lines.append(f"<h3>[{ann.get('exchange_short', '?')}] {ann.get('title', '?')}</h3>")
        lines.append(f"<p><a href='{ann.get('url', '#')}'>{ann.get('url', '?')}</a></p>")
        impact = ann.get("impact", ann.get("llm_analysis_raw", "待分析"))[:500]
        lines.append(f"<pre>{impact}</pre>")
        lines.append("<hr>")

    html = "\n".join(lines)
    msg = MIMEText(html, "html", "utf-8")
    msg["Subject"] = f"[NotifierWatchdog] {len(announcements)} 条重要公告变更"
    msg["From"] = formataddr(("机器人通知", SENDER))
    msg["To"] = RECIPIENTS

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx) as server:
            server.login(SENDER, SMTP_PASSWORD)
            server.sendmail(SENDER, RECIPIENTS.split(","), msg.as_string())
        log(f"  ✅ 邮件告警已发送 ({len(announcements)} 条)")
    except Exception as e:
        log(f"  ❌ 邮件告警发送失败: {e}")


def fetch_known_articles() -> List[dict]:
    """
    检查已知的交易所公告页面，获取内容做关键词过滤。
    """
    known = [
        {
            "url": "https://www.ine.cn/publicnotice/notice/202601/t20260116_830122.html",
            "title": "INE 集运指数(欧线)期货标准合约修订案",
            "exchange": "INE",
            "exchange_short": "INE",
        },
        {
            "url": "https://www.ine.cn/publicnotice/notice/202601/t20260116_830126.html",
            "title": "INE 集运指数(欧线)期货合约修订事项通知",
            "exchange": "INE",
            "exchange_short": "INE",
        },
        {
            "url": "https://www.shfe.com.cn/publicnotice/notice/202605/t20260515_831713.html",
            "title": "SHFE 公告",
            "exchange": "SHFE",
            "exchange_short": "SHFE",
        },
    ]

    results = []
    for article in known:
        html = fetch_page(article["url"])
        if html:
            text = extract_text_from_html(html)
            matched, skipped, _ = filter_keywords(text)
            results.append({
                **article,
                "content": text[:2000],
                "matched_keywords": matched,
                "skipped_keywords": skipped,
                "accessible": True,
            })
            log(f"  ✓ {article['exchange_short']}: {article['title'][:50]} ({len(text)} chars)")
        else:
            results.append({**article, "accessible": False})
            log(f"  ✗ {article['exchange_short']}: 不可达")

    return results


def search_all_exchanges(days_back: int = 7) -> List[dict]:
    """
    对所有交易所执行搜索引擎搜索，收集公告。
    """
    all_results = []
    seen_urls = set()

    for exchange in EXCHANGES:
        for query, query_days in exchange["search_queries"]:
            # 用较小的 days_back
            effective_days = min(days_back, query_days)
            results = search_tavily(query, effective_days)
            for r in results:
                url = r.get("url", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                all_results.append({
                    "exchange": exchange["name"],
                    "exchange_short": exchange["short"],
                    "title": r.get("title", "?"),
                    "url": url,
                    "snippet": r.get("content", ""),
                    "source": "search",
                })

    log(f"搜索引擎共发现 {len(all_results)} 条唯一公告链接")
    return all_results


def is_announcement_url(url: str) -> bool:
    """判断URL是否为正式的交易所公告页。"""
    for pattern in EXCLUDED_URL_PATTERNS:
        if re.search(pattern, url, re.IGNORECASE):
            return False
    for pattern in ANNOUNCEMENT_URL_PATTERNS:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    # 如果不是明确匹配公告模式也不是排除模式，保留（可能是新格式）
    return True


def fetch_new_announcements(search_results: List[dict],
                             known_ids: set) -> List[dict]:
    """
    从搜索结果中筛选新公告，获取全文，做关键词过滤。
    返回全新的、未见过且关键词命中的公告。
    """
    new_announcements = []

    for i, ann in enumerate(search_results):
        url = ann["url"]
        aid = compute_id(url, ann.get("title", ""))
        if aid in known_ids:
            continue

        # URL 白名单过滤
        if not is_announcement_url(url):
            log(f"    跳过(非公告页) [{ann['exchange_short']}] {ann['title'][:50]}")
            continue

        # 先检查标题/摘要是否应跳过
        matched, skipped, should_skip = filter_keywords(
            ann.get("snippet", "") + (ann.get("title", ""))
        )
        if should_skip:
            log(f"    跳过(关键词过滤) [{ann['exchange_short']}] {ann['title'][:50]}")
            continue

        # 尝试获取全文（如果 WAF 拦截则用 Tavily extract 兜底）
        full_text = None
        html = fetch_page(url)
        if html:
            full_text = extract_text_from_html(html)
        else:
            # WAF 拦截，改用 Tavily extract
            log(f"  ↺ Tavily extract 兜底 {url[:60]}...")
            extracted = tavily_extract_page(url)
            if extracted:
                full_text = extracted
                ann["tavily_extracted"] = True

        if full_text:
            matched, skipped, should_skip = filter_keywords(full_text)
            ann["content"] = full_text[:3000]
            ann["matched_keywords"] = matched
            ann["skipped_keywords"] = skipped
            ann["full_fetched"] = True
        else:
            ann["content"] = ann.get("snippet", "")
            ann["matched_keywords"] = matched
            ann["full_fetched"] = False

        # 全文过滤后仍匹配跳过关键词 → 丢弃
        if should_skip:
            log(f"    跳过(全文过滤) [{ann['exchange_short']}] {ann['title'][:50]}")
            # 加入 known_ids 防止重复检查
            if aid not in known_ids:
                known_ids.add(aid)
            continue

        ann["id"] = aid
        new_announcements.append(ann)

        log(f"  {'🔍' if matched else '   '} [{ann['exchange_short']}] "
            f"{ann['title'][:60]} "
            f"{('命中: ' + ', '.join(matched[:3])) if matched else ''}")

    return new_announcements


# ═══════════════════════════════════════════════════════════════════
#  主流程
# ═══════════════════════════════════════════════════════════════════

def run(days_back: int = 7, force: bool = False, dry_run: bool = False):
    """执行一次完整的公告监控。"""
    log(f"{'='*60}")
    log(f"NotifierWatchdog 启动 (回溯 {days_back} 天)")
    if dry_run:
        log("DRY RUN 模式：仅输出，不告警")
    log(f"{'='*60}")

    state = load_state()
    known_ids = set() if force else set(state.get("known_ids", []))

    # Step 1: 搜索引擎搜索
    log("\n[Step 1/5] 搜索引擎检索...")
    search_results = search_all_exchanges(days_back)

    # Step 2: 已知公告页直连
    log("\n[Step 2/5] 已知公告页检查...")
    known_articles = fetch_known_articles()

    # 合并搜索结果
    all_candidates = search_results
    for ka in known_articles:
        aid = compute_id(ka["url"], ka.get("title", ""))
        if aid not in known_ids:
            ka["id"] = aid
            all_candidates.append(ka)

    # Step 3: 筛选新公告
    log("\n[Step 3/5] 筛选新公告...")
    new_anns = fetch_new_announcements(all_candidates, known_ids)

    if not new_anns:
        log("无新公告，静默退出")
        # 更新状态
        state["last_check"] = datetime.now(timezone.utc).isoformat()
        state["last_result"] = "no_new"
        save_state(state)
        return

    # Step 4: LLM 分析
    log(f"\n[Step 4/6] LLM 分析 ({len(new_anns)} 条)...")
    new_anns = analyze_with_llm(new_anns)

    # Step 4.5: order_limits.csv 自动更新
    order_limit_changes = []
    log(f"\n[Step 4.5/6] 检测 minoq/maxoq 变更 → 更新 order_limits.csv...")
    if HAS_ORDER_LIMITS_UPDATER and new_anns:
        order_limit_changes = process_minoq_maxoq_change(new_anns)
        if order_limit_changes:
            log(f"  ✅ 自动更新 {len(order_limit_changes)} 条")
            for line in order_limit_changes:
                log(f"     {line}")
        else:
            log("  ℹ️ 未检测到 minoq/maxoq 变更")

    # Step 5: 告警
    log(f"\n[Step 5/6] 发送告警 ({len(new_anns)} 条)...")
    if not dry_run:
        send_feishu_alert(new_anns, order_limit_changes)
        send_email_alert(new_anns)
    else:
        log("(DRY RUN 跳过告警)")
        for ann in new_anns:
            log(f"  告警待发: [{ann['exchange_short']}] {ann['title'][:80]}")
            log(f"    {ann['url']}")

    # 更新状态
    all_ids = known_ids | {ann["id"] for ann in new_anns}
    state["known_ids"] = list(all_ids)
    state["last_check"] = datetime.now(timezone.utc).isoformat()
    state["last_result"] = f"found_{len(new_anns)}"
    state["announcements"] = state.get("announcements", {})
    for ann in new_anns:
        state["announcements"][ann["id"]] = {
            "title": ann["title"],
            "url": ann["url"],
            "exchange": ann["exchange_short"],
            "found_at": datetime.now(timezone.utc).isoformat(),
            "matched_keywords": ann.get("matched_keywords", []),
        }
    save_state(state)

    # Step 6: 日志汇总
    log(f"\n✅ 完成: 发现 {len(new_anns)} 条新公告")
    if order_limit_changes:
        log(f"   order_limits.csv: {len(order_limit_changes)} 条自动更新")
    log(f"   已发送告警")


def main():
    parser = argparse.ArgumentParser(description="NotifierWatchdog — 交易所公告监控")
    parser.add_argument("--days", type=int, default=7, help="回溯天数")
    parser.add_argument("--force", action="store_true", help="强制重新检查已见过的公告")
    parser.add_argument("--dry-run", action="store_true", help="只输出不告警")
    args = parser.parse_args()

    run(days_back=args.days, force=args.force, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
