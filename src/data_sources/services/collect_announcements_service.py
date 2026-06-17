#!/usr/bin/env python3
"""
单次公告采集服务

每次启动只跑一轮，采集 CFFEX、DCE、GFEX、INE、SHFE 五家期货交易所公告后退出。
由外部 cron / systemd timer 控制周期。

采集策略 (按交易所):
  - CFFEX: Playwright 直采 (jysgz 规则页 + jysgg/jystz 分页)
  - DCE: CMS API (columnId=244)
  - GFEX: requests 直采 (静态 HTML)
  - INE: Playwright 直采 (SafeLine WAF, 同 SHFE 架构)
  - SHFE: Playwright 直采 (SafeLine WAF)

输出:
  CFFEX 规则文档:            data/raw/announcements/exchanges/CFFEX/...
  DCE 公告:                  data/raw/announcements/exchanges/DCE/...
  metadata:                  data/raw/announcements/announcements_metadata.json

用法 (通过 service_manager.sh):
  bash scripts/service_manager.sh start
  bash scripts/service_manager.sh stop
  bash scripts/service_manager.sh status

所需环境变量 (由 service_manager.sh 注入):
  DATA_DIR                 公告文件存储目录
  LOG_DIR                  日志目录
  TAVILY_API_KEY           Tavily API 密钥 (DCE 规则类 Tavily 回退用)
  DCE_API_KEY              DCE CMS API 密钥
  DCE_API_SECRET           DCE CMS API 密钥
"""

import fcntl
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

from playwright.sync_api import sync_playwright
from mxz_utils.logging_config import get_logger


def _tavily_key() -> str:
    return os.environ["TAVILY_API_KEY"]


def _now_str() -> str:
    """当前时间 +08:00 格式字符串."""
    return datetime.now(
        timezone(timedelta(hours=8))
    ).strftime("%Y%m%dT%H:%M:%S+08:00")


# ── 路径 (由 service_manager.sh 传入) ──
DATA_DIR = Path(os.environ["DATA_DIR"])
LOG_DIR = Path(os.environ["LOG_DIR"])
METADATA_FILE = DATA_DIR / "raw" / "announcements" / "announcements_metadata.json"
ANNOUNCEMENTS_DIR = DATA_DIR / "raw" / "announcements"

# ── Logger ────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger = get_logger(
    name="CollectAnnouncementsService",
    level="DEBUG",
    dirpath_logs=str(LOG_DIR),
    logfile_basename="CollectAnnouncementsService",
)


# ── 交易所目录结构 (共享) ──────────────────────────
# 所有交易所: exchanges/{EXCHANGE}/{general,product,daily}/
EXCHANGE_CATEGORIES = {"general", "product", "daily"}

def _exchange_dir(exchange: str) -> Path:
    return DATA_DIR / "exchanges" / exchange

# ── CFFEX ────────────────────────────────────────────
CFFEX_BASE = _exchange_dir("CFFEX")

# CFFEX 规则页 (general + product 来源)
CFFEX_RULES_URL = "http://www.cffex.com.cn/cn/jysgz.html"

# CFFEX 日常公告列表 (翻页: {base}_{N}.html)
CFFEX_DAILY_PAGES = [
    "http://www.cffex.com.cn/cn/jysgg.html",   # 交易所公告
    "http://www.cffex.com.cn/cn/jystz.html",   # 交易所通知
]
DAILY_START_DATE = os.environ.get("DAILY_START_DATE", "20251201")

# CFFEX 规则页中需要跳过的栏目
CFFEX_SKIP_SECTIONS = {"已废止业务规则", "历史版本"}

# ── DCE ──────────────────────────────────────────────
DCE_BASE = _exchange_dir("DCE")

# DCE 规则类频道 (一次性全量, 无日期过滤)
DCE_RULES_CHANNELS = [
    # (channel_id, page_count, description, category)
    (7000236, 1, "章程", "general"),
    (149,     1, "交易所规则和结算规则", "general"),
    (150,     3, "业务办法", "general"),
    (151,     3, "品种细则", "product"),
    (152,     1, "其它相关规定", "general"),
]

# DCE 业务公告与通知 (daily, 需日期过滤 ≥ DAILY_START_DATE)
DCE_API_COLUMN_244 = 244  # CMS API 对应的栏目 (已验证有权限)
DCE_MAX_PAGES = 500

# DCE CMS API 端点
DCE_CMS_TOKEN_URL = "http://www.dce.com.cn/dceapi/cms/auth/accessToken"
DCE_CMS_ARTICLE_URL = "http://www.dce.com.cn/dceapi/cms/info/articleByPage"




# ══════════════════════════════════════════════════════
#  下载级简单验证 (基于 HTTP 响应, 不开文件分析内容)
# ══════════════════════════════════════════════════════

_download_failures: list[dict] = []


def _check_download(html: str, exchange: str, aid: str, title: str, url: str = "") -> str | None:
    """协议级检查: 响应是否空/截断/404.

    返回 None = 通过, 返回 str = 失败原因.
    """
    if not html or len(html) < 200:
        _download_failures.append(dict(
            exchange=exchange, aid=aid, title=title[:60], url=url,
            reason="响应体过短",
        ))
        return "响应体过短"

    # 仅检查 <title> 中的 404, 不扫描 body 关键词
    title_tag = re.search(r"<title>([^<]+)</title>", html)
    if title_tag and "404" in title_tag.group(1):
        _download_failures.append(dict(
            exchange=exchange, aid=aid, title=title[:60], url=url,
            reason="404",
        ))
        return "404"

    return None


def _log_download_failures():
    """采集结束时打印下载级验证异常汇总."""
    if not _download_failures:
        logger.info("[下载验证] 全部通过")
        return

    logger.warning("[下载验证] 异常: %s 条", len(_download_failures))
    by_ex = {}
    for f in _download_failures:
        by_ex.setdefault(f["exchange"], []).append(f)
    for ex, items in sorted(by_ex.items()):
        logger.warning("  %s: %s 条", ex, len(items))
        for item in items:
            logger.warning(
                "    [%s] %s | %s | %s",
                item["reason"], item["aid"], item["title"], item.get("url", ""),
            )
    _download_failures.clear()


# ══════════════════════════════════════════════════════
#  文件锁 + Metadata (key → record JSON dict)
# ══════════════════════════════════════════════════════

def _read_locked(filepath: Path):
    """带共享锁读取 JSON 文件。"""
    if not filepath.exists():
        return {}
    with open(filepath, "r", encoding="utf-8") as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        try:
            return json.load(f)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _locked_write_html(filepath: Path, html: str):
    """带排他锁写入 HTML 公告文件。"""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.write(html)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def load_metadata() -> dict[str, dict]:
    """加载 metadata (key → record)。"""
    return _read_locked(METADATA_FILE)


def upsert_record(key: str, record: dict):
    """插入或更新单条公告记录（读-改-写，带锁保护）。"""
    # 用写锁保护整个 read-modify-write
    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(METADATA_FILE, "a+", encoding="utf-8") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.seek(0)
            raw = f.read()
            meta = json.loads(raw) if raw.strip() else {}
            meta[key] = record
            f.seek(0)
            f.truncate()
            json.dump(meta, f, ensure_ascii=False, indent=2)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def clean_orphans(meta: dict[str, dict]):
    """删除 DATA_DIR 中存在但 metadata 中无记录的文件."""
    if not ANNOUNCEMENTS_DIR.exists():
        return

    meta_paths: set[str] = set()
    for rec in meta.values():
        sf = rec["source_file"]
        if sf:
            meta_paths.add(sf)

    removed = 0
    for fpath in ANNOUNCEMENTS_DIR.rglob("*.html"):
        if fpath.is_file():
            rel = str(fpath.relative_to(Path.cwd()))
            if rel not in meta_paths:
                fpath.unlink()
                removed += 1
                logger.info("  [clean] 删除孤儿文件: %s", rel)

    if removed:
        logger.info("[clean] 共删除 %s 个孤儿文件", removed)

    # 清理空目录
    for dirpath in sorted(ANNOUNCEMENTS_DIR.rglob("*"), reverse=True):
        if dirpath.is_dir() and not any(dirpath.iterdir()):
            dirpath.rmdir()


# ══════════════════════════════════════════════════════
#  CFFEX Playwright 采集
# ══════════════════════════════════════════════════════


def _extract_url_date_id(url: str) -> tuple[str, str]:
    """从 URL 提取日期 (YYYYMMDD) 和文章 ID.

    /cn/jysgg/20260522/47900.html → ('20260522', '47900')
    """
    m = re.search(r'/(\d{8})/(\d+)\.html?$', url)
    if m:
        return m.group(1), m.group(2)
    # Fallback: 仅文章 ID
    m = re.search(r'/(\d+)\.html?$', url)
    return ("00000000", m.group(1) if m else _compute_id(url))


def _sanitize_filename(name: str, max_len: int = 80) -> str:
    """清理文件名中的非法字符。"""
    name = re.sub(r'[\\/:*?"<>|]', '', name)
    name = re.sub(r'\s+', '', name)
    if len(name) > max_len:
        name = name[:max_len]
    return name


def _cffex_article_id(url: str, exchange: str = "CFFEX") -> str:
    """生成 CFFEX 文章唯一 ID: CFFEX_20100326_10719."""
    url_date, url_id = _extract_url_date_id(url)
    return f"{exchange}_{url_date}_{url_id}"


def _build_cffex_filename(article: dict) -> str:
    """构建 CFFEX 公告文件名.

    格式: CFFEX_YYYYMMDD_URLID.html
    中文标题保留在 metadata, 不在文件名中 (避免跨平台兼容问题).
    """
    exchange = article.get("exchange", "CFFEX")
    url = article.get("url", "")

    url_date, url_id = _extract_url_date_id(url)
    pub_date = article.get("pub_date", "")
    if pub_date:
        pub_date = pub_date.replace("-", "")  # 2026-05-19 → 20260519
    else:
        pub_date = url_date

    return f"{exchange}_{pub_date}_{url_id}.html"


def _classify_cffex(title: str, section: str = "") -> str:
    """根据标题/栏目分类 CFFEX 文章."""
    if section in ("jysgg", "jystz"):
        return "daily"
    if section == "实施细则":
        if any(kw in title for kw in ("期货合约", "期权合约", "交割", "期转现")):
            return "product"
        return "general"
    # 交易规则和结算规则 / 业务指引 / 业务通知 → general
    return "general"


def _parse_cffex_rules_page(html: str) -> list[dict]:
    """解析 /cn/jysgz.html, 提取 general + product 规则链接."""
    articles = []

    # 按 head_title div 切割各栏目
    for m in re.finditer(
        r'<div class="head_title"[^>]*>\s*(?:<span[^>]*></span>)?\s*'
        r'([^<]+)\s*</div>(.*?)(?=<div class="head_title"|\Z)',
        html, re.DOTALL
    ):
        section_name = m.group(1).strip()
        section_html = m.group(2)

        if section_name in CFFEX_SKIP_SECTIONS:
            continue

        # 提取栏目下所有 list_a_text 链接
        for lm in re.finditer(
            r'<a class="list_a_text[^"]*"\s+href="(/cn/[^"]+)"[^>]*>'
            r'\s*([^<]+?)\s*</a>',
            section_html, re.DOTALL
        ):
            href = lm.group(1)
            title = lm.group(2).strip()
            url = f"http://www.cffex.com.cn{href}"
            category = _classify_cffex(title, section=section_name)

            # 从 URL 提取发布日期 (如 /cn/jystz/20100326/10719.html → 20100326)
            url_date, _ = _extract_url_date_id(url)

            articles.append({
                "title": title,
                "url": url,
                "exchange": "CFFEX",
                "category": category,
                "source": "jysgz",
                "pub_date": url_date,
            })

    # 统计
    for cat in ("general", "product"):
        cnt = sum(1 for a in articles if a["category"] == cat)
        logger.info("[CFFEX] jysgz → %s: %s 条", cat, cnt)
    return articles


def _parse_cffex_daily_pages(ctx, base_url: str) -> list[dict]:
    """翻页解析 daily 公告列表, 日期 < DAILY_START_DATE 则停止翻页."""
    articles = []
    prefix = base_url.rsplit(".", 1)[0]  # 剥离 .html 后缀
    source = "jysgg" if "jysgg" in base_url else "jystz"

    for page_num in range(1, 100):  # 安全上限
        url = base_url if page_num == 1 else f"{prefix}_{page_num}.html"

        page = ctx.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(2)
            html = page.content()
        except Exception:
            logger.warning("[CFFEX] 翻页 %s 加载失败, 停止", url)
            page.close()
            break
        page.close()

        # 提取文章: 标题从 HTML, 日期从 URL
        page_articles = []
        all_before_start = True
        for m in re.finditer(
            r'<a class="list_a_text[^"]*"\s+href="(/cn/[^"]+)"[^>]*>'
            r'\s*([^<]+?)\s*</a>',
            html, re.DOTALL
        ):
            href = m.group(1)
            title = m.group(2).strip()
            url_full = f"http://www.cffex.com.cn{href}"

            # 日期从 URL 提取 (/cn/jysgg/20260522/47900.html → 20260522)
            pub_date, _ = _extract_url_date_id(url_full)

            if pub_date >= DAILY_START_DATE:
                all_before_start = False
                page_articles.append({
                    "title": title,
                    "url": url_full,
                    "pub_date": pub_date,
                    "exchange": "CFFEX",
                    "category": "daily",
                    "source": source,
                })

        articles.extend(page_articles)
        logger.debug(
            "[CFFEX] %s 第%d页: %d 条 (>= %s)",
            source, page_num, len(page_articles), DAILY_START_DATE
        )

        if all_before_start:
            break

    logger.info("[CFFEX] %s 共 %s 条 (翻 %s 页)", source, len(articles), page_num)
    return articles


def _download_cffex_articles(ctx, articles: list[dict], meta: dict):
    """下载 CFFEX 文章详情页, 保存到 exchanges/CFFEX/{category}/ 目录.

    文件命名: CFFEX_YYYYMMDD_URLID_标题.html
    """
    new_articles = [
        a for a in articles
        if _cffex_article_id(a["url"]) not in meta
    ]
    if not new_articles:
        logger.info("[CFFEX] 无新文章")
        return 0

    downloaded = 0
    for a in new_articles:
        aid = _cffex_article_id(a["url"])
        category = a.get("category", "unknown")
        filename = _build_cffex_filename(a)
        filepath = CFFEX_BASE / category / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)

        try:
            page = ctx.new_page()
            page.goto(a["url"], wait_until="domcontentloaded", timeout=25000)
            time.sleep(2)
            html = page.content()
            page.close()

            if len(html) > 500:
                if _check_download(html, "CFFEX", aid, a.get("title", ""), a.get("url", "")):
                    continue
                _locked_write_html(filepath, html)

                rec = {
                    "id": aid,
                    "url": a["url"],
                    "title": a.get("title", ""),
                    "exchange": "CFFEX",
                    "category": category,
                    "pub_date": a.get("pub_date", ""),
                    "source_file": str(filepath.relative_to(Path.cwd())),
                    "status": "downloaded",
                    "downloaded_at": _now_str(),
                }
                upsert_record(aid, rec)
                meta[aid] = rec
                downloaded += 1
                logger.info(
                    "  ✓ [%7s] %s | %s",
                    category, aid[:8], a['title'][:50],
                )
        except Exception:
            logger.exception(
                "  ✗ [%7s] %s | %s",
                category, aid[:8], a.get('title', '?')[:40],
            )

    logger.info("[CFFEX] 下载完成: %s/%s 成功", downloaded, len(new_articles))
    return downloaded


def collect_cffex():
    """CFFEX 全量采集: general + product (jysgz) + daily (jysgg + jystz 翻页)."""

    meta = load_metadata()

    with sync_playwright() as p:
        launch_kwargs = {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
            ],
        }

        browser = p.chromium.launch(**launch_kwargs)
        ctx = browser.new_context(locale="zh-CN")

        try:
            # ── 1. jysgz → general + product ──
            logger.info("[CFFEX] 采集规则页 (jysgz)")
            page = ctx.new_page()
            page.goto(
                CFFEX_RULES_URL,
                wait_until="domcontentloaded",
                timeout=30000,
            )
            time.sleep(2)
            rules_html = page.content()
            page.close()

            rules_articles = _parse_cffex_rules_page(rules_html)
            _download_cffex_articles(ctx, rules_articles, meta)

            # ── 2. jysgg + jystz 翻页 → daily ──
            for base_url in CFFEX_DAILY_PAGES:
                source = "jysgg" if "jysgg" in base_url else "jystz"
                logger.info("[CFFEX] 采集 %s (翻页)", source)
                daily_articles = _parse_cffex_daily_pages(ctx, base_url)
                _download_cffex_articles(ctx, daily_articles, meta)

            logger.info("[CFFEX] 采集完成")
        finally:
            ctx.close()
            browser.close()


# ══════════════════════════════════════════════════════
#  DCE 采集
#  策略: daily = CMS API (columnId=244)
#        rules = Tavily Extract (无 API 通道)
# ══════════════════════════════════════════════════════

def _dce_extract_date_id(url: str, pub_date: str = "") -> tuple[str, str]:
    """从 DCE URL 提取 (YYYYMMDD, article_id).

    规则类 URL: /dce/content/2018/zchjygz/6146499.html  → ('2018MMDD', '6146499')
    日常类 URL: 从标题中的 YYYY-MM-DD 取日期
    """
    m = re.search(r'/(\d+)\.html?$', url)
    article_id = m.group(1) if m else _compute_id(url)

    if pub_date:
        return pub_date.replace("-", ""), article_id

    # Fallback: 从 URL 路径提取日期 (YYYY / YYYYMM / YYYYMMDD)
    # 缺失部分用字面占位: YYYY → YYYYMMDD, YYYYMM → YYYYMMDD
    m = re.search(r'/content/(\d{4,8})/', url)
    if m:
        d = m.group(1)
        if len(d) == 8:            # YYYYMMDD → 直接使用
            return (d, article_id)
        elif len(d) == 6:          # YYYYMM → 补 DD
            return (f"{d}DD", article_id)
        else:                      # YYYY → 补 MMDD
            return (f"{d}MMDD", article_id)

    return ("YYYYMMDD", article_id)


def _dce_article_id(url: str, pub_date: str = "") -> str:
    """生成 DCE 文章唯一 ID: DCE_YYYYMMDD_articleId."""
    d, aid = _dce_extract_date_id(url, pub_date)
    return f"DCE_{d}_{aid}"


def _dce_build_filename(article: dict) -> str:
    """构建 DCE 公告文件名: DCE_YYYYMMDD_ID.html."""
    exchange = article.get("exchange", "DCE")
    url = article.get("url", "")
    pub_date = article.get("pub_date", "")
    d, aid = _dce_extract_date_id(url, pub_date)
    return f"{exchange}_{d}_{aid}.html"


# ══════════════════════════════════════════════════════
#  DCE CMS API (columnId=244 已验证可用)
# ══════════════════════════════════════════════════════

_dce_token_cache = {"token": None, "expiry": 0.0}


def _dce_get_token() -> str | None:
    """获取 DCE CMS API Bearer token, 缓存 30 分钟.

    凭证从环境变量 DCE_API_KEY / DCE_API_SECRET 读取.
    返回 None 表示失败 (环境变量缺失/API不通).
    """
    now = time.time()
    if _dce_token_cache["token"] and now < _dce_token_cache["expiry"]:
        return _dce_token_cache["token"]

    try:
        key = os.environ["DCE_API_KEY"]
        secret = os.environ["DCE_API_SECRET"]
    except KeyError as e:
        logger.warning("[DCE] API: 缺少环境变量 %s, 无法使用 API 通道", e)
        return None

    try:
        resp = requests.post(
            DCE_CMS_TOKEN_URL,
            headers={"apikey": key, "Content-Type": "application/json"},
            json={"secret": secret},
            timeout=15,
        )
        data = resp.json()
        if data.get("success"):
            token = data["data"]["token"]
            _dce_token_cache["token"] = token
            _dce_token_cache["expiry"] = now + 1800
            logger.debug("[DCE] API token 获取成功")
            return token
        logger.warning("[DCE] API token 失败: %s", data.get('msg'))
        return None
    except Exception:
        logger.exception("[DCE] API token 获取异常")
        return None


def _dce_api_fetch_page(
    column_id: int, page_no: int, page_size: int = 20
) -> list[dict] | None:
    """通过 DCE CMS API 获取一页公告列表.

    返回 article 列表, 每项含:
      id / title / pub_date / show_date / content (完整HTML) / url / source

    返回 None 表示 API 不可用 (环境变量缺失/无权限/异常).
    返回空列表 [] 表示该页无数据.

    注意: 请求 CMS 接口必须同时传 apikey 头 + Authorization Bearer,
    缺少任一都会返回 402/401 (详见 devlog §8.2).
    """
    key = os.environ["DCE_API_KEY"]
    token = _dce_get_token()
    if not token:
        return None

    try:
        resp = requests.post(
            DCE_CMS_ARTICLE_URL,
            headers={
                "apikey": key,
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "columnId": str(column_id),
                "pageNo": page_no,
                "siteId": 5,
                "pageSize": page_size,
            },
            timeout=15,
        )
        data = resp.json()
        if not data.get("success"):
            logger.warning(
                "[DCE] API 第%s页失败: %s (code=%s)",
                page_no, data.get("msg"), data.get("code"),
            )
            return None

        rows = data["data"].get("resultList", [])
        articles = []
        for row in rows:
            show_date = (
                row["showDate"][:10].replace("-", "")
            )  # "2026-06-09" → "20260609"
            static_url = row["articleStaticUrl"]
            articles.append({
                "id": row["id"],
                "title": row["title"],
                "pub_date": show_date,
                "show_date": row["showDate"],
                "content": row["content"],
                "url": (
                "http://www.dce.com.cn/dce/" + static_url
                if static_url else ""
            ),
                "source": row.get("sourceId", ""),
                "exchange": "DCE",
            })
        return articles
    except Exception:
        logger.exception("[DCE] API 第%s页异常", page_no)
        return None


def _dce_tavily_extract(url: str) -> str:
    """用 Tavily Extract 获取单页 raw_content."""
    try:
        resp = requests.post(
            "https://api.tavily.com/extract",
            json={
            "api_key": _tavily_key(),
            "urls": [url],
            "extract_depth": "advanced",
        },
            timeout=60,
        )
        if resp.status_code != 200:
            logger.warning(
            "[DCE] Tavily HTTP %s: %s",
            resp.status_code, url[:70],
        )
            return ""
        data = resp.json()
        results = data.get("results", [])
        if results:
            return results[0].get("raw_content", "")
    except Exception:
        logger.exception("[DCE] Tavily 异常: %s", url[:70])
    return ""


def _dce_extract_rules_articles(
    channel_id: int, page_num: int,
) -> list[dict]:
    """提取 DCE 规则类频道某页的文章列表.

    Tavily 链接格式: [标题](http://www.dce.com.cn/dce/content/YYYY/xxx/ID.html)
    """
    url = f"http://www.dce.com.cn/dce/channel/list/{channel_id}.html"
    if page_num > 1:
        url = ("http://www.dce.com.cn/dce/channel/list/"
         f"{channel_id}_{page_num}.html")

    content = _dce_tavily_extract(url)
    if not content:
        return []

    # 规则类链接不含日期: [标题](url)
    # 排除 footer 链接
    skip_titles = {"法律声明", "个人信息保护政策"}
    articles = []
    dce_re = re.compile(
        r'\[([^\]]+?)\]\s*\('
        r'(http://www\.dce\.com\.cn/dce/content/[^)]+\.html)\)'
    )
    for m in dce_re.finditer(content):
        title = m.group(1).strip()
        url_found = m.group(2)
        if title in skip_titles:
            continue
        articles.append({
            "title": title,
            "url": url_found,
            "exchange": "DCE",
        })

    return articles


def _dce_download_articles(
    articles: list[dict],
    meta: dict, dce_category: str,
) -> int:
    """用 Tavily Extract 批量下载 DCE 文章详情页.

    由于 DCE WAF 阻挡 Playwright, 使用 Tavily 提取详情页 raw_content
    存入 exchanges/DCE/{category}/ 目录.

    去重策略 (双重校验):
    1. metadata 中已存在该 article_id → 跳过
    2. 目标文件已存在于磁盘 → 跳过 (防 metadata 丢失后重复下载)
    """
    new_articles = []
    for a in articles:
        aid = _dce_article_id(a["url"], a.get("pub_date", ""))
        if aid in meta:
            continue
        new_articles.append(a)

    if not new_articles:
        logger.info("[DCE/%s] 无新文章", dce_category)
        return 0

    downloaded = 0
    # 分批: Tavily 单次最多 20 个 URL
    batch_size = 20
    for i in range(0, len(new_articles), batch_size):
        batch = new_articles[i:i + batch_size]
        urls = [a["url"] for a in batch]

        try:
            resp = requests.post(
                "https://api.tavily.com/extract",
                json={
                "api_key": _tavily_key(),
                "urls": urls,
                "extract_depth": "advanced",
            },
                timeout=120,
            )
            if resp.status_code != 200:
                logger.warning(
                "[DCE/%s] Tavily 批量下载 HTTP %s",
                dce_category, resp.status_code,
            )
                continue

            data = resp.json()
            results = data.get("results", [])
            # 建立 url → raw_content 映射
            url_to_content = {}
            for r in results:
                url_to_content[r.get("url", "")] = r.get("raw_content", "")

            for a in batch:
                aid = _dce_article_id(a["url"], a.get("pub_date", ""))
                raw_content = url_to_content.get(a["url"], "")
                if len(raw_content) < 500:
                    logger.warning(
                    "[DCE/%s] 内容过短: %s (%s chars)",
                    dce_category, aid, len(raw_content),
                )
                    continue

                filename = _dce_build_filename(a)
                filepath = DCE_BASE / dce_category / filename
                filepath.parent.mkdir(parents=True, exist_ok=True)

                if _check_download(raw_content, "DCE", aid, a.get("title", ""), a.get("url", "")):
                    continue

                _locked_write_html(filepath, raw_content)

                rec = {
                    "id": aid,
                    "url": a["url"],
                    "title": a.get("title", ""),
                    "exchange": "DCE",
                    "category": dce_category,
                    "pub_date": a.get("pub_date") or _dce_extract_date_id(a["url"], "")[0],
                    "source_file": str(filepath.relative_to(Path.cwd())),
                    "status": "downloaded",
                    "note": "Tavily 提取的文本内容 (非原始 HTML, 因 WAF 阻挡)",
                    "downloaded_at": _now_str(),
                }
                upsert_record(aid, rec)
                meta[aid] = rec
                downloaded += 1
                logger.info(
                "  ✓ [%7s] %s | %s",
                dce_category, aid, a["title"][:50],
            )

        except Exception:
            logger.exception("[DCE/%s] 批量下载异常", dce_category)

    logger.info(
    "[DCE/%s] 下载完成: %s/%s 成功",
    dce_category, downloaded, len(new_articles),
)
    return downloaded


def _dce_api_save_articles(
    articles: list[dict], meta: dict, dce_category: str,
) -> int:
    """保存 CMS API 返回的文章 (含完整 HTML 内容).

    API 返回的 content 字段已是完整 HTML, 直接写入文件.
    """
    saved = 0
    for a in articles:
        aid = _dce_article_id(a["url"], a.get("pub_date", ""))
        if aid in meta:
            continue

        content_html = a.get("content", "")
        if not content_html or len(content_html) < 100:
            logger.warning("[DCE/API] 内容过短: %s", aid)
            continue

        if _check_download(content_html, "DCE", aid, a.get("title", ""), a.get("url", "")):
            continue

        filename = _dce_build_filename(a)
        filepath = DCE_BASE / dce_category / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)

        _locked_write_html(filepath, content_html)

        rec = {
            "id": aid,
            "url": a["url"],
            "title": a.get("title", ""),
            "exchange": "DCE",
            "category": dce_category,
            "pub_date": a.get("pub_date") or a.get("showDate", ""),
            "source_file": str(filepath.relative_to(Path.cwd())),
            "status": "downloaded",
            "note": "DCE CMS API 返回的原始 HTML",
            "downloaded_at": _now_str(),
        }
        upsert_record(aid, rec)
        meta[aid] = rec
        saved += 1
        logger.info("  ✓ [API/%s] %s | %s", dce_category, aid, a['title'][:50])

    return saved


def _dce_collect_daily_via_api() -> list[dict]:
    """通过 CMS API (columnId=244) 采集日常公告.

    返回待保存的文章列表 (已去重). 返回空列表表示 API 不可用.
    注意: 日期过滤 ≥ DAILY_START_DATE 在此函数内完成.
    """
    logger.info("[DCE] 尝试 CMS API 通道 (columnId=%s)", DCE_API_COLUMN_244)

    # 先测一页确认 API 可用
    first_page = _dce_api_fetch_page(DCE_API_COLUMN_244, 1, page_size=10)
    if first_page is None:
        logger.warning("[DCE] CMS API (columnId=%s) 不可用", DCE_API_COLUMN_244)
        return []

    if not first_page:
        logger.info("[DCE] CMS API 返回空, 无数据")
        return []

    logger.info(
    "[DCE] CMS API 可用, 首条: %s (%s)",
    first_page[0]["title"][:50], first_page[0]["pub_date"],
)

    all_articles = []
    for page_num in range(1, DCE_MAX_PAGES):
        articles = _dce_api_fetch_page(
            DCE_API_COLUMN_244, page_num, page_size=20
        )
        if not articles:  # None 或 []
            logger.info("[DCE] API 第%s页: 无数据, 停止", page_num)
            break

        for a in articles:
            pub = a.get("pub_date", "00000000")
            if pub < DAILY_START_DATE:
                # 公告由近→远排序, 首条过期即可停止翻页
                logger.info(
                    "[DCE] API 第%s页: 发现 %s < %s, 停止",
                    page_num, pub, DAILY_START_DATE,
                )
                return all_articles
            all_articles.append(a)

        logger.info(
            "[DCE] API 第%s页: %s 条 (均 ≥ %s)",
            page_num, len(articles), DAILY_START_DATE,
        )

    return all_articles


def collect_dce():
    """DCE 全量采集.

    策略:
    - 规则类 (general/product): Tavily Extract
    - 日常公告 (daily): CMS API (columnId=244)
    """
    meta = load_metadata()

    # ── 阶段 1: 规则类频道 (Tavily) ──
    logger.info("[DCE] 采集规则类频道 (Tavily)")
    for channel_id, page_count, desc, category in DCE_RULES_CHANNELS:
        logger.info(
        "[DCE]   %s (channel=%s, %s页)",
        desc, channel_id, page_count,
    )
        all_articles = []
        for page_num in range(1, page_count + 1):
            articles = _dce_extract_rules_articles(
                channel_id, page_num
            )
            for a in articles:
                a["channel"] = channel_id
                a["category"] = category
            all_articles.extend(articles)
            logger.info("  [DCE]   第%s页: %s 条", page_num, len(articles))

        logger.info("[DCE]   %s: 共 %s 条", desc, len(all_articles))
        _dce_download_articles(all_articles, meta, category)

    # ── 阶段 2: 日常公告 ──
    # CMS API (columnId=244, 已验证有权限)
    logger.info("[DCE] 采集业务公告与通知 (≥ %s)", DAILY_START_DATE)

    daily_articles = _dce_collect_daily_via_api()

    if daily_articles:
        logger.info("[DCE] 日常公告: 共 %s 条", len(daily_articles))
        saved = _dce_api_save_articles(daily_articles, meta, "daily")
        logger.info(
        "[DCE] 日常公告: 保存 %s 条 (去重跳过 %s)",
        saved, len(daily_articles) - saved,
    )
    else:
        logger.info("[DCE] 日常公告: 无新数据")

    logger.info("[DCE] 采集完成")


# ══════════════════════════════════════════════════════
#  GFEX requests 采集
# ══════════════════════════════════════════════════════

# GFEX 规则分类 → 目录映射
GFEX_RULES_CATEGORIES = {
    "jygz": "general",   # 交易规则和结算规则
    "ywbf": "general",   # 业务办法
    "qtxggd": "general", # 其他相关规定
    "pzxz": "product",   # 品种细则
    # "llbb" (历史版本) 不下载
}

GFEX_RULES_PAGES = {
    "jygz": "http://www.gfex.com.cn/gfex/jygz/list_notime.shtml",
    "ywbf": "http://www.gfex.com.cn/gfex/ywbf/list_notime.shtml",
    "qtxggd": "http://www.gfex.com.cn/gfex/qtxggd/list_notime.shtml",
    "pzxz": "http://www.gfex.com.cn/gfex/pzxz/list_notime.shtml",
}

GFEX_DAILY_LIST = "http://www.gfex.com.cn/gfex/tzts/list_yw.shtml"
GFEX_DAILY_PAGES = 41

GFEX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
}

GFEX_BASE = _exchange_dir("GFEX")


def _gfex_fetch(url: str) -> str | None:
    """GET 请求 GFEX 页面, 返回 HTML 或 None."""
    try:
        resp = requests.get(url, headers=GFEX_HEADERS, timeout=30)
        if resp.status_code != 200:
            logger.warning("[GFEX] HTTP %s: %s", resp.status_code, url)
            return None
        resp.encoding = "utf-8"  # GFEX 未返回 charset, requests 默认 ISO-8859-1
        return resp.text
    except Exception:
        logger.exception("[GFEX] 请求失败: %s", url)
        return None


def _gfex_article_id(url: str) -> str:
    """GFEX 文章唯一 ID, 仅从 URL 派生 (不受后续 pub_date 更新影响)."""
    m = re.search(r"/(\d{6})/([a-f0-9]+)\.s?html$", url)
    if m:
        return f"GFEX_{m.group(1)}_{m.group(2)[:8]}"
    return f"GFEX_{_compute_id(url)}"


def _gfex_extract_rules_articles() -> list[dict]:
    """从规则列表页提取文章 (静态 HTML), 支持翻页.

    分类:
      jygz (交易规则和结算规则)  → general
      ywbf (业务办法)            → general
      qtxggd (其他相关规定)      → general
      pzxz (品种细则)            → product
      llbb (历史版本)            → 不下载
    """
    pattern_map = {
        c: re.compile(
            r'<a[^>]*href="(/gfex/' + c + r'/\d{6}/[a-f0-9]+\.s?html)"'
            r'[^>]*>([^<]+)</a>'
        )
        for c in ("jygz", "ywbf", "qtxggd", "pzxz")
    }

    def _parse_page(cat_code: str, html: str) -> list[tuple[str, str]]:
        return re.findall(pattern_map[cat_code], html)

    def _get_page_count(html: str) -> int:
        """从 createPageHTML JS 调用解析总页数."""
        m = re.search(r'createPageHTML\([^,]+,\s*(\d+)', html)
        return int(m.group(1)) if m else 1

    articles = []
    for cat_code, category in GFEX_RULES_CATEGORIES.items():
        url = GFEX_RULES_PAGES[cat_code]
        html = _gfex_fetch(url)
        if not html:
            continue

        # 解析总页数
        total_pages = _get_page_count(html)
        total_items = 0

        for page_no in range(1, total_pages + 1):
            if page_no > 1:
                page_url = url.replace(".shtml", f"_{page_no}.shtml")
                page_html = _gfex_fetch(page_url)
                if not page_html:
                    break
            else:
                page_html = html

            items = _parse_page(cat_code, page_html)
            if not items:
                if page_no > 1:
                    break
                continue

            for href, title in items:
                full_url = f"http://www.gfex.com.cn{href}"
                articles.append({
                    "title": title.strip(),
                    "url": full_url,
                    "category": category,
                    "pub_date": href.split("/")[3],  # YYYYMM
                })
            total_items += len(items)

        logger.info(
            "[GFEX] 规则 [%7s] → %s: %s 条 (%s 页)",
            cat_code, category, total_items, total_pages,
        )

    return articles


def _gfex_extract_daily_articles() -> list[dict]:
    """遍历日常公告翻页 (list_yw), 提取文章列表."""
    articles = []
    for page_no in range(1, GFEX_DAILY_PAGES + 1):
        url = GFEX_DAILY_LIST if page_no == 1 else \
            f"http://www.gfex.com.cn/gfex/tzts/list_yw_{page_no}.shtml"

        html = _gfex_fetch(url)
        if not html:
            continue

        # 提取公告 item: 日期 + 标题 + URL
        items = re.findall(
            r'<li>.*?'
            r'<span class="dd">(\d+)</span>.*?'
            r'<span class="yyMM">(\d{4})\.(\d{2})</span>.*?'
            r'href="(/gfex/tzts/\d{6}/[a-f0-9]+\.s?html)"[^>]*>([^<]+)</a>',
            html, re.DOTALL
        )
        if not items:
            logger.warning("[GFEX] 第%s页未提取到公告", page_no)
            continue

        for dd, yy, mm, href, title in items:
            pub_date = f"{yy}{mm}{dd}"
            if pub_date < DAILY_START_DATE:
                continue
            articles.append({
                "title": title.strip(),
                "url": f"http://www.gfex.com.cn{href}",
                "pub_date": pub_date,
                "category": "daily",
            })

        logger.info("[GFEX] 第%s页: %s 条", page_no, len(items))

    return articles


def _gfex_download_articles(articles: list[dict], meta: dict) -> int:
    """用 requests 下载 GFEX 公告详情页."""
    new = [a for a in articles
           if _gfex_article_id(a["url"]) not in meta]
    if not new:
        return 0

    downloaded = 0
    for a in new:
        aid = _gfex_article_id(a["url"])
        category = a.get("category", "unknown")
        pub_date = a.get("pub_date", "")

        url = a["url"]
        m = re.search(r"([a-f0-9]+)\.s?html$", url)
        uuid_part = m.group(1)[:8] if m else aid[-8:]
        fname = f"GFEX_{pub_date}_{uuid_part}.html"
        filepath = GFEX_BASE / category / fname
        filepath.parent.mkdir(parents=True, exist_ok=True)

        html = _gfex_fetch(url)
        if html and len(html) > 500:
            # 从详情页提取完整发布日期
            m_pub = re.search(
                r'<meta name="PubDate" content="(\d{4})-(\d{2})-(\d{2})',
                html,
            )
            if m_pub:
                pub_date = f"{m_pub.group(1)}{m_pub.group(2)}{m_pub.group(3)}"
                fname = f"GFEX_{pub_date}_{uuid_part}.html"
                filepath = GFEX_BASE / category / fname

            if _check_download(html, "GFEX", aid, a.get("title", ""), a.get("url", "")):
                continue

            _locked_write_html(filepath, html)
            rec = {
                "id": aid,
                "url": url,
                "title": a.get("title", ""),
                "exchange": "GFEX",
                "category": category,
                "pub_date": pub_date,
                "source_file": str(filepath.relative_to(Path.cwd())),
                "status": "downloaded",
                "downloaded_at": _now_str(),
            }
            upsert_record(aid, rec)
            meta[aid] = rec
            downloaded += 1
            logger.info(
                "  ✓ [%7s] %s | %s",
                category, aid[:8], a["title"][:50],
            )
        else:
            logger.warning(
                "  ✗ [%7s] %s | %s (响应过短)",
                category, aid[:8], a["title"][:40],
            )

    return downloaded


def collect_gfex():
    """GFEX 全量采集: 规则类 (general/product) + 日常公告 (daily).

    全部使用 requests 而非 Playwright, 因为 GFEX 列表页和详情页均为静态 HTML.
    """
    meta = load_metadata()

    # ── 阶段 1: 规则类 (general + product) ──
    logger.info("[GFEX] 采集规则类 (lists_notime)")
    rules_articles = _gfex_extract_rules_articles()
    if rules_articles:
        ok = _gfex_download_articles(rules_articles, meta)
        logger.info("[GFEX] 规则类: 下载 %s/%s 条", ok, len(rules_articles))
    else:
        logger.info("[GFEX] 规则类: 无新数据")

    # ── 阶段 2: 日常公告 (daily) ──
    logger.info("[GFEX] 采集日常公告 (翻页, ≥ %s)", DAILY_START_DATE)
    daily_articles = _gfex_extract_daily_articles()
    if daily_articles:
        ok = _gfex_download_articles(daily_articles, meta)
        logger.info("[GFEX] 日常公告: 下载 %s/%s 条", ok, len(daily_articles))
    else:
        logger.warning("[GFEX] 日常公告: 未发现新公告")

    logger.info("[GFEX] 采集完成")


# ══════════════════════════════════════════════════════
#  INE Playwright 采集 (同 SHFE 架构)
# ══════════════════════════════════════════════════════

INE_BASE = _exchange_dir("INE")

INE_RULES_URL = "https://www.ine.cn/regulation/ineregulation/rules/"
INE_NOTICES_URL = "https://www.ine.cn/publicnotice/notice/"


def _ine_article_id(url: str) -> str:
    """INE 文章 ID: INE_YYYYMMDD_NNNNN."""
    m = re.search(r"/t(\d{8})_(\d+)\.html$", url)
    if m:
        return f"INE_{m.group(1)}_{m.group(2)}"
    return f"INE_{_compute_id(url)}"


def _ine_classify(title: str) -> str:
    """根据标题分类 INE 文章 (general/product)."""
    product_keywords = ["期货合约", "期货标准合约", "期权合约"]
    if any(kw in title for kw in product_keywords):
        return "product"
    return "general"


def _ine_extract_list(page) -> list[tuple[str, str]]:
    """在 Playwright page 中提取列表页链接, 返回 [(href, title), ...]."""
    html = page.content()
    return re.findall(
        r'<a[^>]*href="\./(\d{6}/t\d{8}_\d+\.html)"[^>]*>([^<]+)</a>',
        html,
    )


def _ine_extract_rules(page) -> list[dict]:
    """Playwright 翻页提取规则文章 (general + product)."""
    articles = []
    page_idx = None  # None=首页, 1=index_1, 2=index_2 ...
    while True:
        url = (INE_RULES_URL if page_idx is None
               else f"{INE_RULES_URL}index_{page_idx}.html")
        page.goto(url, wait_until="networkidle", timeout=30000)
        time.sleep(1)
        items = _ine_extract_list(page)
        if not items:
            break
        for href, title in items:
            m = re.search(r"t(\d{8})_(\d+)", href)
            pub_date = m.group(1) if m else "00000000"
            full_url = f"{INE_RULES_URL}{href}"
            articles.append({
                "title": title.strip(),
                "url": full_url,
                "pub_date": pub_date,
                "category": _ine_classify(title),
            })
        pg_label = "1" if page_idx is None else str(page_idx + 1)
        logger.info("[INE] 规则第%s页: %s 条", pg_label, len(items))
        if page_idx is None:
            page_idx = 1
        else:
            page_idx += 1
    return articles


def _ine_extract_daily(page) -> list[dict]:
    """Playwright 翻页提取公告通知, 日期 < DAILY_START_DATE 停止."""
    articles = []
    page_idx = None
    stopped = False
    while not stopped:
        url = (INE_NOTICES_URL if page_idx is None
               else f"{INE_NOTICES_URL}index_{page_idx}.html")
        page.goto(url, wait_until="networkidle", timeout=30000)
        time.sleep(1)
        items = _ine_extract_list(page)
        if not items:
            break
        page_count = 0
        for href, title in items:
            m = re.search(r"t(\d{8})_(\d+)", href)
            pub_date = m.group(1) if m else "00000000"
            if pub_date < DAILY_START_DATE:
                stopped = True
                break
            full_url = f"{INE_NOTICES_URL}{href}"
            articles.append({
                "title": title.strip(),
                "url": full_url,
                "pub_date": pub_date,
                "category": "daily",
            })
            page_count += 1
        pg_label = "1" if page_idx is None else str(page_idx + 1)
        logger.info("[INE] 公告第%s页: %s 条", pg_label, page_count)
        if page_idx is None:
            page_idx = 1
        else:
            page_idx += 1
    return articles


def collect_ine():
    """INE 全量采集: 规则类 (general+product) + 公告通知.

    全部用 Playwright (SafeLine WAF, 同 SHFE 架构).
    """
    meta = load_metadata()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        ctx = browser.new_context(locale="zh-CN")
        page = ctx.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', "
            "{get: () => undefined})"
        )

        # 规则页过 SafeLine WAF
        page.goto(INE_RULES_URL, wait_until="networkidle", timeout=30000)
        time.sleep(2)

        # ── 规则类 ──
        logger.info("[INE] 采集规则类")
        rules = _ine_extract_rules(page)
        if rules:
            new = [a for a in rules
                   if _ine_article_id(a["url"]) not in meta]
            for a in new:
                aid = _ine_article_id(a["url"])
                cat = a["category"]
                pd = a["pub_date"]
                url = a["url"]
                m = re.search(r"(\d+)\.html$", url)
                url_id = m.group(1) if m else aid[-8:]
                fname = f"INE_{pd}_{url_id}.html"
                fp = INE_BASE / cat / fname
                fp.parent.mkdir(parents=True, exist_ok=True)
                try:
                    page.goto(url, wait_until="networkidle", timeout=30000)
                    time.sleep(1)
                    html = page.content()
                    if len(html) > 500:
                        if _check_download(html, "INE", aid, a["title"], a.get("url", "")):
                            continue
                        _locked_write_html(fp, html)
                        upsert_record(aid, {
                            "id": aid,
                            "url": url,
                            "title": a["title"],
                            "exchange": "INE",
                            "category": cat,
                            "pub_date": pd,
                            "source_file": str(fp.relative_to(Path.cwd())),
                            "status": "downloaded",
                            "downloaded_at": _now_str(),
                        })
                        meta[aid] = True
                        logger.info("  ✓ [%7s] %s | %s", cat, aid,
                                     a["title"][:50])
                except Exception:
                    logger.exception("  ✗ [%s] %s | %s",
                                     cat, aid, a["title"][:40])
            logger.info("[INE] 规则类: 下载 %s/%s 条",
                         sum(1 for a in new
                             if _ine_article_id(a["url"]) in meta),
                         len(new))

        # ── 公告通知 ──
        logger.info("[INE] 采集公告通知 (≥ %s)", DAILY_START_DATE)
        daily = _ine_extract_daily(page)
        if daily:
            new_d = [a for a in daily
                     if _ine_article_id(a["url"]) not in meta]
            for a in new_d:
                aid = _ine_article_id(a["url"])
                pd = a["pub_date"]
                url = a["url"]
                m = re.search(r"(\d+)\.html$", url)
                url_id = m.group(1) if m else aid[-8:]
                fname = f"INE_{pd}_{url_id}.html"
                fp = INE_BASE / "daily" / fname
                fp.parent.mkdir(parents=True, exist_ok=True)
                try:
                    page.goto(url, wait_until="networkidle", timeout=30000)
                    time.sleep(1)
                    html = page.content()
                    if len(html) > 500:
                        if _check_download(html, "INE", aid, a["title"], a.get("url", "")):
                            continue
                        _locked_write_html(fp, html)
                        upsert_record(aid, {
                            "id": aid,
                            "url": url,
                            "title": a["title"],
                            "exchange": "INE",
                            "category": "daily",
                            "pub_date": pd,
                            "source_file": str(fp.relative_to(Path.cwd())),
                            "status": "downloaded",
                            "downloaded_at": _now_str(),
                        })
                        meta[aid] = True
                        logger.info("  ✓ [  daily] %s | %s", aid,
                                     a["title"][:50])
                except Exception:
                    logger.exception("  ✗ [daily] %s | %s",
                                     aid, a["title"][:40])
            logger.info("[INE] 公告: 下载 %s/%s 条",
                         sum(1 for a in new_d
                             if _ine_article_id(a["url"]) in meta),
                         len(new_d))
        else:
            logger.warning("[INE] 公告: 未发现新公告")

        browser.close()

    logger.info("[INE] 采集完成")


# ══════════════════════════════════════════════════════
#  SHFE Playwright 采集
# ══════════════════════════════════════════════════════

SHFE_BASE = _exchange_dir("SHFE")

SHFE_RULE_CATEGORIES = [
    ("rules", "general",
     "https://www.shfe.com.cn/regulation/exchangerules/rules/"),
    ("otherrules", "general",
     "https://www.shfe.com.cn/regulation/exchangerules/otherrules/"),
    ("productrules", "product",
     "https://www.shfe.com.cn/regulation/exchangerules/productrules/"),
]

SHFE_NOTICES_URL = "https://www.shfe.com.cn/publicnotice/notice/"


def _shfe_article_id(url: str) -> str:
    """SHFE 文章 ID: SHFE_YYYYMMDD_NNNNN."""
    m = re.search(r"/t(\d{8})_(\d+)\.html$", url)
    if m:
        return f"SHFE_{m.group(1)}_{m.group(2)}"
    return f"SHFE_{_compute_id(url)}"


def _shfe_extract_list(
    page, base_url: str,
) -> list[tuple[str, str]]:
    """在 Playwright page 中访问列表页, 返回 [(href, title), ...]."""
    html = page.content()
    return re.findall(
        r'<a[^>]*href="\./(\d{6}/t\d{8}_\d+\.html)"'
        r'[^>]*>([^<]+)</a>',
        html,
    )


def _shfe_extract_rules(page) -> list[dict]:
    """Playwright 翻页提取规则文章."""
    articles = []
    for cat_code, category, base_url in SHFE_RULE_CATEGORIES:
        page_idx = None
        cat_items = 0
        while True:
            url = (base_url if page_idx is None
                   else f"{base_url}index_{page_idx}.html")
            page.goto(url, wait_until="networkidle", timeout=30000)
            time.sleep(1)
            items = _shfe_extract_list(page, base_url)
            if not items:
                break
            for href, title in items:
                m = re.search(r"t(\d{8})_(\d+)", href)
                pub_date = m.group(1) if m else "00000000"
                full_url = f"{base_url}{href}"
                articles.append({
                    "title": title.strip(),
                    "url": full_url,
                    "pub_date": pub_date,
                    "category": category,
                })
            cat_items += len(items)
            if page_idx is None:
                page_idx = 1
            else:
                page_idx += 1
        logger.info("[SHFE] 规则 [%s]: %s 条", cat_code, cat_items)
    return articles


def _shfe_extract_daily(page) -> list[dict]:
    """Playwright 翻页提取公告通知, 日期 < DAILY_START_DATE 停止."""
    articles = []
    page_idx = None
    stopped = False
    while not stopped:
        url = (SHFE_NOTICES_URL if page_idx is None
               else f"{SHFE_NOTICES_URL}index_{page_idx}.html")
        page.goto(url, wait_until="networkidle", timeout=30000)
        time.sleep(1)
        items = _shfe_extract_list(page, SHFE_NOTICES_URL)
        if not items:
            break
        page_count = 0
        for href, title in items:
            m = re.search(r"t(\d{8})_(\d+)", href)
            pub_date = m.group(1) if m else "00000000"
            if pub_date < DAILY_START_DATE:
                stopped = True
                break
            articles.append({
                "title": title.strip(),
                "url": f"{SHFE_NOTICES_URL}{href}",
                "pub_date": pub_date,
                "category": "daily",
            })
            page_count += 1
        pg_label = "1" if page_idx is None else str(page_idx + 1)
        logger.info("[SHFE] 公告第%s页: %s 条", pg_label, page_count)
        if page_idx is None:
            page_idx = 1
        else:
            page_idx += 1
    return articles


def collect_shfe():
    """SHFE 全量采集: 规则类 + 公告通知. 全部用 Playwright."""
    meta = load_metadata()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        ctx = browser.new_context(locale="zh-CN")
        page = ctx.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', "
            "{get: () => undefined})"
        )

        # Warm up cookie — 品种细则页无 WAF
        page.goto(
            SHFE_RULE_CATEGORIES[2][2],
            wait_until="networkidle", timeout=30000,
        )
        time.sleep(2)

        # ── 规则类 ──
        logger.info("[SHFE] 采集规则类")
        rules = _shfe_extract_rules(page)
        if rules:
            new = [a for a in rules
                   if _shfe_article_id(a["url"]) not in meta]
            for a in new:
                aid = _shfe_article_id(a["url"])
                cat = a["category"]
                pd = a["pub_date"]
                url = a["url"]
                m = re.search(r"(\d+)\.html$", url)
                url_id = m.group(1) if m else aid[-8:]
                fname = f"SHFE_{pd}_{url_id}.html"
                fp = SHFE_BASE / cat / fname
                fp.parent.mkdir(parents=True, exist_ok=True)
                try:
                    page.goto(url, wait_until="networkidle", timeout=30000)
                    time.sleep(1)
                    html = page.content()
                    if len(html) > 500:
                        if _check_download(html, "SHFE", aid, a["title"], a.get("url", "")):
                            continue
                        _locked_write_html(fp, html)
                        upsert_record(aid, {
                            "id": aid,
                            "url": url,
                            "title": a["title"],
                            "exchange": "SHFE",
                            "category": cat,
                            "pub_date": pd,
                            "source_file": str(fp.relative_to(Path.cwd())),
                            "status": "downloaded",
                            "downloaded_at": _now_str(),
                        })
                        meta[aid] = True
                        logger.info("  ✓ [%7s] %s | %s", cat, aid,
                                     a["title"][:50])
                except Exception:
                    logger.exception("  ✗ [%s] %s | %s",
                                     cat, aid, a["title"][:40])
            logger.info("[SHFE] 规则类: 下载 %s/%s 条",
                         sum(1 for a in new
                             if _shfe_article_id(a["url"]) in meta),
                         len(new))

        # ── 公告通知 ──
        logger.info("[SHFE] 采集公告通知 (≥ %s)", DAILY_START_DATE)
        daily = _shfe_extract_daily(page)
        if daily:
            new_d = [a for a in daily
                     if _shfe_article_id(a["url"]) not in meta]
            for a in new_d:
                aid = _shfe_article_id(a["url"])
                pd = a["pub_date"]
                url = a["url"]
                m = re.search(r"(\d+)\.html$", url)
                url_id = m.group(1) if m else aid[-8:]
                fname = f"SHFE_{pd}_{url_id}.html"
                fp = SHFE_BASE / "daily" / fname
                fp.parent.mkdir(parents=True, exist_ok=True)
                try:
                    page.goto(url, wait_until="networkidle", timeout=30000)
                    time.sleep(1)
                    html = page.content()
                    if len(html) > 500:
                        if _check_download(html, "SHFE", aid, a["title"], a.get("url", "")):
                            continue
                        _locked_write_html(fp, html)
                        upsert_record(aid, {
                            "id": aid,
                            "url": url,
                            "title": a["title"],
                            "exchange": "SHFE",
                            "category": "daily",
                            "pub_date": pd,
                            "source_file": str(fp.relative_to(Path.cwd())),
                            "status": "downloaded",
                            "downloaded_at": _now_str(),
                        })
                        meta[aid] = True
                        logger.info("  ✓ [  daily] %s | %s", aid,
                                     a["title"][:50])
                except Exception:
                    logger.exception("  ✗ [daily] %s | %s",
                                     aid, a["title"][:40])
            logger.info("[SHFE] 公告: 下载 %s/%s 条",
                         sum(1 for a in new_d
                             if _shfe_article_id(a["url"]) in meta),
                         len(new_d))
        else:
            logger.warning("[SHFE] 公告: 未发现新公告")

        browser.close()

    logger.info("[SHFE] 采集完成")


# ══════════════════════════════════════════════════════
#  主流程
# ══════════════════════════════════════════════════════

def _compute_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


def collect_once():
    """单次采集循环. CFFEX, DCE, GFEX, INE, SHFE."""
    logger.info("%s", '='*60)
    logger.info("采集开始 — %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # 目录对账
    meta = load_metadata()
    clean_orphans(meta)
    logger.info("已知公告: %s 条", len(meta))

    # ── CFFEX: Playwright 直采 ──
    collect_cffex()

    # ── DCE: CMS API ──
    collect_dce()

    # ── GFEX: requests 直采 ──
    collect_gfex()

    # ── INE: Playwright 直采 ──
    collect_ine()

    # ── SHFE: Playwright 直采 ──
    collect_shfe()

    _log_download_failures()
    logger.info("%s", '='*60)


def main():
    logger.info("collect_announcements_service 启动")
    logger.info(
    "交易所: CFFEX (Playwright), DCE (CMS API),"
    " GFEX (requests), INE, SHFE (Playwright)",
)

    # 单次采集，完成后退出 (由外部 cron/systemd 控制周期)
    try:
        collect_once()
    except KeyboardInterrupt:
        logger.info("收到 KeyboardInterrupt，停止。")
        sys.exit(0)
    except Exception:
        logger.exception("采集异常")
        sys.exit(1)

    logger.info("采集完毕，退出。")


if __name__ == "__main__":
    main()
