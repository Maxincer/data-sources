# data-sources 开发日志

> 项目开发过程中的设计讨论、技术决策、问题排查记录。
> 合并自：`announcement-dev-log.md`、`announcement-monitoring-plan.md`、
> `collect_announcements_service_devlog.md`、`daily-order-limits-design.md`、
> `deepseek-aiohttp-connection-troubleshooting.md`、`tick-change-validation.md`

---

## 一、公告采集与解析

### 1. 项目背景

`NotifierWatchdog` 数据源项目需要对六大期货交易所的公告进行监控，发现影响数据管线的参数变更（最小变动价位、涨跌停板幅度、最大/最小开仓手数等）。

### 2. 公告存储结构

```
data/raw/announcements/
├── announcements_metadata.json       # 公告元数据（JSON，key=ID）
└── exchanges/
    ├── CFFEX/
    │   ├── general/      # 通用规则（章程、交易规则等）
    │   ├── product/      # 品种细则
    │   └── daily/        # 日常时效性公告
    └── DCE/
        ├── general/
        ├── product/
        └── daily/
```

#### 分类原则

按公告内容的**持久性**分类（而非交易所官网栏目名称）：

| 类别 | 内容 | 来源 |
|------|------|------|
| `general/` | 永久/长期有效的规则（章程、交易规则、结算规则、业务办法） | 交易所规则页 |
| `product/` | 品种级细则（各合约交易细则、交割细则） | 交易所品种细则页 |
| `daily/` | 日常时效性公告（通知、业务公告，≥20260101） | 交易所公告列表 |

#### 公告 ID 与文件命名

最终方案：纯 ASCII，去中文

```
{交易所}_{YYYYMMDD}_{文章ID}.html
示例: CFFEX_20100326_10719.html
```

命名演进：

| 版本 | 格式 | 问题 |
|------|------|------|
| v0 | MD5 hash | 完全不可辨识 |
| v1 | 含中文标题 | WSL2 终端中文转义 |
| v2 ✅ | 纯 ASCII | 清洁、可 grep、跨平台 |

日期统一规范：`pub_date` 统一从 URL 提取（`/\d{8}/` 段），不使用页面 DOM 解析日期。

### 3. 元数据 Schema

```json
{
  "CFFEX_20100326_10719": {
    "id": "CFFEX_20100326_10719",
    "url": "http://www.cffex.com.cn/cn/jystz/20100326/10719.html",
    "title": "关于沪深300股指期货合约上市交易有关事项的通知",
    "exchange": "CFFEX",
    "category": "general",
    "pub_date": "20100326",
    "source_file": "data/raw/announcements/exchanges/CFFEX/general/CFFEX_20100326_10719.html",
    "status": "downloaded",
    "downloaded_at": "2026-06-08T15:36:08+08:00"
  }
}
```

#### 字段命名规范

| 字段 | 格式 | 来源 | 示例 |
|------|------|------|------|
| `pub_date` | `YYYYMMDD` | URL 路径日期段 | `20100326` |
| `downloaded_at` | ISO 8601 +08:00 | 东八区当前时间 | `2026-06-08T15:36:08+08:00` |

---

### 4. 各交易所采集通道

| 交易所 | 通道 | 类别 | 备注 |
|--------|------|------|------|
| CFFEX | Playwright 直采 | general/product/daily | 完整原始 HTML |
| DCE | CMS API (columnId=244) + Tavily (rules) | general/product/daily | daily 返回原始 HTML, rules 为 Tavily 文本 |
| GFEX | 待实现 | daily | Tavily 正则修正 |
| SHFE | 待实现 | daily | JS 渲染，Playwright |
| INE | 待实现 | daily | Tavily 基本可用 |
| CZCE | 特殊方案（待定） | daily | 强 WAF |

#### CFFEX — Playwright 直采 ✅

CFFEX 是最先完成的采集通道，结构最清晰。

**采集来源与分类映射**：

| 来源页 | URL | 分类 | 策略 |
|--------|-----|------|------|
| 交易所规则页 | `/cn/jysgz.html` | general + product | 单页全量下载 |
| 交易所公告 | `/cn/jysgg[_N].html` | daily | 翻页，日期 < 20260101 停止 |
| 交易所通知 | `/cn/jystz[_N].html` | daily | 同上 |
| 交易所动态 | `/cn/jysdt[_N].html` | 不采集 | 新闻类，非公告 |

**jysgz.html 栏目解析**（按 `<div class="head_title">` 切割）：

| 栏目 | 处理 | 分类逻辑 |
|------|------|---------|
| 交易规则和结算规则 | 下载 | → general |
| 实施细则 | 下载 | 含"期货合约/期权合约/交割/期转现" → product，否则 → general |
| 业务指引 | 下载 | → general |
| 业务通知 | 下载 | → general（跨所有年份的重要通知） |
| 已废止业务规则 | **跳过** | — |
| 历史版本 | **跳过** | — |

**去重策略**：先采集 general + product，再采集 daily。同一 URL 的 metadata key 唯一，已存即 skip。

**完成状态**：104 条 metadata，CFFEX/general/ 61 文件，CFFEX/product/ 11 文件，CFFEX/daily/ 32 文件。

#### DCE — CMS API (columnId=244) + Tavily ✅

**频道定义**（用户指定 6 个频道/栏目）：

| 频道 ID | 内容 | 页数 | 分类 | 文章数 |
|---------|------|------|------|--------|
| 7000236 | 章程 | 1 | general | 1 |
| 149 | 交易所规则和结算规则 | 1 | general | 2 |
| 150 | 业务办法 | 3 | general | 21 |
| 151 | 品种细则 | 3 | product | 26 |
| 152 | 其它相关规定 | 1 | general | 8 |
| **239** | 业务公告与通知 (≥20260101) | 9 | daily | 81 |

**CMS API 踩坑过程**：

```
尝试过程：
─ POST /cms/auth/accessToken + apikey → ✅ token (eyJ...)
─ POST /cms/info/articleByPage + apikey + token + columnId=239 → ❌ 401 无权限
─ POST /cms/info/articleByPage + apikey + token + columnId=244 → ✅ success=true
```

**根因**：CMS API 需要**同时携带** `apikey` 头和 `Authorization: Bearer` 头，缺一不可。API 文档未在 `articleByPage` 调用示例中强调也需要传递 apikey 头。

**最终策略**：
- 规则类（general/product）→ Tavily Extract（API 无对应栏目权限）
- 日常公告（daily）→ CMS API columnId=244 优先，Tavily 回退

**ID 与文件命名**：

| 类型 | 日期来源 | 示例 |
|------|---------|------|
| 日常公告 | 标题中的 `YYYY-MM-DD` | `DCE_20260603_18630327.html` |
| 规则类 | URL 路径年份 + 0101 | `DCE_20180101_6146499.html` |

#### CZCE — 强 WAF (特殊方案，待定)

CZCE（郑州商品交易所）全站部署阿里云 WAF，封锁程度远超其他交易所：

| 环境 | 结果 |
|------|------|
| WSL2 Playwright headless/headful | ❌ 412 |
| server202 Snap Chromium headless | ❌ 412 (JS challenge 执行后被拒) |
| Python `requests` | ❌ 412 |
| Tavily (美国) | ✅ 200（但 JS 不渲染） |
| Windows 真实 Chrome（有历史 cookie） | ✅ 通过 |

**根因**：WAF 检测 TLS 指纹 + headless 运行时特征，非地理位置。仅用户真实 Chrome profile 可通过。

**可行路径（仅限 Windows）**：通过移动端 JSON API (`/cmsapi/cmsapp/content/selectNoticeCN?Jv5sQwFC=...`) 获取公告列表。但 `Jv5sQwFC` token 为 IP 绑定 + 有时效性，仅从 Windows Chrome 发出的请求才有效。

**交易参数已有替代源**：CZCE 的涨跌停板幅度等参数已通过现有 fetcher pipeline 每日下载 `TradingParameters.txt`，**不需要从公告中提取**。

#### SHFE — JS 渲染 (待实现)

SHFE/INE 列表页有 JS WAF 挑战，需 Playwright 直采。首次 `goto` 后等待 ~5s JS 放行。

#### WAF 绕过方案

已验证可行方案：

```python
browser = p.chromium.launch(
    headless=True,
    executable_path=CHROME_BIN,
    args=[
        "--no-sandbox",
        "--disable-gpu",
        "--disable-blink-features=AutomationControlled",  # 隐藏自动化标记
        "--window-size=1920,1080",
        "--disable-dev-shm-usage",
    ],
)
```

#### Tavily vs Playwright 对比

Tavily → 仅适用于列表页为纯静态 HTML 的交易所（INE、DCE）
JS 渲染页面 → 必须用 Playwright 直采

| 维度 | 直接扫列表页 | Tavily Search |
|------|-------------|---------------|
| 完整性 | ✅ 不遗漏 | ⚠️ 依赖搜索引擎索引 |
| 发布时间 | ✅ 页面提取 | ⚠️ 需二次抓取 |
| API 费用 | ✅ 无 | ❌ 按次计费 |
| 维护负担 | ⚠️ 每家交易所独立解析器 | ✅ 统一接口 |

---

### 5. 代码架构

#### 双通道设计

```
collect_once():
  ├── collect_cffex()                    # Playwright 直采
  ├── collect_dce()                      # CMS API (daily) + Tavily (rules)
  └── for ex in EXCHANGE_LIST_PAGES:     # Tavily 通用循环
      ├── discover_articles()            # Tavily Extract
      └── download_articles()            # Playwright 下载
```

#### 关键模块

| 文件 | 说明 |
|------|------|
| `src/data_sources/services/collect_announcements_service.py` | 采集服务主代码 |
| `scripts/service_manager.sh` | 服务启停管理 |
| `scripts/announcement_watcher.py` | 公告搜索+分析+告警（Tavily方式） |
| `scripts/check_announcements.py` | 公告页面连通性检查 |
| `scripts/download_announcements.py` | 公告详情页下载工具 |
| `scripts/order_limits_updater.py` | Order Limits CSV 更新 |
| `scripts/transform_order_limits.py` | CSV 列改造工具 |
| `scripts/collect_czce_windows.py` | CZCE 特殊采集脚本（Windows 专用） |

#### 环境变量

| 变量 | 说明 |
|------|------|
| `CHROME_BIN` | Chrome/Chromium 二进制路径 |
| `CHROME_LD_LIBRARY_PATH` | Chrome 共享库路径 |
| `PLAYWRIGHT_BROWSERS_PATH` | Playwright 浏览器目录 |

---

### 6. Playwright Chromium 浏览器修复 (2026-06-09)

**问题**：WSL2 Ubuntu 26.04 上 Playwright 1.58.0 不支持 `playwright install chromium`：
```
Error: Playwright does not support chromium on ubuntu26.04-x64
```
6月4日 Playwright 触发浏览器版本检查，旧版被删，新版无法安装，浏览器丢失。

**解决**：使用预解压 Google Chrome 二进制替代：

```bash
# 复制 Chrome 到 Playwright 期望路径
mkdir -p ~/.cache/ms-playwright/chromium-1208/chrome-linux64
cp -r /tmp/chrome-extract/opt/google/chrome/* ~/.cache/ms-playwright/chromium-1208/chrome-linux64/

# 创建 headless shell 软链接
# Playwright 1.58.0 headless 模式找 chromium_headless_shell-1208
mkdir -p ~/.cache/ms-playwright/chromium_headless_shell-1208/chrome-headless-shell-linux64
ln -sf ~/.cache/ms-playwright/chromium-1208/chrome-linux64/chrome \
      ~/.cache/ms-playwright/chromium_headless_shell-1208/chrome-headless-shell-linux64/chrome-headless-shell

# 补充共享库
cp -rn /tmp/chrome-deps/usr/lib/x86_64-linux-gnu/* ~/.local/chrome-libs/
```

**Playwright 1.58.0 行为变更**：

| 模式 | 查找的二进制 |
|------|-------------|
| `headless=True`（默认） | `chromium_headless_shell-1208/.../chrome-headless-shell` |
| `headless=False` | `chromium-1208/.../chrome` |

headless 与 full browser 拆成两个独立浏览器包，不再是同一个二进制加 `--headless` 参数。

**影响范围**：
- 6月4日起 dev 模式下 Fetcher 只成功下载 SHFE 数据（3 文件/天），其他交易所全部缺失
- 此前（4/20~6/3）每天 18~19 个文件完整下载
- 生产环境（server202, Ubuntu 22.04）不受影响

---

## 二、公告监控方案

### 1. 监控字段

需要自行计算的字段（依赖交易所公告中的参数变更）：

| 交易所 | 自行计算字段 | 计算方式 | 依赖的原始数据 |
|--------|--------------|---------|--------------|
| INE/SHFE | maxup, maxdown | floor(前结算 × (1±幅度) / tick) × tick | 前结算 + 幅度 + tick |
| CZCE | maxup, maxdown | ceil/floor(前结算 × (1±幅度) / tick) × tick | 同上，取整规则不同 |
| DCE | (无) | 直接从 TradingParameters 读取 | — |
| CFFEX | (无) | 直接从结算 CSV 读取 | — |
| GFEX | (无) | 直接从 TradingParameters 读取 | — |

**关键风险**：只有 INE/SHFE 和 CZCE 的涨跌停是计算出来的，其余交易所直接读取 API。

### 2. 公告页面可访问性

| 交易所 | 列表页 | 状态 | 单页 | 说明 |
|--------|--------|------|------|------|
| INE | `/publicnotice/` | 🟡 JS 渲染 | ✅ 可达 | 列表页需 JS |
| SHFE | `/publicnotice/` | 🟡 JS 渲染 | ✅ 可达 | 同一套 CMS |
| DCE | 公告列表页 | ❌ 全站反爬 | ❌ 单页也反爬 | 仅搜索引擎可索引 |
| CZCE | 公告列表页 | ❌ 412 | ? | CDN 拦截 |
| CFFEX | `/cn/jysgg.html` | 🟡 JS 渲染 | ? | 需进一步测试 |
| GFEX | `/gfex/tzts/list_yw.shtml` | 🟡 JS 渲染 | ? | 同 CFFEX |

### 3. 监控工作流

```
搜索(web_search) → 发现公告URL → 获取单页内容 → LLM分析 → 有影响则飞书告警
```

```python
def run_watch():
    # Step 1: 对各交易所搜索近期公告
    queries = [
        "上海国际能源交易中心 公告 site:ine.cn 涨跌停|保证金|最小变动",
        "上海期货交易所 公告 site:shfe.com.cn",
        # ... (6 家交易所)
    ]
    for q in queries:
        results = web_search(q, days_back=30)
        announcements.extend(results)

    # Step 2: 获取公告全文
    for url in announcement_urls:
        text = fetch_text(url)

    # Step 3: LLM 分析
    result = llm_analyze(prompt)
    if result.has_impact:
        feishu_alert(result.summary)
```

### 4. 调整记录输出格式

```
- 调整合约：SC<MMDD>，调整项目：涨跌停板幅度，调整前：12%，调整后：17%，生效时间：20260602
- 调整合约：XX<06DD>，调整项目：保证金比率，调整前：12%，调整后：17%，生效时间：20260602
```

**字段说明**：
- `SC<MMDD>` — 品种代码 + `<合约月份>`，如 SC2606 作 `SC<2606>`
- `XX<06DD>` — 泛指品种代码 + `<06DD>`，其中 `06` 为固定月份前缀，`DD` 为到期日
- `调整项目` — 枚举：涨跌停板幅度 / 保证金比率 / 最小变动价位 等
- `调整前/后` — 带 `%` 或绝对数值
- `生效时间` — YYYYMMDD

---

## 三、Order Limits（minoq/maxoq）设计

### 1. 数据源覆盖分析

| 交易所 | minoq/maxoq 来源 | 当前状态 |
|--------|-----------------|---------|
| CZCE | TradingParameters.txt (col 8,9) | ✅ parser 逐合约提取 |
| DCE | `VarietyTradingParam.json` → `maxHand` | ✅ fetcher + parser 已添加 |
| CFFEX | 静态 CSV（品种级固定值） | ✅ 静态注入 |
| SHFE/INE | 静态 CSV（品种级固定值） | ✅ 静态注入 |
| GFEX | 静态 CSV（品种级固定值） | ✅ 静态注入 |

**结论**：只有 CZCE 提供逐合约日频 minoq/maxoq，其余交易所为品种级静态参数或 API 获取。

### 2. 分层注入架构

```
交易所日频文件
├── CZCE TradingParameters.txt → parser 逐合约提取 minoq/maxoq
├── DCE VarietyTradingParam.json → parser 提取 maxHand (品种级)
└── modifier.inject_order_limits()
    └── 从静态 CSV 补充缺失的 minoq/maxoq
        ├── CFFEX / SHFE / INE / GFEX → 品种级静态值
        └── CZCE → 跳过（已有精确值不覆盖）
```

### 3. DCE maxoq 精准获取

DCE 提供 `/tradepara/tradingParam` API，返回品种级参数表包含 `maxHand`。

**文件命名区分**：品种级数据文件命名为 `VarietyTradingParam.json`，与合约级 `TradingParameters.json` 区分。

**`-F` 月均价合约处理**（2026-06-12）：

DCE 月均价合约（如 `l2607F`）的品种 ID 为 `l-F`。品种级 code 与合约级 code 不同，maxoq 无法通过 `merge_by_code_date()` 合并。

**解决**：在 `merge_by_code_date()` 中提取品种级记录的字母前缀，按前缀向合约级记录传播 `maxoq`。

```python
# 品种级：无数字 → 提取前缀
prefix = "".join(c for c in raw if c.isalpha()).upper()
dce_variety[prefix] = dict(rec)

# 合约级：有数字 → 按前缀匹配
prefix = "".join(c for c in raw if c.isalpha()).upper()
if prefix in dce_variety:
    rec["maxoq"] = dce_variety[prefix]["maxoq"]
```

### 4. 交割月动态调整

| 情形 | 处理 | 说明 |
|------|------|------|
| CZCE 交割月下调 | ✅ parser 每日精确获取 | TradingParameters 自动反映 |
| 其他所交割月下调 | ❌ 静态 CSV 无法反映 | 影响面小（仅临近交割合约） |

### 5. 推荐方案：CZCE 日频 + 静态 CSV + 公告告警

```
每日 pipeline 执行时：
├── CZCE 合约 → parser 逐合约精确提取 minoq/maxoq ✅
├── 其他合约 → static CSV 注入 ✅
└── 公告监控 → 发现 minoq/maxoq 变更 → 飞书告警 → 手动更新 CSV
```

### 6. CSV 格式

```csv
exchange,variety_id,variety_name,product_type,maxoq_limit,maxoq_market,minoq,source,notes
CFFEX,IF,沪深300股指,期货,100,50,1,交易细则,限价100手 市价50手
```

字段说明：
- `maxoq_limit` — 限价单最大下单量
- `maxoq_market` — 市价单最大下单量
- `minoq` — 最小开仓量

---

## 四、DeepSeek aiohttp 连接超时排查

> 2026-06-16 · `analyse_announcements_service` · WenDaoInvPC-200 (WSL2)

### 问题现象

60%+ 请求返回 `Connection timeout`，但 `curl api.deepseek.com` 仅需 0.19s。

### 根因

**主因：WSL2 IPv6 路由缺失 + aiohttp Happy Eyeballs 策略**

```
WSL2 环境：
  - IPv6 socket: available（socket.AF_INET6 可用）
  - IPv6 路由: 无（操作系统没有 IPv6 默认网关）
  - DNS 解析: 返回 AAAA（IPv6）+ A（IPv4）双记录
```

aiohttp 遵循 RFC 6555 Happy Eyeballs：
1. 先尝试 IPv6 连接 → 30s 无响应才超时
2. fallback 到 IPv4 → 0.2s 成功
3. **每次请求都重复这个流程** → 700 条公告 × 30s = 灾难

`curl` 不受影响是因为 libcurl 在 WSL2 上默认直接走 IPv4。

**次要因素：Windows 代理关闭空闲连接**

启用连接池复用时，请求复用 keep-alive 连接，但 Windows 代理关闭了空闲连接 → `Broken pipe`。

### 修复清单

| # | 问题 | 严重 | 修复 |
|---|------|------|------|
| 1 | IPv6 无路由，aiohttp Happy Eyeballs 空等 30s | 🔴 | `TCPConnector(family=AF_INET)` 强制 IPv4 |
| 2 | 代理关闭空闲 keep-alive 连接 → Broken pipe | 🔴 | 保留 `force_close=True` |
| 3 | retry 跨 event loop 崩溃 | 🔴 | 重试并入单 event loop |
| 4 | `async with sem/lock` 在 try 外 | 🔴 | 移入 try |
| 5 | Exception 无 full traceback | 🟡 | 加 `exc_info=True` |
| 6 | 无法区分"没发请求"和"发了没回" | 🟡 | 加 `DS_REQ` / `DS_RESP` / `DS_RAW` 三级日志 |
| 7 | timeouts 未显式配置 | 🟢 | `ClientTimeout(total=600, connect=30, sock_read=300)` |

### 教训

1. 不要假设 aiohttp 的 DNS/连接策略与 curl 一致
2. WSL2 IPv6 陷阱：socket available ≠ 路由可达
3. `force_close=True` 在代理环境更安全
4. 请求前/请求完成/解析完成三级日志大大缩短排查周期

---

## 五、Tick 变更验证 — INE EC 最小变动价位调整

> 生效日期：2026-05-11
> 旧 tick: 0.1 点 → 新 tick: 0.5 点

### 发现过程

2026-05-11 数据验证报告显示 INE EC 合约 4 个合约 maxup 有差异，3 个合约 maxdown 有差异。

**追溯路径**：
1. 原始数据校验：四项原始依赖均正确
2. 全日期扫描：旧 tick=0.1 下完全符合 FLOOR，05-11 新表不符合
3. 交易所公告溯源：查到 INE 在 2026-05-11 调整 tick
4. **根因**：代码直接用原始前结算价 × 1.2，漏掉了"前结算价先调整到新 tick"这一步

### 交易所规定的前结算价调整规则

2026-05-11 起，前结算价从旧 tick (0.1) 调整到新 tick (0.5)：

| 末位数 | 调整方式 |
|--------|---------|
| 0, 5 | 不变 |
| 1, 2 | 调整为 0 |
| 3, 4, 6, 7 | 调整为 5 |
| 8, 9 | 调整为 0 并向前进 1 位 |

然后用调整后的前结算价 + 新 tick 计算涨跌停价位。

### 计算验证（EC2605）

```
Step 1: 前结算价 = 1799.7（旧 tick=0.1）
Step 2: 末位 7 → 调整为 5 → 1799.5
Step 3: maxup = floor(1799.5 × 1.20 / 0.5) × 0.5 = 2159.0 ✅
         maxdown = floor(1799.5 × 0.80 / 0.5) × 0.5 = 1439.5 ✅
```

### 受影响合约

| 合约 | 字段 | 正确值 | 错误值 | 偏差 |
|------|------|--------|--------|------|
| EC2605 | maxup | 2159.0 | 2159.5 | +0.5 |
| EC2607 | maxup | 3112.0 | 3111.5 | -0.5 |
| EC2609 | maxdown | 1427.5 | 1427.0 | -0.5 |
| EC2610 | maxdown | 1311.5 | 1311.0 | -0.5 |
| EC2612 | maxup | 2267.0 | 2267.5 | +0.5 |
| EC2703 | maxup | 1857.0 | 1856.5 | -0.5 |
| EC2703 | maxdown | 1238.0 | 1237.5 | -0.5 |

### 长期解决方案

1. **交叉验证**：verifier.py 每日比较新表 vs Wind 原表
2. **报告告警**：reporter.py 汇总差异到飞书/邮件
3. **人工研判**：追溯交易所公告确认规则变更
4. **规则入库**：确认后更新代码中的历史参数表

### 类似场景

| 参数 | 来源 | 触发条件 |
|------|------|---------|
| 最小变动价位 | 交易所合约规格公告 | 交易所规则修订 |
| 涨跌停板幅度 | TradingParameters API | 节假日/风险调整 |
| 保证金比例 | TradingParameters API | 节假日/风险调整 |
| 合约乘数 | 合约规格 | 极少变更 |

---

## 六、Order Limits CSV 改造 (2026-06-04)

完成对四个 `order_limits_*.csv` 文件的改造：

1. **`adjustments.csv`**：`announcement_ref` 列替换为 metadata 中的完整公告原标题
2. **所有四个文件**：新增 `announcement_date`、`effective_date`、`source_url` 三列
3. 消除了所有 `source`、`evidence` 列中的"同上"缩写
4. `general_rules.csv`、`product_rules.csv`、`summary.csv` 的三列值待填充

---

## 七、LLM 字段分类问题 (2026-06-12)

### 首次全量运行发现的问题

| 问题 | 举例 | 根因 |
|------|------|------|
| minoq 全部误标为 maxoq | "最小开仓下单量调整为8手" → field=maxoq | prompt 未区分 min/max |
| 跨交易所品种 | BZ 出现在 CFFEX | prompt 未约束品种白名单 |
| 品种代码格式错误 | "DCE A" 应为 "A" | 未统一大小写 |
| 概念混淆 | "单日开仓量"被当成 maxoq | 未定义易混概念边界 |

### 已修复 (2026-06-12)

- prompt 中增加字段定义（maxoq/minoq 明确含义）
- 增加"严禁混淆"清单（单日开仓量、持仓限额、交易限额 ≠ maxoq/minoq）
- 增加交易所品种白名单约束
- 相关性预判增加"单日开仓限额不在此列"提示

---

## 八、待办事项

- [ ] CZCE/DCE 列表页 IP 解封后验证 DOM 结构
- [ ] 实现 SHFE/INE 公告采集（Playwright 直采）
- [ ] 修正 GFEX Tavily 正则
- [ ] 实现独立解析脚本（LLM 解析公告内容）
- [ ] 填充 `general_rules.csv` / `product_rules.csv` / `summary.csv` 的日期和链接列
- [ ] 交易参数扩展解析（limit_up/down、tick_size、margin_rate 等）
- [ ] 交易参数算法调整解析（计算方法变更的语义理解）
- [ ] Chromium 浏览器路径在目标 Ubuntu 服务器上正规化
