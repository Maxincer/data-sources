# Collect Announcements Service — 开发日志

> 记录 `collect_announcements_service.py` 开发过程中遇到的问题、解决方案与架构决策。

## 1. 问题：Tavily Extract 不适合所有交易所 (2026-06-05)

### 现象

最初设计统一用 Tavily Extract 获取六大期货交易所公告列表页的 `raw_content`，再用正则提取文章链接。首次运行结果：

| 交易所 | Tavily 发现 | 实际情况 |
|--------|------------|---------|
| GFEX | 0 条 | 正则格式不匹配 Tavily 输出 |
| CFFEX | 13 条 | 含导航栏假阳性 |
| SHFE | 41 条 | **JS 渲染页面，Tavily raw_content 中无公告链接** |
| INE | 25 条 | 基本可用 |
| CZCE | 1341 条 | **全站导航菜单被误匹配，真正的 webinfo 链接一条没有** |
| DCE | 10 条 | 基本可用 |

### 根因分析

1. **SHFE / CZCE**：公告列表由 JS 动态渲染，Tavily 拿到的 `raw_content` 只有导航和页面骨架，不含真正的公告链接。这是 Tavily Extract 的本质局限——它无法执行 JavaScript。
2. **GFEX**：Tavily 输出的 Markdown 格式与代码中预设的正则不匹配。
3. **CFFEX**：正则偏宽，匹配了部分导航链接。

### 结论

- Tavily → 仅适用于 **列表页为纯静态 HTML** 的交易所（INE、DCE）
- JS 渲染页面 → 必须用 **Playwright 直采**
- 先搞定 CFFEX（结构最清晰），积累模式后推广到其他交易所

---

## 2. CFFEX 目录结构设计 (2026-06-08)

### 2.1 三层分类

按公告内容的**持久性**分类（而非交易所的栏目名称）：

```
data/raw/announcements/exchanges/CFFEX/
├── general/       # 通用规则类公告（交易所章程、交易规则、结算规则等）
├── product/       # 品种细则类公告（各合约交易细则、交割细则等）
└── daily/         # 日常时效性公告（通知、公告、动态）
```

**设计讨论**：最初考虑按 CFFEX 官网栏目命名（`notices/announcements/news`），但用户指出这些都是 `announcements` 的子类型，应该按**公告内容的持久性**分类——永久/半永久规则 vs 日常时效信息。最终确定 `general/product/daily`。

### 2.2 采集来源与分类映射

| 来源页 | URL | 分类 | 策略 |
|--------|-----|------|------|
| 交易所规则页 | `/cn/jysgz.html` | general + product | 单页全量下载 |
| 交易所公告 | `/cn/jysgg[_N].html` | daily | 翻页，日期 < 20260101 停止 |
| 交易所通知 | `/cn/jystz[_N].html` | daily | 同上 |
| 交易所动态 | `/cn/jysdt[_N].html` | 不采集 | 新闻类，非公告 |

### 2.3 jysgz.html 栏目解析

页面有 6 个栏目，按 `<div class="head_title">` 切割：

| 栏目 | 处理 | 分类逻辑 |
|------|------|---------|
| 交易规则和结算规则 | 下载 | → general |
| 实施细则 | 下载 | 含"期货合约/期权合约/交割/期转现" → product，否则 → general |
| 业务指引 | 下载 | → general |
| 业务通知 | 下载 | → general（跨所有年份的重要通知） |
| 已废止业务规则 | **跳过** | — |
| 历史版本 | **跳过** | — |

### 2.4 翻页与停止条件

```python
# jysgg.html → jysgg_2.html → jysgg_3.html → ...
# 每页文章按日期降序，全部早于起始日时停止
DAILY_START_DATE = "20260101"  # YYYYMMDD
```

---

## 3. 公告 ID 与文件命名 (2026-06-08)

### 3.1 命名演进

| 版本 | 格式 | 示例 | 问题 |
|------|------|------|------|
| v0 | MD5 hash | `0bb5afb17c70.html` | 完全不可辨识 |
| v1 | 含中文标题 | `CFFEX_20260519_47870_关于增加...通知.html` | WSL2 终端中文转义，不可用 |
| v2 ✅ | 纯 ASCII | `CFFEX_20260519_47870.html` | 清洁、可 grep、跨平台兼容 |

### 3.2 ID 格式

```
{交易所}_{YYYYMMDD}_{文章ID}
示例: CFFEX_20100326_10719
```

- **交易所**：大写缩写（CFFEX）
- **日期**：从 URL 路径中的 `/\d{8}/` 段提取（所有 CFFEX URL 均含），格式 YYYYMMDD
- **文章ID**：URL 末尾 `\d+` 部分（如 `/47900.html` → `47900`）

### 3.3 日期统一规范

| 字段 | 格式 | 来源 | 示例 |
|------|------|------|------|
| `pub_date` | `YYYYMMDD` | URL 路径中的日期段 | `20100326` |
| `downloaded_at` | `ISO 8601 +08:00` | 东八区当前时间 | `2026-06-08T15:36:08+08:00` |

**关键决策 (2026-06-08)**：`pub_date` 统一从 URL 提取，不使用页面 DOM 中解析的日期——URL 结构稳定且 `/\d{8}/\d+\.html$` 模式在所有 CFFEX 页面中一致。

---

## 4. 代码架构

### 4.1 双通道设计

```
collect_once():
  ├── collect_cffex()                    # Playwright 直采
  │   ├── _parse_cffex_rules_page()      #   jysgz → general + product
  │   ├── _parse_cffex_daily_pages()     #   jysgg/jystz 翻页 → daily
  │   └── _download_cffex_articles()     #   详情页下载
  │
  └── for ex in EXCHANGE_LIST_PAGES:     # Tavily 通道 (CFFEX 除外)
      ├── discover_articles()            #   Tavily Extract
      └── download_articles()            #   Playwright 下载
```

### 4.2 关键函数

| 函数 | 职责 |
|------|------|
| `_extract_url_date_id(url)` | 从 URL 提取 `(YYYYMMDD, 文章ID)` |
| `_cffex_article_id(url)` | 生成 `CFFEX_YYYYMMDD_ID` |
| `_build_cffex_filename(article)` | 生成 `CFFEX_YYYYMMDD_ID.html` |
| `_classify_cffex(title, section)` | 按标题和页面栏目分类 general/product/daily |
| `_parse_cffex_rules_page(html)` | 解析 jysgz.html 各栏目 |
| `_parse_cffex_daily_pages(ctx, base_url)` | Playwright 翻页 daily 列表 |
| `_download_cffex_articles(ctx, articles, meta)` | 下载详情页到分类目录 |
| `collect_cffex()` | CFFEX 采集入口，管理浏览器生命周期 |

### 4.3 去重策略

- CFFEX 先采集 general + product，再采集 daily
- 同一 URL 去重：metadata key 唯一（`CFFEX_YYYYMMDD_ID`）
- 业务通知（来自 jysgz）中的 2026 通知被 daily jystz 再次遇到时自动跳过
- 用户确认：重复链接存储可接受（实际实现为 metadata 已存即 skip）

---

## 5. 遇到的问题与解决

### 5.1 Playwright 浏览器不可用 (WSL2 Ubuntu) — 2026-06-08

**现象**：`playwright install chromium` 报错 `does not support chromium on ubuntu26.04-x64`。

**原因**：WSL2 内核版本较新，Playwright 尚未适配。

**解决**：使用 `service_manager.sh` 预设的预解压 Chrome binary：
```bash
export CHROME_BIN="/tmp/chrome-extract/opt/google/chrome/chrome"
export CHROME_LD_LIBRARY_PATH="/tmp/chrome-deps/usr/lib/x86_64-linux-gnu"
```

### 5.2 requests 编码检测错误 — 2026-06-08

**现象**：开发调试时 `requests.get()` 中文乱码，正则无法匹配。

**原因**：`requests` 自动编码检测误判为 Latin-1。

**解决**：调试时 `resp.encoding = 'utf-8'`；生产环境 Playwright 返回 UTF-8 不受影响。

### 5.3 实施细则中品种合约的区分 — 2026-06-08

**问题**：`实施细则` 栏目同时含通用规则和品种合约细则。

**解决**：标题关键词判断——含"期货合约/期权合约/交割/期转现" → product，否则 → general。11 个品种全部正确分类。

### 5.4 中文文件名终端兼容问题 — 2026-06-08

**现象**：含中文标题的文件名在 WSL2 bash 中显示为 ANSI-C 转义序列（`$'\345\205\263...'`），无法 tab 补全。

**解决**：文件名去掉中文标题，改为 `CFFEX_YYYYMMDD_ID.html`。完整标题仅保留在 metadata 的 `title` 字段中。

### 5.5 pub_date 来源决策 — 2026-06-08

**问题**：jysgz 规则页文章初始无 `pub_date` (72/104)，daily 页面的日期来自 `<a class="time">` DOM 元素。

**解决**：统一从 URL 路径的 `/\d{8}/` 段提取。CFFEX 所有文章 URL 均包含日期，结构稳定，不依赖页面 DOM 解析。

### 5.6 环境变量与配置 — 2026-06-08

**决策**：
- `CHROME_BIN` / `CHROME_LD_LIBRARY_PATH`：无默认值，缺失直接 `KeyError`
- Logger 名称：`CollectAnnouncementsService`
- 移除 `import logging`：`mxz_utils.get_logger` 接受字符串 level

### 5.7 旧数据清理 — 2026-06-08

清理了 6月5日 Tavily 首轮测试产生的废数据：
- 删除 65 个 MD5 命名的 HTML 文件
- 删除 5 个 failback 生成的非标准文件
- 清空 5 个空交易所目录
- 丢弃 65 条非 CFFEX 的 metadata 记录

---

## 6. 当前状态

### CFFEX ✅ 完成

```
data/raw/announcements/
├── announcements_metadata.json         # 104 条
└── exchanges/CFFEX/
    ├── general/  61 files  (2845 KB)
    ├── product/  11 files  (526 KB)
    └── daily/    32 files  (1068 KB)
```

metadata 样例：
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

### 待处理（其他交易所）

| 交易所 | 问题 | 计划方案 |
|--------|------|---------|
| SHFE | JS 渲染，Tavily 无链接 | Playwright 直采（同 CFFEX 模式） |
| **CZCE** | **强 WAF，仅 Windows 真实 Chrome 可过** | **移动端 JSON API + 浏览器 token** |
| GFEX | Tavily 正则不匹配 | 修正则 或 Playwright |
| INE | Tavily 基本可用 | 修正则收紧 |
| DCE | ✅ **已完成** | Tavily Extract 全量采集 (2026-06-09) |

---

## 6b. CZCE 特殊情况 (2026-06-08)

### WAF 封锁分析

CZCE（郑州商品交易所）全站部署阿里云 WAF，封锁程度远超其他交易所：

| 环境 | 浏览器 | 网络 | CZCE 页面 | CZCE API |
|------|--------|------|----------|----------|
| WSL2 | Playwright headless Chrome (预解压) | 经代理隧道 | ❌ 412 (39B空) | ❌ |
| WSL2 | Playwright headful Chrome + WSLg | 经代理隧道 | ❌ 412 (39B空) | ❌ |
| WSL2 | Python `requests` + Chrome headers | 经代理隧道 | ❌ 412 (challenge) | ❌ |
| WSL2 | `curl` + Chrome headers | 经代理隧道 | ❌ 412 | ❌ |
| server202 (深圳电信) | Snap Chromium 149 headless | 中国直连 | ❌ 412 (JS challenge 执行后被拒) | ❌ |
| server202 (深圳电信) | Python `requests` | 中国直连 | ❌ 412 | ❌ |
| Windows (WenDaoInvPC-200) | 正常 Chrome | 中国直连 | ✅ 通过 | ✅ JSON 返回 |
| Tavily (美国) | Extract API | 美国 | ✅ 200 (但 JS 不渲染) | ❌ token IP 绑定 |

### 根因

1. **非 IP 检测**：深圳电信内网 IP 同样被拦，WAF 检测的是 TLS 指纹 + 浏览器特征，非地理位置
2. **JS challenge 对 headless 无效**：WSL2 上 WAF JS 被下载并执行（200），但 challenge token 被拒（400）——检测到了 headless 运行时特征
3. **Playwright 新 profile 也被检测**：即使 headful 模式 + WSLg GUI，无 cookie 的新 Chromium profile 也被拦
4. **仅用户真实 Chrome profile 可过**：Windows 上正常 Chrome（有历史 cookie）才能通过

### 可行路径：移动端 JSON API

CZCE 页面加载后发起 AJAX 请求 `GET /cmsapi/cmsapp/content/selectNoticeCN?Jv5sQwFC=...`，返回分页 JSON：

```json
{
  "result": {
    "records": [{
      "topic": "关于...的通知",
      "pubtime": "2026-06-05T07:38:30.000+00:00",
      "pathurl": "http://www.czce.com.cn/cn/.../webinfo/2026/6/xxx.htm",
      "infoid": "1512465649572511744"
    }],
    "total": 2453,
    "pages": 164,
    "size": 15
  }
}
```

**关键限制**：
- `Jv5sQwFC` 参数是 WAF 生成的 token，**IP 绑定 + 有时效性**
- 仅从用户 Windows Chrome 发出才有效
- Tavily / server202 / WSL2 均无法使用该 token
- `requests` 从 Windows 也无法调用（非浏览器 TLS）

**可行采集流程**（仅限 Windows）：
1. 关闭所有 Chrome → 运行脚本 → Playwright 用真实 Chrome profile 打开 CZCE
2. 拦截 `selectNoticeCN` API 响应，拿到 token + 第 1 页数据
3. 遍历 164 页，收集全量公告列表
4. 详情页下载也走浏览器（`requests` 也会被拦）

脚本位置：`scripts/collect_czce_windows.py`

### 交易参数数据源

CZCE 的涨跌停板幅度、最小/最大下单手数等交易参数，已通过现有 fetcher pipeline 从 `/cn/DFSStaticFiles/Future/` 路径每日下载（TradingParameters.txt / SettlementParameters.txt），**不需要从公告中提取**。公告采集的范围可以缩小为仅关注规则变更类通知。

## 6c. DCE 采集方案（前期调研，2026-06-08）

前期确认了 DCE 各频道结构、Tavily 可用性、WAF 封锁情况。详见 §8。

---

## 8. DCE 采集实现 (2026-06-09)

### 8.1 采集频道定义

用户指定的 DCE 公告采集范围（6 个频道/栏目）：

| 频道 ID | 内容 | 页数 | 分类 | 文章数 |
|---------|------|------|------|--------|
| 7000236 | 章程 | 1 | general | 1 |
| 149 | 交易所规则和结算规则 | 1 | general | 2 |
| 150 | 业务办法 | 3 | general | 21 |
| 151 | 品种细则 | 3 | product | 26 |
| 152 | 其它相关规定 | 1 | general | 8 |
| **239** | **业务公告与通知 (≥20260101)** | **9** | **daily** | **81** |
| | **合计** | | | **~139** |

### 8.2 DCE API 拒绝

API 文档（`docs/dceapiv1.0.docx`）提供了**业务公告与通知**的接口：

```
POST /dceapi/cms/info/articleByPage
Header: apikey=*** Authorization: Bearer <token>
Body: {"columnId":"239", "pageNo":1, "siteId":5, "pageSize":10}
```

实测流程：

| 步骤 | 结果 |
|------|------|
| `POST /dceapi/cms/auth/accessToken` 获取 token | ✅ 成功 (`token: eyJ...`) |
| `POST /dceapi/cms/info/articleByPage` + apikey + token | ❌ 401: `"无权限访问该栏目！"` |
| `POST /dceapi/forward/publicweb/tradepara/tradingParam` + apikey + token (现有交易数据接口) | ✅ 成功 |

**根因**：现有 API 密钥 `ofxc69rpmd59` 只有交易数据接口（`/forward/publicweb/*`）的权限范围，`/cms/info/*` 需要**单独申请**不同 scope 的 CMS 密钥。

**结论**：DCE CMS API 不可用，全量退回到 Tavily Extract。

### 8.3 最终采集策略

| 组件 | 方案 | 原因 |
|------|------|------|
| 列表发现 | Tavily Extract (advanced depth) | WAF 412 阻挡 Playwright |
| 详情下载 | Tavily Extract 批量 (20个/批) | 同上 |
| 采集间隔 | 每日一次（`COLLECTOR_INTERVAL=86400`） | Tavily 有配额限制 |

**代码位置**：
- 新增常量 `DCE_BASE`, `DCE_RULES_CHANNELS`, `DCE_DAILY_CHANNEL`
- 新增辅助函数：`_dce_extract_date_id()`, `_dce_article_id()`, `_dce_build_filename()`
- 新增提取函数：`_dce_tavily_extract()`, `_dce_extract_rules_articles()`, `_dce_extract_daily_articles()`
- 新增下载函数：`_dce_download_articles()`（支持批量 20 个 URL）
- 主入口：`collect_dce(api_key)`
- 接入 `collect_once()`：在 CFFEX 之后、通用 Tavily 循环之前调用

### 8.4 ID 与文件命名

格式同 CFFEX 规范：`{交易所}_{YYYYMMDD}_{文章ID}.html`

| 类型 | 日期来源 | 示例 |
|------|---------|------|
| 日常公告 | 标题中的 `YYYY-MM-DD` | `DCE_20260603_18630327.html` |
| 规则类 | URL 路径中的年份 + 0101 | `DCE_20180101_6146499.html` |

### 8.5 目录结构

```
data/raw/announcements/exchanges/DCE/
├── general/   # 章程 + 规则和结算规则 + 业务办法 + 其它相关规定
├── product/   # 品种细则
└── daily/     # 业务公告与通知 (≥20260101)
```

### 8.7 CMS API 通道发现 (2026-06-09)

#### 背景

初始设计认为 CMS API 密钥无权限，全量使用 Tavily。实际测试发现 **columnId=244 的 CMS 接口可用**。

#### 踩坑过程

| 尝试 | header | 结果 | 原因 |
|------|--------|------|------|
| `POST /cms/info/articleByPage` + `Authorization: Bearer` | ❌ 402 验证token失败 | 缺少 `apikey` 头，API 不认可 token |
| 仅 `apikey` 头，无 token | ❌ 402 无token | 缺少 token |
| 加上 `apikey` 头 + `Authorization` + columnId=239 | ❌ 401 无权限访问该栏目 | columnId 不对 |
| **`apikey` 头 + `Authorization` + columnId=244** | **✅ success=true** | **正确组合!** |

#### 根因

CMS API 需要**同时携带**两个认证信息：
1. `apikey` 头 — 标识应用身份
2. `Authorization: Bearer <token>` — 标识会话身份

两者缺一不可，缺少 `apikey` 头时 API 返回 `402 验证token失败`（误导性地指向 token 问题）。

API 文档的请求参数表只列出了 `apikey` 在登录接口的 header 参数中，未在 `articleByPage` 的调用示例中强调也需要传递，导致遗漏。

#### columnId=244 vs columnId=239

| 栏目 | API 状态 | 网页频道 |
|------|---------|---------|
| **244** | ✅ `success=true`，总量 2,359 条 | 网页端 channel 244 (原代码中) |
| 239 | ❌ 401 无权限 | 网页端 channel 239 (用户指定) |

实测两者 Tavily 列表页返回的公告链接完全一致（相同的 ywggytz URL），可判定 columnId=244 是 channel 239 的 API 等效栏目。

#### API vs Tavily 对比

| 对比项 | Tavily | CMS API |
|--------|--------|--------|
| 内容 | 文本(raw_content)，含导航 | **完整原始 HTML** |
| 配额 | 消耗 Tavily 额度 | **免费，无限制** |
| 速度 | 慢 (批量 20 个/次) | **快 (分页，一次 20-50 条)** |
| 发布时间 | 需从标题正则提取 (YYYY-MM-DD) | **直接返回 showDate 字段** |

#### 最终策略变更

```python
# 代码变更:
# 新增 
_dce_get_token()          # 获取 Bearer token
_dce_api_fetch_page()     # API 翻页获取公告列表
_dce_api_save_articles()  # 保存 API 返回的 HTML 内容 (无需下载)
_dce_collect_daily_via_api()   # API 通道主入口
_dce_collect_daily_via_tavily() # Tavily 回退通道

# collect_dce() 修改:
# 规则类 → 保留 Tavily (API 无对应栏目权限)
# 日常公告 → API 优先 (columnId=244), API 不可用时回退 Tavily
```

### 8.8 当前状态 (最终)

#### 采集通道

| 交易所 | 通道 | 类别 | 备注 |
|--------|------|------|------|
| CFFEX | Playwright 直采 | general/product/daily | 完整原始 HTML |
| **DCE** | **CMS API (columnId=244) + Tavily (rules)** | **general/product/daily** | **daily 返回原始 HTML, rules 为 Tavily 文本** |
| GFEX | Tavily | daily (待实现) | |
| SHFE | Plawright (待实现) | daily (待实现) | JS 渲染 |
| INE | Tavily (待实现) | daily (待实现) | |
| CZCE | 特殊方案 (待定) | daily (待实现) | 强 WAF |

#### 已知限制 (更新)

1. **规则类 (rules) 内容非原始 HTML**：因 WAF + 无 API 通道，规则类频道通过 Tavily Extract 获取，存储的是文本内容
2. **日常公告 (daily) 为原始 HTML**：通过 CMS API (columnId=244) 获取，内容为完整原始 HTML
3. **Tavily 配额消耗**：仅规则类首次全量消耗 ~3 次调用，后续增量约为 0

---

## 9. Playwright Chromium 浏览器修复 (2026-06-09)

### 9.1 问题

WSL2 开发环境下 Fetcher 的 `_fetch_exchange_product_config`（SHFE/INE 产品配置页面采集）在 6 月 4 日后崩溃，导致后续所有交易所数据未下载。

### 9.2 根因

Playwright 1.58.0 安装于 5 月 20 日，其 `playwright install chromium` 在 WSL2 Ubuntu 26.04 x64 上不兼容：

```
$ .venv/bin/playwright install chromium
Error: ERROR: Playwright does not support chromium on ubuntu26.04-x64
```

此前浏览器文件存在于 `~/.cache/ms-playwright/` 中并正常工作。6 月 4 日 Playwright 内部触发了一次浏览器版本检查/重装（`.links` 新增记录），旧版浏览器被删除，新版因系统不支持而无法安装，导致浏览器丢失。

### 9.3 解决方案

预解压的 Google Chrome 二进制位于 `/tmp/chrome-extract/opt/google/chrome/chrome`（264MB, ELF 64-bit），是之前 `service_manager.sh` 下载用于 `collect_announcements_service` 的。

**操作步骤：**

```bash
# 1. 复制 Chrome 到 Playwright 期望的路径
mkdir -p ~/.cache/ms-playwright/chromium-1208/chrome-linux64
cp -r /tmp/chrome-extract/opt/google/chrome/* ~/.cache/ms-playwright/chromium-1208/chrome-linux64/

# 2. 创建 headless shell 软链接
#    Playwright 1.58.0 headless 模式找的是 chromium_headless_shell-1208
#    headless=False 则找 chromium-1208/chrome-linux64/chrome
mkdir -p ~/.cache/ms-playwright/chromium_headless_shell-1208/chrome-headless-shell-linux64
ln -sf ~/.cache/ms-playwright/chromium-1208/chrome-linux64/chrome \
      ~/.cache/ms-playwright/chromium_headless_shell-1208/chrome-headless-shell-linux64/chrome-headless-shell

# 3. 补充共享库 (从 /tmp/chrome-deps 同步到已有 LD_LIBRARY_PATH 目录)
cp -rn /tmp/chrome-deps/usr/lib/x86_64-linux-gnu/* ~/.local/chrome-libs/
```

### 9.4 环境变量配置

在 `scripts/data_sources_pipeline` dev 模式块中新增：

```bash
export PLAYWRIGHT_BROWSERS_PATH="$HOME/.cache/ms-playwright"
```

（原来 dev 模式只有 `LD_LIBRARY_PATH`，没有设置浏览器路径。）

### 9.5 Playwright 1.58.0 行为变更

| 模式 | 查找的二进制 |
|------|-------------|
| `headless=True`（默认） | `chromium_headless_shell-1208/.../chrome-headless-shell` |
| `headless=False` | `chromium-1208/.../chrome` |

Playwright 1.58.0 将 headless 与 full browser 拆成两个独立浏览器包，不再是同一个二进制加 `--headless` 参数。我们的 Google Chrome 二进制只有一个，因此用软链接让 headless shell 路径指向同一文件，避免复制 264MB 的重复数据。

### 9.6 影响范围

- **6 月 4 日起** dev 模式下 Fetcher 只成功下载了 SHFE 数据（3 个文件/天），其他交易所全部缺失
- 此前（4 月 20 日~6 月 3 日）每天 18~19 个文件完整下载
- 生产环境（server202, Ubuntu 22.04）不受影响，`PLAYWRIGHT_BROWSERS_PATH=/home/data_ops/playwright-browsers` 正常
- **修复后无任何 fetcher.py 代码改动**

---

## 7. 文件索引

| 文件 | 说明 |
|------|------|
| `src/data_sources/services/collect_announcements_service.py` | 采集服务主代码 |
| `scripts/service_manager.sh` | 服务启停管理 |
| `docs/announcement-dev-log.md` | 公告采集整体设计讨论 |
| `docs/collect_announcements_service_devlog.md` | 本文件：采集服务开发日志 |
| `data/raw/announcements/exchanges/CFFEX/` | CFFEX 公告数据目录 |
| `data/raw/announcements/exchanges/DCE/` | DCE 公告数据目录 (API + Tavily 混合方案) |
| `data/raw/announcements/announcements_metadata.json` | 全局元数据索引 |

## 10. TODO (待办事项)

### 10.1 交易参数扩展解析 (暂缓)

当前 `parse_announcement_fields()` 仅提取 `minoq` / `maxoq`。需扩展以下字段类型：

| 字段 | 说明 | 优先级 |
|------|------|--------|
| `limit_up` / `limit_down` | 涨跌停价调整 | P1 |
| `tick_size` | 最小变动价位 | P1 |
| `limit_rate` | 涨跌停板幅度 | P1 |
| `margin_rate` | 交易保证金比例 | P1 |
| `open_qty` | 开仓手数/开仓量 | P1 |
| `position_limit` | 持仓限额 | P2 |

这些字段通过 `fields_from_announcements.csv` 的 `field` 列扩展，不另起文件。

### 10.2 交易参数算法调整解析 (待设计)

公告中可能存在对参数**计算方法**的变更，例如：
- 涨跌停价计算公式变更（如四舍五入改为向下取整）
- 由于最小变动价位调整导致的上一日结算价调整方法

这类变更涉及时序逻辑和语义理解，需要特殊的数据结构和 LLM 提示词设计。存储格式尚未确定，可能方案：
- 纯文本描述字段 `algorithm_change_desc`
- 结构化规则表（影响品种、生效日期、变更前后描述）

当前状态：记录需求，待后续版本设计。

### 10.3 CSV 质量问题跟踪 (2026-06-12)

| 问题 | 说明 | 状态 |
|------|------|------|
| DCE pub_date 为空 | CMS API 未返回 pub_date，需用 showDate 回退或从 URL 提取 | ✅ 已修复 |
| product_code 格式不统一 | 部分条目返回 "DCE A" 应为 "A" | 待 LLM prompt 优化 |


### 10.4 LLM 字段分类问题 (2026-06-12)

**首次全量运行发现的问题**：

| 问题 | 举例 | 根因 |
|------|------|------|
| minoq 全部误标为 maxoq | "最小开仓下单数量调整为8手" → field=maxoq | prompt 未区分 min/max |
| 跨交易所产品 | BZ 出现在 CFFEX, SC 出现在 SHFE | prompt 未约束交易所品种范围 |
| 品种代码格式错误 | "DCE A" 应为 "A"，"ag" 应为 "AG" | 未统一大小写 |
| 合约级条目混入 | IM2208、HO2301 等合约月份 | 当公告列了具体合约时 LLM 直接输出 |
| 概念混淆 | "单日开仓量"被当成 maxoq | 未定义易混概念边界 |

**典型案例**：[GFEX 碳酸锂交易限额通知](http://www.gfex.com.cn/gfex/tzts/202601/25a2dd07d2094c0498609b284ec78492.shtml)中提到"单日开仓量"，定义为"非期货公司会员或者客户当日在单个合约上的买开仓数量与卖开仓数量之和"——这是单日开仓限额，**不等于** minoq（单笔最小开仓量）。LLM 必须严格区分这两个概念。

**已修复** (2026-06-12)：
- prompt 中增加字段定义（maxoq/minoq 明确含义）
- 增加"严禁混淆"清单（单日开仓量、持仓限额、交易限额 ≠ maxoq/minoq）
- 增加交易所品种白名单约束
- 相关性预判增加"单日开仓限额不在此列"提示
