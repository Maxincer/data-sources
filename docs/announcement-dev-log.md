# 公告采集与解析 — 开发讨论记录

## 1. 项目背景

`NotifierWatchdog` 数据源项目需要对六大期货交易所的公告进行监控，发现影响数据管线的参数变更（最小变动价位、涨跌停板幅度、最大/最小开仓手数等）。

## 2. 文件结构设计

### 2.1 公告存储

```
data/raw/announcements/
├── announcements_metadata.jsonl   # 公告元数据（独立于原来的 metadata.jsonl）
├── {id}.html                      # 公告原始 HTML
└── {id}.txt                       # 提取的纯文本
```

### 2.2 公告元数据 schema（设计中）

阶段一（发现）+ 阶段二（下载）需要的字段：

- `id` — 唯一标识
- `exchange` — 交易所
- `url` — 公告原始链接
- `title` — 公告标题（列表页可能截断，待解析确认）
- `publish_date` — 发布日期（从列表页获取，可能不精确）
- `source_file` — 本地 HTML 路径
- `source_txt` — 本地文本路径
- `crawled_at` — 下载时间
- `status` — pending / downloaded / parse_failed

阶段三（解析，后续独立脚本）可能增加的字段：
- `publish_time` — 精确发布时间
- `effective_date` — 生效日期
- `relevant_fields` — 涉及的参数字段
- `variety_ids` — 涉及的品种代码
- `changes` — 变更详情（旧值→新值、合约范围）
- `summary` — 摘要
- `announcement_type` — 公告类型

### 2.3 公告采集 vs 解析的分工

- `collect_announcements_service.py` 只负责采集（发现 + 下载），不负责解析
- 元数据中采集阶段能拿到的信息有限（列表页标题可能截断、日期可能不精确）
- 解析阶段由后续独立脚本负责（计划用智谱 GLM5 Coding Plan API）

## 3. 各交易所列表页采集方案

### 3.1 可用的交易所

| 交易所 | 列表页 URL | 方法 | 备注 |
|---|---|---|---|
| GFEX | `http://www.gfex.com.cn/gfex/tzts/list_yw.shtml` | 静态 HTML，直接解析 `<a>` | 链接格式 `/gfex/tzts/{YYYYMM}/{uuid}.shtml` |
| CFFEX | `http://www.cffex.com.cn/cn/jysgg.html` | 分页 HTML，直接解析 | 链接格式 `/cn/jysgg/{YYYYMMDD}/{NNNNN}.html`，~20 页 |
| SHFE | `https://www.shfe.com.cn/publicnotice/notice/` | WAF JS 挑战，需 `wait_until='load'` + 5s sleep | 同时包含 INE 公告，链接格式 `/publicnotice/notice/{YYYYMM}/t{YYYYMMDD}_{NNN}.html` |
| INE | `https://www.ine.cn/publicnotice/notice/` | 服务端渲染 HTML，`.table_item_info` 结构 | 链接格式 `./{YYYYMM}/t{YYYYMMDD}_{NNN}.html`，~55 页 |

### 3.2 当前阻塞的交易所

| 交易所 | 列表页 URL | 问题 |
|---|---|---|
| CZCE | `https://www.czce.com.cn/cn/gyjys/jysdt/ggytz/` | WAF 站点级拦截（WSL2 IP 被拉黑） |
| DCE | `http://www.dce.com.cn/dce/content/ywggytz/` | WAF 站点级拦截（WSL2 IP 被拉黑） |

**已确认信息**：
- CZCE/DCE 文章详情页可用 Playwright headless + `--disable-blink-features=AutomationControlled` 成功下载（418KB/117KB）
- 列表页同为 WAF 保护，但 IP 被临时拉黑无法测试
- CZCE 文章链接格式：`/cn/gyjys/jysdt/ggytz/webinfo/{YYYY}/{M}/{uuid}.htm`
- DCE 文章链接格式：`/dce/content/{YYYY}/ywggytz/{NNNNN}.html`

## 4. WAF 绕过方案

### 4.1 已验证可行的方法

```python
browser = p.chromium.launch(
    headless=True,
    executable_path=CHROME_BIN,
    args=[
        "--no-sandbox",
        "--disable-gpu",
        "--disable-blink-features=AutomationControlled",  # 关键：隐藏自动化标记
        "--window-size=1920,1080",
        "--disable-dev-shm-usage",
    ],
)
```

### 4.2 浏览器依赖问题（Ubuntu 26.04）
- Playwright 1.58.0 不支持 Ubuntu 26.04，无法通过 `playwright install` 安装浏览器
- 解决方案：手动下载 Google Chrome `.deb`，用 `dpkg-deb -x` 解压到 `/tmp/chrome-extract/`
- 缺失的系统库（libnspr4, libnss3, libasound2t64）同样下载 `.deb` 解压，通过 `LD_LIBRARY_PATH` 指向
- Chromium 路径：`/tmp/chrome-extract/opt/google/chrome/chrome`
- 依赖路径：`/tmp/chrome-deps/usr/lib/x86_64-linux-gnu`

### 4.3 SHFE 特有处理
- SHFE 列表页有 JS WAF 挑战
- 首次 `goto` 后页面是防火墙页面，等待 ~5 秒后 JS 自动放行，刷新为正常内容
- 需要 `wait_until='load'`（不能用 `domcontentloaded`），然后 `time.sleep(5)`

### 4.4 Tavily 方案对比

| 维度 | 直接扫列表页 | Tavily Search |
|---|---|---|
| 完整性 | ✅ 不遗漏 | ⚠️ 依赖搜索引擎索引 |
| 发布时间 | ✅ 页面提取 | ⚠️ 需二次抓取 |
| API 费用 | ✅ 无 | ❌ 按次计费 |
| 维护负担 | ⚠️ 每个交易所独立解析器 | ✅ 统一接口 |
| 结论 | **主方案** | 兜底验证 |

## 5. Order Limits CSV 改造

2026-06-04 完成对四个 `order_limits_*.csv` 文件的改造：

1. **`adjustments.csv`**：`announcement_ref` 列已替换为 metadata.jsonl 中的完整公告原标题
2. **所有四个文件**：
   - 新增 `announcement_date`、`effective_date`、`source_url` 三列
   - 消除了所有 `source`、`evidence` 列中的"同上"缩写
   - `general_rules.csv`、`product_rules.csv`、`summary.csv` 的三列值待填充（需查各规则文档的发布信息）

## 6. 待办事项

- [ ] CZCE/DCE 列表页 IP 解封后验证 DOM 结构，完善解析器
- [ ] 实现 `collect_announcements_service.py` 常驻进程（含列表页解析 + 详情页下载）
- [ ] 设计 `announcements_metadata.jsonl` 完整 schema
- [ ] 实现独立解析脚本（GLM5 API 解析公告内容）
- [ ] 填充 `general_rules.csv` / `product_rules.csv` / `summary.csv` 的日期和链接列
- [ ] Chromium 浏览器路径在目标 Ubuntu 服务器上正规化（不用 `/tmp` 临时方案）

## 7. 相关文件清单

```
scripts/
├── announcement_watcher.py          # 公告搜索+分析+告警（Tavily方式）
├── check_announcements.py           # 公告页面连通性检查
├── collect_announcements_service.py # ★ 公告采集服务（开发中，当前为配置骨架）
├── download_announcements.py        # ★ 公告详情页下载工具
├── order_limits_updater.py          # Order Limits CSV 更新
└── transform_order_limits.py        # ★ CSV 列改造工具

config/proofs/                       # 交易所规则原文证明文件
data/raw/announcements/              # 公告原始 HTML + metadata
data/order_limits_*.csv             # 委托限制规则文件
```
