# data-sources — 自主可控的期货数据管道

替代原 Wind 数据源填充的 `t_futures_info` 表，从 7 家交易所官网定时下载结算参数、日行情、交易参数等原始数据，解析后写入 MySQL，实现自主可控的期货数据体系。

---

## 架构总览

```
                                    ┌───────────┐
                                    │  rutask   │
                                    │ (定时调度) │
                                    └─────┬─────┘
                                          │ 工作日 16:40
                                          ▼
                ┌───────────────────────────────────────────────────┐
                │            update_if_exclude_wsi.sh               │
                │                 (data-crawler)                    │
                │                                                   │
                │  ┌────────────────┐                               │
                │  │  t_futures.py  │ → t_futures                   │
                │  └────────────────┘                               │
                │                                                   │
                │  ┌──────────── data_sources_pipeline ──────────┐  │
                │  │               ← 替代 t_futures_info.py       │  │
                │  │                                              │  │
                │  │  ┌──────────────────┐  ┌─────────────────┐  │  │
                │  │  │ ① Collect        │  │ ③ Fetcher       │  │  │
                │  │  │   Announcements  │  │   → 交易所 API  │  │  │
                │  │  └───────┬──────────┘  └────┬────────────┘  │  │
                │  │          ▼                   │               │  │
                │  │  ┌──────────────────┐        │               │  │
                │  │  │ ② Analyse        │        │               │  │
                │  │  │   Announcements  │        │               │  │
                │  │  │   (LLM → CSV)    │        │               │  │
                │  │  └───────┬──────────┘        │               │  │
                │  │          │                   │               │  │
                │  │          └───────┬───────────┘               │  │
                │  │                  ▼                           │  │
                │  │  ┌──────────────────────────────────────┐    │  │
                │  │  │           Parser (parse_file)         │    │  │
                │  │  └──────────────┬───────────────────────┘    │  │
                │  │                 ▼                            │  │
                │  │  ┌──────────────────────────────────────┐    │  │
                │  │  │      Modifier (_apply_modifiers)      │    │  │
                │  │  │  ① 节假日风控参数修正                 │    │  │
                │  │  │  ② 涨跌停取整精度修正                 │    │  │
                │  │  │  ③ TAS 结算价填充                    │    │  │
                │  │  │  ④ close 回退 settle                 │    │  │
                │  │  │  ⑤ margin 继承/回填                  │    │  │
                │  │  │  ⑥ if_basis 计算                     │    │  │
                │  │  └──────────────┬───────────────────────┘    │  │
                │  │                 ▼                            │  │
                │  │  ┌──────────────────────────────────────┐    │  │
                │  │  │ ④ Writer (INSERT ... ON DUPLICATE)   │    │  │
                │  │  │     → t_futures_info（生产表）        │    │  │
                │  │  └──────────────┬───────────────────────┘    │  │
                │  │                 ▼                            │  │
                │  │  ┌──────────────────────────────────────┐    │  │
                │  │  │    Verifier (compare_all)             │    │  │
                │  │  │     → exchange vs WSS 逐字段对比     │    │  │
                │  │  └──────────────┬───────────────────────┘    │  │
                │  │                 ▼                            │  │
                │  │  ┌──────────────────────────────────────┐    │  │
                │  │  │ ⑤ Reporter                           │    │  │
                │  │  │     → 飞书报告 + 邮件                │    │  │
                │  │  │     → _evaluate_rollback() 验收      │    │  │
                │  │  │                                      │    │  │
                │  │  │  验收通过 ✅ → exit(0)               │    │  │
                │  │  │  验收不通过 🔄 → DELETE 当日行       │    │  │
                │  │  │                    → exit(非0)        │    │  │
                │  │  └──────────────────────────────────────┘    │  │
                │  └──────────────────────────────────────────────┘  │
                │                                                     │
                │  ┌── [回滚] ────────────────────────────────┐      │
                │  │  pipeline exit(非0) → t_futures_info.py   │      │
                │  │  (Wind 回退，INSERT IGNORE 补回)          │      │
                │  └───────────────────────────────────────────┘      │
                │                                                     │
                │  ┌────────────────┐                                 │
                │  │t_main_contracts│ → t_main_contracts               │
                │  └────────────────┘                                 │
                │  ┌──────────────────┐                               │
                │  │t_futures_tradable│ → t_futures_tradable           │
                │  └──────────────────┘                               │
                └─────────────────────────────────────────────────────┘
```

### 阶段演进

| 阶段 | 写入表 | 验证方式 | 状态 |
|------|--------|---------|------|
| **阶段一** | `t_futures_info_exchange`（过渡表） | 两表对比 + WSS | ✅ 已稳定运行 |
| **阶段二** | `t_futures_info`（生产表） | WSS 交叉验证 + 自动回滚 | 🔄 切换中 |

### 自动回滚机制

```
Reporter: exchange vs WSS 逐合约逐字段对比
  │
  ├── 条件 a: exchange=NULL, WSS≠NULL
  │     → 回滚 ❌（TAS 合约豁免）
  │
  ├── 条件 b1: 行情字段不精确相等
  │     → 回滚 ❌
  │     ├── settle 例外：WSS=0 且 exchange≠0
  │     │     → 豁免 ✅（Wind 数据异常，以我方为准）
  │     └── 其余价格字段：精确相等判定
  │
  ├── 条件 b2: volume/amt/oi 比例>0.001
  │       if_basis 绝对值≥0.001
  │     → 回滚 ❌
  │     ├── oi 不参与回滚判定
  │     │     → 豁免 ✅（oi=0 为合法值，新/到期合约常见）
  │     └── volume/amt：比例 0.1% 阈值
  │         if_basis：绝对值 0.001 阈值
  │
  └── 条件 c: exchange≠NULL, WSS=NULL
        → 不触发 ✅
```

---

## 数据流

### 数据源

**7 家交易所** × 3 类核心文件：

| 文件类型 | 内容 |
|---------|------|
| `{date}.{exchange}.DailyMarketData.{ext}` | 日行情（OHLCV/oi/settle） |
| `{date}.{exchange}.SettlementParameters.{ext}` | 结算参数（settle/margin） |
| `{date}.{exchange}.TradingParameters.{ext}` | 交易参数（maxup/maxdown/margin/minoq/maxoq） |

**SHFE / INE** 额外：
| 文件类型 | 内容 |
|---------|------|
| `{date}.{exchange}.ProductConfig.html` | 产品规格配置页（Playwright 采集） |

**CSI**：
| 文件类型 | 内容 |
|---------|------|
| `{date}.CSI.MarketData.json` | 中证指数日行情 |

### 公告采集 — order_limits 系统

公告采集与分析是 pipeline 的 Step 1-2，独立于主数据流运行。

```
CollectAnnouncementsService → 爬取 4 家交易所公告列表
  │     (CFFEX/GFEX/INE/SHFE)
  │
  ▼
AnalyseAnnouncementsService → LLM 分析变更
  │     (ZHIPU_API_KEY)
  │
  ▼
自动更新 order_limits_summary.csv → inject_order_limits() 合并到 writer 输出
```

规则层级（从低到高）：

| 层级 | 文件 | 说明 |
|:---:|------|------|
| 1 | `order_limits_general_rules.csv` | 交易所级通用规则 |
| 2 | `order_limits_product_rules.csv` | 品种级例外规则 |
| 3 | `order_limits_adjustments.csv` | 公告级调整记录 |
| 4 | `order_limits_summary.csv` | 前三层合并后的当前值 |

---

## Modifier 规则

### 1. 节假日风控参数修正（核心规则）

**背景**：交易所（DCE/CZC/SHFE/INE/GFE）在节假日前会调整涨跌停比例和保证金率（风控参数）。API 在调整日返回的是假日标准值，但 `t_futures_info`（原 Wind 表）中使用的是调整前的标准值。

**规则**：`date T` 使用的涨跌停价和保证金率 = `date T-1`（前一交易日）API 的原始返回值。

即：不重新计算，直接继承前一交易日的官方值，确保新表与原表口径一致。

| 交易所 | 受影响字段 | 函数 |
|:-----:|:---------|------|
| DCE | maxup, maxdown, long_margin, short_margin | `fix_dce_limit_prices()` |
| CZC/INE/SHF | long_margin, short_margin | `fix_all_margin()` → `_fix_margin_inherit()` |
| GFE | long_margin, short_margin | `fix_gfe_margin()` |

### 2. 涨跌停取整精度修正

**背景**：`price × (1 + rate)` 的浮点运算会产生 `5940 × 1.10 = 6534.000000000001` 这类噪声，导致 `ceil(6534.000000000001) = 6535`（应为 6534）。

**方案**：使用 `safe_ceil(value, tick)` / `safe_floor(value, tick)` 替代原生 `math.ceil()` / `math.floor()`。函数内部先 `round(value / tick, 6)` 消除 FP 噪声再取整。

- `tick` 参数必传（最小变动价位）

### 3. TAS 合约结算价填充

TAS 合约结算价 = 同月份常规合约结算价。

### 4. close 回退 settle

`fill_zero_volume_close()` — 当合约的 `close` 为空但 `settle` 有值时，将 `close` 设为 `settle`。

常见于以下情况：
- **零交易合约**：日行情文件只包含有交易的合约，未列出的零交易合约无 `close`
- **低成交近交割合约**：如 PX2605、eb2605 等 5 月到期合约，即使有少量成交（< 500 手），交易所行情中也无有效 OHLC 数据，但 Wind 等数据源会用 `settle` 填充 `close`

此规则与 Wind 的处理方式保持一致。

### 5. CFFEX margin 从历史继承

`fill_cffex_margin_from_history()` — CFFEX 结算参数不定时发布，无发布日从最近的 `*CFFEX.SettlementParameters.csv` 中读取 margin 回填。

### 6. if_basis 计算

`fill_if_basis()` — 计算 `close - settle` 填充 `if_basis` 字段。

### 7. EFP 合约过滤

`should_filter_contract()` — EFP（期转现）合约在 DB 中无对应记录，入库前过滤。

### 8. CSI 指数数据过滤

Writer 中过滤 `.CSI` 后缀的指数数据（`t_futures_info` 为纯期货数据表）。

### 9. GFE 非历史 API 保护

GFE 交易参数和日行情 API 不支持按日期查询历史数据（始终返回最新快照）。Fetcher 中自动跳过非当日的 GFEX 数据请求，保护已有历史文件不被当前快照覆盖。GFE 结算参数 API 使用数组格式 `["20260429"]` 可正确获取历史数据。

### 10. 数据库 schema

数值字段使用 `DECIMAL(20,2)` 替代 `FLOAT`，避免大数值（如 `amt` 上亿元）的精度丢失。

### 11. CFFEX 结算参数表（不定时发布）

CFFEX（中金所）三种文件发布频率不同：

| 文件 | 发布频率 |
|------|----------|
| `DailyMarketData` | **每日**发布，每个交易日都有 |
| `SettlementParameters` | **不定时**，仅在结算参数变化时发布 |
| `TradingParameters` | **不定时** |

Fetcher 每天仍会尝试下载。当天未发布时服务器返回 HTML 错误页，Fetcher 识别后标记 `no_data` 不报错：

```python
if "HTML error page" in reason:
    return {"success": True, "no_data": True}
```

继承逻辑：`fill_cffex_margin_from_history()` 在 Writer 中自动调用（见 modifier 规则 5），当日无结算文件时从最近的 `*CFFEX.SettlementParameters.csv` 中读取 margin 值回填，确保 `long_margin` / `short_margin` 不因未发布日而缺失。

---

## 可允许异常（与 Wind 表一致，非数据问题）

以下异常由数据源自身限制导致，新旧表表现一致，不计为差异。

### 量小合约 OHLC 缺失（DCE 为主）

DCE 低流动性合约（冷门品种或临近到期），API 不返回 Open/High/Low。即使有成交（amt ≠ 0），open/high/low 也为 NULL，仅提供 close 和 settle。

典型场景：
- **冷门品种**（纯苯 BZ、焦炭 J 等）：成交量极低
- **临近到期合约**：进入交割月后无场内竞价

启发式识别规则：`open/high/low` 全部 NULL、`close` 有值、`volume < 300`。

### INE TAS 合约字段缺失

TAS（结算价交易）合约的 TradingParameters 和 SettlementParameters 中不包含风控参数。新表中已做入库前过滤，TAS 合约不入库。

### 期货合约覆盖率

| 交易所 | 原表合约数 | 新表合约数 | 差异说明 |
|:-----:|:---------:|:---------:|---------|
| CFE | 28 | 28 | ✅ 一致 |
| CZC | 241 | 241 | ✅ 一致 |
| DCE | 250 | 250~253 | +3 月均价合约（F 后缀），原表无 |
| GFE | 48 | 48 | ✅ 一致 |
| INE | 67 | 67 | ✅ 一致 |
| SHF | 238 | 238 | ✅ 一致 |

### 字段数据 — 准许误差类别

#### B. GFE 涨跌停价 — 已知 API 限制

| 差异 | 原因 | 状态 |
|:----:|------|:----:|
| 48 条全部不同 | GFE 交易参数 API 不支持按日期查询，无法获取历史涨跌停价 | 已知限制 |

#### C. 数据源修正差异

CZCE 官网收盘后可能对日行情数据进行微调。Wind 抓取的是修正前版本，新表使用的是终版数据。

| 合约 | 字段 | 原表 | 新表 | 差异 |
|:----:|:----:|:---:|:---:|:----:|
| SA2605.CZC | volume/oi/amt | 27,323 / 47,995 / 638,630,000 | **27,731** / **47,587** / **648,138,000** | +408 / -408 / +9,508,000 |

以交易所官网终版数据为准。

#### D. 成交额（amt）精度差

SHFE/INE 原始 API 返回的 TURNOVER 值包含更多小数位（如 `89434.945`），网页展示时保留两位（`89,434.95`）。原表使用 FLOAT，精度丢失；新表使用 DECIMAL，精度完整。偏差范围 0.0001%~0.06%，认定为准许误差。

#### E. CZCE 成交额万元换算精度差

CZCE 成交额以万元为单位（2 位小数），×10000 换算为元时产生微小舍入差。偏差范围 < 0.05%，认定为准许误差。

#### F. 无 close 合约

合约 close 为空时，新表回退为 settle，与原表一致。不计为差异。

#### G. minoq/maxoq 字段

新表从 CZCE 交易参数文件中完整解析最小/最大开仓量，覆盖全部 241 条 CZC 合约。原表仅覆盖 36 条。新表更完整，不计为差异。

### 最终验证结果（20260429）

| 交易所 | 合约数 | 字段零差异率 | 说明 |
|:-----:|:------:|:----------:|------|
| CFE | 28/28 | 100% | ✅ |
| DCE | 250/250 | 100% | ✅（含修正后）|
| INE | 67/67 | 100% | ✅ |
| SHF | 238/238 | 100% | ✅ |
| CZC | 241/241 | >99% | SA2605 终版修正、amt 微差（准许）|
| GFE | 48/48 | 0%（涨跌停）| API 限制（准许）|

---

## 交易日历工具（trade_date.py）

**文件位置：** `src/data_sources/trade_date.py`
**数据文件：** `data/trade_dates.txt`（纯文本，升序排列的 YYYYMMDD）

### 存储方案取舍

| 方案 | 舍弃原因 |
|------|----------|
| JSON 字符串数组 | Python 前需 json.load 完整解析，shell 查日期需 jq |
| SQLite | 为 9k 行数据开一个 DB 太重 |
| Python .py 字面量 | Python 内最快，shell 无法直接读取 |
| ✅ **纯文本逐行** | **Shell 原生支持（grep/awk），Python 同样高效** |

### 核心原因

**1. Shell 原生操作**：`LAST_TRADE_DATE=$(awk -v d=20260506 '$1<=d{last=$1} END{print last}' data/trade_dates.txt)`，日期是裸整数，无需引号/jq/date 转格式。

**2. Python 模块级缓存**：`load()` 首次读入后驻留内存（≈ 80KB），后续查索引 O(log n) 二分查找，不再读盘。

**3. 快速人工 hack**：临时增删交易日只需一行 shell。

### 接口

| 函数 | 说明 |
|------|------|
| `load(force=False)` | 读文件（force=True 强制刷盘）|
| `nearest(d)` | 含自身，向前最近 |
| `prev_trading_date(d)` | 不含自身，真正上一天 |
| `is_trading(d)` | 是否交易日 |

手工编辑 `trade_dates.txt` 后刷新缓存：

```python
from trade_date import load, nearest
load(force=True)
n = nearest(20260506)
```

> **每年更新**：上交所每年 12 月发布次年交易日历，届时编辑 `data/trade_dates.txt` 追加即可。

---

## 关键文件

| 文件 | 职责 |
|------|------|
| `src/data_sources/fetcher.py` | 数据下载 |
| `src/data_sources/parser.py` | 原始文件解析 |
| `src/data_sources/modifier.py` | 数据调整规则 |
| `src/data_sources/writer.py` | 修正链编排 + DB 写入 |
| `src/data_sources/verifier.py` | 双表/数据源对比验证 |
| `src/data_sources/reporter.py` | 日度验证报告 + 飞书 + 邮件 |
| `src/data_sources/db.py` | 数据库连接与 schema |
| `src/data_sources/trade_date.py` | 交易日历工具 |
| `src/data_sources/configs.py` | 任务配置 + URL 常量 |
| `src/data_sources/services/collect_announcements_service.py` | 公告采集 |
| `src/data_sources/services/analyse_announcements_service.py` | 公告分析 |
| `scripts/data_sources_pipeline` | pipeline 入口脚本 |

---

## 部署

### 环境

| 组件 | 版本 |
|------|------|
| Python | >= 3.10 |
| MySQL | 8.0+ |
| Playwright Chromium | v1.58.0（离线包） |

### 安装

```bash
# 升级
tar -xzf 20260708-xxxx.tgz -C /tmp/data-sources
pip3 install --user --no-index --upgrade --find-links=/tmp/data-sources/pip data_sources
```

### 定时调度（rutask）

```bash
# /home/data_ops/rustask/rustask.local.json5
data_sources_pipeline: {
  "start_cron": "00 40 16 * * 1-5",
  "cmd": "bash ~/.local/bin/data_sources_pipeline",
  "output": ">>/home/data_ops/log/DataSourcesPipeline.$date.log",
  "safe_start": true,
  "hide_log": false,
}
```

生产模式下 pipeline 自动设 `--skip-table-compare`，仅做 WSS 交叉验证。

### 环境变量

| 类别 | 变量 | 说明 |
|------|------|------|
| 数据库 | `DB_HOST/DB_USER/DB_PASSWORD/DB_DATABASE` | 主库 |
| 比对库 | `REF_DB_*/REF_DB_PASSWORD/REF_DB_DATABASE` | Wind t_futures_info |
| 运行模式 | `DEV` | 开发模式 |
| 飞书 | `FEISHU_WEBHOOK` | 告警 webhook |
| 邮件 | `SMTP_HOST/PORT/PASSWORD/SENDER/RECIPIENTS` | 日报发送 |
| 交易所 | `DCE_API_KEY/DCE_API_SECRET` | DCE CMS |
| 行情 | `TUSHARE_TOKEN` | tushare |
| AI | `ZHIPU_API_KEY/DEEPSEEK_API_KEY` | LLM 公告分析/数据修正 |
| 搜索 | `TAVILY_API_KEY` | 公告采集搜索 |
| 系统 | `LD_LIBRARY_PATH` | Chromium 系统库路径 |

---

## 日志

| 文件 | 内容 |
|------|------|
| `{LOG_DIR}/Fetcher.*.log` | 下载日志 |
| `{LOG_DIR}/Writer.*.log` | 入库日志 |
| `{LOG_DIR}/Reporter.*.log` | 报告日志 |
| `{LOG_DIR}/CollectAnnouncementsService.*.log` | 公告采集日志 |
| `{LOG_DIR}/AnalyseAnnouncementsService.*.log` | 公告分析日志 |
| `{LOG_DIR}/DataSourcesPipeline.*.log` | pipeline 整体日志 |

日志文件单文件上限 100MB，保留 30 天。
