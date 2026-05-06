# data-sources — 期货数据管道

## 概述

从 CFE（中金所）、CZC（郑商所）、DCE（大商所）、GFE（广期所）、INE（能源中心）、SHF（上期所）六家期货交易所官网，定时下载结算参数、日行情、交易参数等原始数据，解析后写入 MySQL 数据库 `t_futures_info_exchange`。

目标：替代原 Wind 数据源填充的 `t_futures_info` 表，实现自主可控、全量覆盖的期货数据体系。

## 架构

```
Fetcher (下载) → Parser (解析) → Modifier (修正) → Writer (入库) → Reporter (报告/飞书)
                                                                      ↑
                                                              Verifier (对比验证)
```

## 数据流

### 下载

每天通过各交易所 API/网页 下载三类文件：
- `{date}.{exchange}.DailyMarketData.{ext}` — 日行情（OHLCV/oi/settle）
- `{date}.{exchange}.SettlementParameters.{ext}` — 结算参数（settle/margin）
- `{date}.{exchange}.TradingParameters.{ext}` — 交易参数（maxup/maxdown/margin/minoq/maxoq）

文件保存至 `data/raw/`，元数据（文件大小、变化率）记录在 `.metadata.jsonl`。

### 解析 → 修正 → 入库

```
parse_file() → merge_by_code_date() → _apply_modifiers() → upsert_records()
```

修正规则集中在 `modifier.py` 中按顺序执行（详见下方"Modifier 规则"）。

## Modifier 规则（人为设定的数据调整）

所有规则集中在 `modifier.py`，通过 `writer.py` 的 `_apply_modifiers()` 按序调用。

### 1. 节假日风控参数修正（核心规则）

> **背景**：交易所（DCE/CZC/SHFE/INE/GFE）在节假日前会调整涨跌停比例和保证金率（风控参数）。API 在调整日返回的是假日标准值，但 `t_futures_info`（原 Wind 表）中使用的是调整前的标准值。

**规则：date T 使用的涨跌停价和保证金率 = date T-1（前一交易日）API 的原始返回值。**

即：不重新计算，直接继承前一交易日的官方值。这确保新表与原表口径一致。

**适用交易所和对象：**

| 交易所 | 受影响字段 | 函数 |
|:-----:|:---------|------|
| DCE | `maxup`, `maxdown`, `long_margin`, `short_margin` | `fix_dce_limit_prices()` |
| CZC | `long_margin`, `short_margin` | `fix_all_margin()` → `_fix_margin_inherit()` |
| INE | `long_margin`, `short_margin` | 同上 |
| SHF | `long_margin`, `short_margin` | 同上 |
| GFE | `long_margin`, `short_margin` | `fix_gfe_margin()` |

### 2. 涨跌停取整精度修正

**背景**：`price × (1 + rate)` 的浮点运算会产生 `5940 × 1.10 = 6534.000000000001` 这类噪声，导致 `ceil(6534.000000000001) = 6535`（应为 6534）。

**方案**：使用 `safe_ceil(value, tick)` / `safe_floor(value, tick)` 替代原生 `math.ceil()` / `math.floor()`。函数内部先 `round(value / tick, 6)` 消除 FP 噪声再取整。

- `tick` 参数必传（最小变动价位）

### 3. TAS 合约结算价填充

`fill_tas_settle()` — TAS（结算价交易）合约的结算价等于同月份常规期货合约的结算价。

### 4. EFP 合约过滤

`should_filter_contract()` — EFP（期转现）合约在 DB 中无对应记录，入库前过滤。

### 5. 零交易合约 close 回退

`fill_zero_volume_close()` — 日行情文件只包含有交易的合约。零交易合约的 `close` 无值，按交易所规则回退为 `settle`。

### 6. CSI 指数数据过滤

Writer 中过滤 `.CSI` 后缀的指数数据（`t_futures_info` 为纯期货数据表，不包含指数）。

### 7. GFE 非历史 API 保护

GFE 交易参数和日行情 API 不支持按日期查询历史数据（始终返回最新快照）。Fetcher 中自动跳过非当日的 GFEX 数据请求，保护已有历史文件不被当前快照覆盖。GFE 结算参数 API 使用数组格式 `["20260429"]` 可正确获取历史数据。

### 8. 数据库 schema

数值字段使用 `DECIMAL` 替代 `FLOAT`，避免大数值（如 `amt` 上亿元）的精度丢失。

## 可允许异常（与原表一致，非数据问题）

以下异常由数据源自身的限制导致，新旧表表现一致，不计为差异。

### DCE 远月合约 OHLC 缺失

DCE 的纯苯（BZ）、焦炭（J）等品种的远月合约，API 仅返回主力合约的 OHLC（开盘/最高/最低价）。
远月合约即使有成交（amt ≠ 0），open/high/low 也为 NULL。

**示例（2026-05-06）：**

| 合约 | 品种 | open | amt | 说明 |
|------|------|:----:|:---:|------|
| bz2612.DCE | 纯苯远月 | NULL | 226,000 | API 不提供远月 OHLC |
| bz2701.DCE | 纯苯远月 | NULL | 223,000 | 同上 |
| j2703.DCE | 焦炭远月 | NULL | 195,300 | 同上 |
| j2704.DCE | 焦炭远月 | NULL | 199,300 | 同上 |

新旧表均表现为 NULL，该异常长期存在且可预期。

### INE TAS 合约字段缺失

SC（原油期货）的 TAS（结算价交易）合约由 INE DailyMarketData 生成记录，
但 TradingParameters 和 SettlementParameters 中不包含 TAS 合约的风控参数。

新表中已对 TAS 合约做入库前过滤（`should_filter_contract()`），TAS 合约不入库。
作为参考，TAS 合约缺失的字段包括：

| 字段 | 说明 |
|------|------|
| `oi` | 持仓量（TAS 无持仓数据） |
| `maxup` / `maxdown` | 涨跌停价（TAS 无此设定） |
| `long_margin` / `short_margin` | 保证金率（TAS 无此设定） |
| `volume` / `amt` | 成交量和金额（TAS 在日行情中另有统计） |

以上缺失属于 API 数据源的自然限制，非 pipeline 处理问题。

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

CZCE 官网在收盘后可能对日行情数据进行微调。Wind 抓取的是修正前版本，新表使用的是终版数据。

| 合约 | 字段 | 原表 | 新表 | 差异 |
|:----:|:----:|:---:|:---:|:----:|
| SA2605.CZC | volume/oi/amt | 27,323 / 47,995 / 638,630,000 | **27,731** / **47,587** / **648,138,000** | +408 / -408 / +9,508,000 |

以交易所官网终版数据为准。

#### D. 成交额（amt）精度差

SHFE/INE 原始 API 返回的 TURNOVER 值包含更多小数位（如 `89434.945`），网页展示时保留两位（`89,434.95`）。原表是 FLOAT 类型，存储时精度丢失。新表使用 DECIMAL 类型，精度完整。

偏差范围：0.0001%~0.06%，绝对金额差 10~1000 元。认定为准许误差。

#### E. CZCE 成交额万元换算精度差

CZCE 成交额在文件中以万元为单位（2 位小数），×10000 换算为元时产生微小舍入差。

偏差范围：< 0.05%（绝大部分 < 0.01%）。认定为准许误差。

#### F. 零交易合约

零交易合约无 `close` 值，新表回退为 `settle`，与原表一致。不计为差异。

#### G. minoq/maxoq 字段

新表从 CZCE 交易参数文件中完整解析最小/最大开仓量，覆盖全部 241 条 CZC 合约。原表仅覆盖 36 条。新表数据更完整，不计为差异。

### 最终验证结果（20260429）

| 交易所 | 合约数 | 字段零差异率 | 说明 |
|:-----:|:------:|:----------:|------|
| CFE | 28/28 | 100% | ✅ |
| DCE | 250/250 | 100% | ✅（含修正后） |
| INE | 67/67 | 100% | ✅ |
| SHF | 238/238 | 100% | ✅ |
| CZC | 241/241 | >99% | SA2605 终版修正（准许），amt 微差（准许） |
| GFE | 48/48 | 0%（涨跌停） | API 限制（准许） |

## 定时任务

通过 crontab 交易日 16:30 执行完整流水线（脚本自行判定交易日，非交易日跳过）：

```
30 16 * * 1-5 /mnt/e/projects/data-sources/scripts/pipeline.sh
```

流水线步骤：
1. `Fetcher` — 下载各交易所当日原始数据
2. `Writer` — 解析 → Modifier 修正 → 写入 `t_futures_info_exchange`
3. `Reporter` — 生成三合一日度验证报告并发送飞书 webhook

日志：`~/logs/cron.log`

## 关键文件

| 文件 | 职责 |
|------|------|
| `src/data_sources/fetcher.py` | 数据下载（各交易所 API） |
| `src/data_sources/parser.py` | 原始文件解析 |
| `src/data_sources/modifier.py` | **数据调整规则（本文档核心）** |
| `src/data_sources/writer.py` | 修正链编排 + DB 写入 |
| `src/data_sources/verifier.py` | 双表全面对比验证 |
| `src/data_sources/reporter.py` | 日度验证报告 + 飞书通知 |
| `src/data_sources/db.py` | 数据库连接与 schema |
| `src/data_sources/constants.py` | 交易所 API 基础 URL |
| `scripts/pipeline.sh` | crontab 定时流水线脚本 |

---

## 交易日历工具（trade_date.py）

**文件位置：** `src/data_sources/trade_date.py`
**数据文件：** `data/trade_dates.txt`（纯文本 8797 行）

### 存储方案

纯文本，每行一个 YYYYMMDD 整数，升序排列。

```
19901219
19901220
...
20261231
```

### 方案取舍

| 方案 | 舍弃原因 |
|------|----------|
| JSON 字符串数组 | 生成 Python 对象前需 json.load 完整解析，shell 查日期需 jq |
| JSON 整型数组 | 同上，shell 不友好 |
| SQLite | 为 9k 行数据开一个 DB 太重，运维成本高 |
| Python .py 字面量 | Python 内最快，shell 无法直接读取 |
| ✅ 纯文本逐行 | shell 原生支持（grep/awk 零依赖），Python 同等高效 |

### 核心原因

**1. Shell 原生操作，零格式转换**

运营场景需要在命令行快速读取并赋值环境变量：

```bash
LAST_TRADE_DATE=$(awk -v d=20260506 '$1<=d{last=$1} END{print last}' data/trade_dates.txt)
```

日期 `20260506` 在 shell 中是裸整数，不需要引号，不需要 `jq`，不需要 `date -d` 转格式。纯文本文件一行一个整数，grep/awk 直接操作。

**2. Python 模块级缓存，一次读盘**

`load()` 首次调用读入文件后驻留内存（`list[str]` ≈ 80KB），后续所有查询走二分查找 O(log n)，不再读盘。

**3. 快速人工 hack**

临时调整只需一条 shell 命令修改 `trade_dates.txt`，重启进程即刻生效，不依赖任何数据源：

```bash
# 增加交易日
echo "20260203" >> data/trade_dates.txt && sort -o data/trade_dates.txt data/trade_dates.txt
# 去掉交易日
sed -i '/^20260131$/d' data/trade_dates.txt
```

### 数据源策略

| 层级 | 方式 | 频率 |
|------|------|------|
| 自动 | tushare pro.trade_cal (SSE, 1990~当前+1年) | 每月 crontab 一次 |
| 手动 | 临时调整时直接编辑 trade_dates.txt | 按需 |

tushare 的数据来自上交所官方 API，已覆盖年度常规休市和 COVID 等临时调整。每月一次 update() 按年分段请求（37 次 API），每次间隔 0.6s，远低于限流阈值。

### 接口

```python
def update(data_dir: Optional[Path] = None) -> list[str]      # 拉取并持久化
def load(force: bool = False) -> list[str]                     # 读文件（模块缓存，force=True 强制刷盘）
def nearest(d) -> Optional[str]                                # 含自身，向前最近
def prev_trading_date(d) -> Optional[str]                      # 不含自身，真正上一天
def is_trading(d) -> bool                                      # 是否交易日
```

手工编辑 `trade_dates.txt` 后刷新缓存：

```python
from trade_date import load, nearest
load(force=True)                # 重新读盘
n = nearest(20260506)           # 用新数据查询
```

CLI：`python trade_date.py nearest|prev|is_trading <YYYYMMDD>|--update`


## CFFEX 结算参数表（不定时发布）

### 背景

CFFEX（中金所）有三种文件：

| 文件 | 发布频率 |
|------|----------|
| `DailyMarketData`（日行情） | **每日**发布，每个交易日都有 |
| `SettlementParameters`（结算参数表） | **不定时**，仅在结算参数变化时发布 |
| `TradingParameters`（交易参数表） | **不定时** |

### 无发布日的处理

Fetcher 每天仍会尝试下载。CFFEX 当天未发布时服务器返回 HTML 错误页，Fetcher 识别后标记 `no_data` 不报错：

```python
# fetcher.py _fetch_cffex_settlement
if "HTML error page" in reason:
    return {"success": True, "no_data": True}
```

Writer 扫描原始目录时未找到该日对应文件，则该日该类型字段在 DB 记录中为 **NULL**（而非条目不存在）。日行情文件（DailyMarketData）每日发布，其余字段正常写入。

**结果：** DB 中 CFFEX 结算/交易参数的 settle / maxup / maxdown / margin 等字段在未发布日可能为 NULL。旧表（Wind 来源）对这些字段做了继承填充，新表目前尚无继承逻辑。
