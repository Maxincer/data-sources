# Wind API → 官网数据源 替代方案

## 目标

将 `future_cn.t_futures_info` 的数据源从 Wind API 切换为各期货交易所官网，覆盖约 200 万条历史日线数据（6 个交易所，2001-2026）。

---

## 1. 现状分析

### 数据库表结构

```sql
t_futures_info (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  code        VARCHAR(128),    -- 合约代码 (e.g. IF2605.CFE)
  date        DATE,            -- 交易日
  open        FLOAT,           -- 开盘价
  high        FLOAT,           -- 最高价
  low         FLOAT,           -- 最低价
  close       FLOAT,           -- 收盘价
  volume      FLOAT,           -- 成交量
  amt         FLOAT,           -- 成交金额
  oi          FLOAT,           -- 持仓量
  settle      FLOAT,           -- 结算价
  maxup       FLOAT,           -- 涨停价
  maxdown     FLOAT,           -- 跌停价
  if_basis    FLOAT,           -- (未知, 保留)
  long_margin FLOAT,           -- 多头保证金率
  short_margin FLOAT,          -- 空头保证金率
  minoq       INT,             -- 最小下单量
  maxoq       INT,             -- 最大下单量
)
-- UNIQUE KEY: (code, date)
```

### 当前 data-sources 能获取的数据

| 交易所 | 当前文件 | 内容类型 | 含 OHLCV？ | 含 settle？ | 含 margin？ |
|--------|---------|---------|-----------|------------|------------|
| CFFEX  | `{}_1.csv` | **日行情数据** | ✅ | ✅ | ❌ |
| SHFE   | `js{}.dat` (JSON) | 结算参数 | ❌ | ❌ | ✅ |
| INE    | `js{}.dat` (JSON) | 结算参数 | ❌ | ❌ | ✅ |
| CZCE   | `FutureDataClearParams.txt` | 结算参数 | ❌ | ✅ settle | ✅ 保证金率 |
| GFEX   | JSON API | 结算参数 | ❌ | ❌ | ✅ |
| DCE    | JSON API | 结算参数 | ❌ | ❌ | ✅ |

**核心问题**：5/6 交易所的结算参数文件不含 OHLCV 数据（开盘/最高/最低/收盘/成交量/成交额），需要额外获取日行情数据。

---

## 2. 数据源映射

### 2.1 各交易所日行情数据源

| 交易所 | 数据源 | 覆盖字段 | 状态 |
|--------|--------|---------|------|
| CFFEX  | 现有 CSV（`{}_1.csv`） | open, high, low, close, volume, amt, oi, settle | ✅ 已有 |
| SHFE   | 需找日行情文件或 API | open, high, low, close, volume, amt, oi | ❌ 缺失 |
| INE    | 同 SHFE | open, high, low, close, volume, amt, oi | ❌ 缺失 |
| DCE    | 需找日行情数据 | open, high, low, close, volume, amt, oi | ❌ 缺失 |
| CZCE   | 需找日行情数据 | open, high, low, close, volume, amt, oi | ❌ 缺失 |
| GFEX   | 需找日行情数据 | open, high, low, close, volume, amt, oi | ❌ 缺失 |

### 2.2 各交易所结算参数数据源（当前已有）

| 交易所 | 数据源 | 覆盖字段 |
|--------|--------|---------|
| CFFEX  | 同上 CSV | ✅ settle（重复，同行情文件） |
| SHFE   | `js{}.dat` JSON | ✅ long_margin, short_margin, TRADEFEERATIO |
| INE    | `js{}.dat` JSON | ✅ long_margin, short_margin |
| CZCE   | `FutureDataClearParams.txt` | ✅ settle, 涨跌停板(%), 交易保证金率(%) |
| GFEX   | JSON API | (待分析字段) |
| DCE    | JSON API | (待分析字段) |

### 2.3 缺失字段处理

| 字段 | 来源 | 备注 |
|------|------|------|
| `maxup` / `maxdown` | 结算参数文件（涨跌停板限制 × 前结算价） | CZCE 直接给百分比，需自行计算 |
| `if_basis` | 未知 | 保留字段，初始设为 NULL |
| `minoq` / `maxoq` | 各交易所交易规则（静态配置） | 不随日期变动，配置一次即可 |

---

## 3. 工作方案

### Phase 0：基础设施（1-2天）

1. **安装依赖**：`pip install pip install pymysql sqlalchemy`
2. **创建数据库连接模块** `db.py`，支持 upsert 到 `t_futures_info`
3. **创建统一数据模型**（扩展 `models.py`），新增 `DailyBar(NamedTuple)` 表示日行情行
4. **补齐各交易所日行情查找/抓取**

### Phase 1：CFFEX 完整数据管道（2天）

**目标**：从 CFFEX CSV 解析 → 写入 MySQL

1. **解析器** `parsers/cffex.py`
   - 解析 CSV 中的合约代码、日期、OHLCV、settle、oi
   - 代码格式转换：`IF2605` → `IF2605.CFE`
   - 处理期权行权价合约（含 `-C-`、`-P-` 标记行，过滤或标记）
2. **历史数据回填**：从 2010-04-16 起逐日下载、解析、写入

### Phase 2：SHFE / INE 日行情数据（2-3天）

**目标**：找到并抓取 SHFE/INE 的日行情数据（非结算参数）

**可能的渠道**：
- SHFE 官网有日行情文件（类似 `js{}.dat` 但含行情字段）
- 或使用公开的行情数据页面
- 或通过交易所历史数据下载区

需先调研可用端点。

### Phase 3：CZCE 日行情数据（2-3天）

**目标**：获取 CZCE 的日行情（OHLCV）

**注意**：
- CZCE 的结算参数文件已含 settle 和 涨跌停板(%)
- 但仍缺 open/high/low/close/volume/amt
- CZCE 官网有独立的日行情页面，需调研

### Phase 4：DCE / GFEX 日行情数据（2-3天）

**目标**：获取 DCE 和 GFEX 的日行情数据

### Phase 5：统一解析 → 存储（2天）

1. 所有交易所统一接口：`def parse(content: bytes, trade_date: str) -> List[DailyBar]`
2. 数据校验器扩展：验证价格合理性（open/high/low/close 关系）
3. 数据库 upsert 逻辑：`INSERT ... ON DUPLICATE KEY UPDATE`
4. 日志和监控：记录每轮下载的行数、错误

### Phase 6：数据校验与切换（1-2天）

1. 与现有 Wind 数据交叉验证
2. 增量更新 cron 任务（每日收盘后自动运行）
3. 切换生产环境数据源

---

## 4. 架构设计

```text
data-sources/
├── src/data_sources/
│   ├── __init__.py
│   ├── models.py          # TaskConfig, Task, DailyBar ← 新增
│   ├── verifier.py        # Verifier
│   ├── reporter.py        # Reporter
│   ├── fetcher.py         # Fetcher (现有, 保留)
│   ├── db.py              # ← 新增: MySQL upsert
│   └── parsers/           # ← 新增: 各交易所解析器
│       ├── __init__.py
│       ├── cffex.py       # CFFEX CSV 解析
│       ├── shfe.py        # SHFE JSON 解析
│       ├── ine.py         # INE JSON 解析
│       ├── czce.py        # CZCE TXT 解析
│       ├── dce.py         # DCE JSON 解析
│       └── gfex.py        # GFEX JSON 解析
├── tests/
│   ├── test_fetcher.py
│   ├── test_verifier.py   # ← 新增: verifier 独立测试
│   ├── test_reporter.py   # ← 新增: reporter 独立测试
│   └── test_parsers/      # ← 新增: 解析器测试
│       ├── test_cffex.py
│       └── ...
└── data/
    └── samples/           # ← 新增: 各交易所样本文件用于测试
```

### 核心流程

```text
[调度] → [Fetcher] → [Parser] → [Verifier] → [DB Writer]
  ↓                                        ↓
 定时任务 (cron)                        日志/告警
```

---

## 5. 优先顺序

| 优先级 | 内容 | 理由 |
|--------|------|------|
| **P0** | CFFEX 完整管道 + 历史回填 | 已有数据，最快出成果 |
| **P1** | SHFE/INE 日行情调研与实现 | 数据量最大（70万+98万行） |
| **P2** | CZCE 日行情 | 52万行，有部分数据可用 |
| **P3** | DCE / GFEX | GFEX 较新（2022年起），DCE 量大 |
| **P4** | 交叉验证 + 生产切换 | 最后阶段 |

---

## 6. 已知风险

| 风险 | 影响 | 缓解 |
|------|------|------|
| 日行情数据源不可通过简单 HTTP 获取 | P1-P3 无法实施 | 改用浏览器自动化(Playwright)或 akShare |
| CFFEX CSV 开盘价为 null（期权） | 解析异常 | 区分期货/期权行，期权行跳过 |
| 历史数据需逐日下载 | 耗时长（15 年 × 250 天/年 × 6 所） | 并行下载 + 断点续传 |
| DB 已有 2M 行，upsert 冲突 | 重复数据 | 使用 `INSERT ... ON DUPLICATE KEY UPDATE` |
