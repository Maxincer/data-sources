# data-sources 生产部署方案

> 讨论时间：2026-05-21  
> 参与：新哲、Agent4  
> 目标：将 data-sources 项目部署到 db201，逐步替换 Wind 数据源写入 `future_cn.t_futures_info`

---

## 一、项目现状

### 1.1 项目结构

```
data-sources/
├── src/data_sources/
│   ├── fetcher.py       # 从 6 家交易所 API 拉取原始数据
│   ├── writer.py        # 解析原始数据并写入 MySQL
│   ├── parser.py        # 原始数据解析
│   ├── modifier.py      # 数据修正（maxup/maxdown 计算等）
│   ├── reporter.py      # 验证 + 邮件报告
│   ├── verifier.py      # 数据验证
│   ├── db.py            # 数据库连接 + 建表
│   └── configs.py       # 交易所 API URL 配置
├── data/
│   ├── raw/             # 原始行情文件（274MB，367 个文件）
│   └── trade_dates.txt  # 交易日历
├── scripts/
│   ├── pipeline.sh      # 三步流水线（生产/开发模式自适应）
│   └── deploy.sh        # 离线部署脚本（pipx + whl）
└── pyproject.toml       # 项目配置
```

### 1.2 数据流

```
6 家交易所 API (INE/SHFE/DCE/CZCE/CFFEX/GFEX)
    ↓ fetcher
  data/raw/（原始 .dat / .json / .txt / .csv / .html）
    ↓ writer (parser + modifier)
  future_cn.t_futures_info_exchange（MySQL）
    ↓ reporter (verifier + email)
  每日验证报告
```

### 1.3 自行计算的字段

| 交易所 | 字段 | 算法 |
|--------|------|------|
| INE / SHFE | maxup | floor(前结算 × (1 + 涨停幅度) / tick) × tick |
| INE / SHFE | maxdown | floor(前结算 × (1 - 跌停幅度) / tick) × tick |
| CZCE | maxup | ceil(前结算 × (1 + 涨停幅度) / tick) × tick |
| CZCE | maxdown | floor(前结算 × (1 - 跌停幅度) / tick) × tick |

依赖数据源：
- 前结算价 → `DailyMarketData`（交易所 API 每日拉取）
- 涨跌停幅度 → `TradingParameters`（交易所 API 每日拉取）
- tick（最小变动价位） → `ProductConfig`（fetcher 通过 Playwright 爬取）

其余字段（OHLC/volume/oi/settle）直接从交易所 API 读取，不自行计算。

---

## 二、目标环境

### 2.1 db201 服务器信息

| 项目 | 值 |
|------|-----|
| 主机名 | db201（原名 server201） |
| 内网 IP | `192.168.1.201` |
| 用户 | `maxinzhe` |
| 操作系统 | **Ubuntu 22.04.5 LTS** |
| 系统 Python | 3.10.12（无 Python 3.13） |
| CPU | 72 核 |
| 内存 | 251 GB（可用 ~115 GB） |
| 磁盘 | 467 GB（可用 ~105 GB） |
| SSH | 已从 server202（mini_apps）和 WenDaoInvPC-200 配置密钥 |

### 2.2 MySQL 集群（生产数据库）

| 项目 | 值 |
|------|-----|
| 接入点 | **192.168.1.27:3306**（独立 MySQL 集群） |
| 用户名 | `tools` |
| 密码 | `tools0512` |
| 读写权限 | 有 |
| 数据库 | `future_cn` |

**注意**：生产数据库走的是独立 MySQL 集群（192.168.1.27），不是 db201 本机的 MySQL。

### 2.3 任务调度

系统使用 **rutask**（Rust Job Manager）进行任务调度。

- Web 管理界面：`http://192.168.1.201:8877`
- 配置文件格式：**JSON5**（`sec min hour dom mon dow` 格式）
- 现有相关任务：

| 任务名 | 状态 | 最后运行 | 说明 |
|--------|------|---------|------|
| `update_if_exclude_wsi` | exited(0) | 16:19:59 | **Wind 期货写入（本项目的替换目标）** |
| `update_if` | exited(1) | 19:00:59 | Wind 全量期货更新（今天报错） |
| `update_option_exclude_wsi` | exited(0) | 16:00:59 | Wind 期权写入 |
| `wind_misc_run` | exited(0) | 22:33 | Wind 杂项 |

rutask 配置示例：
```json5
data_sources_exchange: {
  start_cron: "00 30 16 * * 1-5",  // 工作日 16:30:00
  cmd: "bash scripts/pipeline.sh",
  cwd: "/home/xungeng/data-sources",
  output: "/home/data_ops/log/data_sources-$datetime.log",
  safe_start: true,
  life_time: 7200
}
```

---

## 三、Wind 写入分析

### 3.1 Wind 脚本结构

`update_if_exclude_wsi.sh`（位于 `git.corp.wendao.fund:dev/data-crawler`）：

```bash
#!/bin/bash
cd ${self_dir}/../future_cn/src
run_pyrun -e ~/bin/reporterr.py -o ~/log/%n-%D-%T.log -3 t_futures.py
run_pyrun -e ~/bin/reporterr.py -o ~/log/%n-%D-%T.log -3 t_futures_info.py
run_pyrun -e ~/bin/reporterr.py -o ~/log/%n-%D-%T.log -3 t_main_contracts.py
run_pyrun -e ~/bin/reporterr.py -o ~/log/%n-%D-%T.log -3 t_futures_tradable.py
```

每个脚本通过 `pyrun`（data-crawler 自带工具）调用，`reporterr.py` 负责执行、日志和失败重试（3 分钟后重试一次）。

### 3.2 写入工具 DataFrameSaver 的三种写入方式

`DataFrameSaver` 是 data-crawler 的 MySQL 写入工具，基于 pandas + pymysql，提供三种写入方法：

#### 方式一：`INSERT IGNORE INTO`（默认 build_query）

```sql
INSERT IGNORE INTO t_futures_info (`code`,`date`,`open`,...) VALUES (...)
```

| 场景 | 行为 |
|------|------|
| 数据A有、数据B无 | ✅ **正常插入**（新行，不冲突） |
| 数据A无、数据B有 | ✅ **不受影响**（数据A不写这些行） |
| 两边都有（同 code+date） | ❌ **后写的数据被静默丢弃**（已有行，IGNORE 跳过） |

#### 方式二：`INSERT ... ON DUPLICATE KEY UPDATE`（build_upgrade_query）

```sql
INSERT INTO t_futures (...) VALUES (...) AS new 
ON DUPLICATE KEY UPDATE open=new.open, high=new.high, ...
```

| 场景 | 行为 |
|------|------|
| 数据A有、数据B无 | ✅ 正常插入 |
| 数据A无、数据B有 | ✅ 不受影响 |
| 两边都有 | ✅ **后写的值覆盖**（UPDATE 所有非索引字段） |

#### 方式三：`UPDATE ... WHERE ...`（build_update_only_query）

```sql
UPDATE t_futures SET open=%s, ... WHERE (code=%s AND date=%s)
```

纯更新，不插入新行。本仓库中未使用。

### 3.3 各表使用的写入方式汇总

| 脚本 | 写入方式 | 目标表 | 原因 |
|------|---------|--------|------|
| **`t_futures_info.py`** | **`INSERT IGNORE`** | `t_futures_info` | 每日新日期，不重复，首次跑即永久保留 |
| `t_futures.py` | `ON DUPLICATE KEY UPDATE` | `t_futures` | 合约元数据可能变更（最后交易日等），需要更新 |
| `t_main_contracts.py` | 直接 SQL | `t_main_contracts` | 成交量驱动的主力合约判定逻辑 |
| `t_futures_tradable.py` | 直接 SQL | `t_futures_tradable` | 可交易合约判定 |

### 3.4 `t_futures_info.py` 核心写入逻辑

#### 数据获取

```python
# 逐合约调用 Wind WSD API（日频时间序列）
result = win2log.query_wsd(
    code,                                    # 如: 'CU2606.SHF'
    ['open','high','low','close','volume',   # 15 个字段
     'amt','oi','settle','maxup','maxdown',
     'if_basis','long_margin','short_margin',
     'minoq','maxoq'],
    start_date,                              # max(IPO日期, 该合约已有最新日期+1)
    self.today,                              # 当天
    options="futinstrtype=1",               # Wind 期货品种标识
    times_col='date'                         # 时间轴列名
)
result['code'] = df.iloc[i].code            # 硬编码为原始代码
all_df = pd.concat([all_df, result])         # 逐合约拼接
```

#### 关键特征

| 特征 | 说明 |
|------|------|
| 数据来源 | **Wind 原生数据**，通过 WSD API 直接获取 |
| 是否合并其他表 | **否**，纯 Wind 数据，不 JOIN 任何自建表 |
| 是否计算字段 | **否**，maxup/maxdown 是 Wind 直接给的，非自行计算 |
| 代码特殊处理 | 仅 CZCE：4 位精简为 3 位后调 API，*写回时恢复 4 位原代码* |
| 写入方式 | **`INSERT IGNORE INTO`**，非 `ON DUPLICATE KEY UPDATE` |
| 写入范围 | 每个合约从「已有最新日期+1」到「今天」，不重复拉取历史 |

### 3.5 写入方式对迁移方案的影响

**核心发现**：Wind 对 `t_futures_info` 用的是 **`INSERT IGNORE`**，不是 `ON DUPLICATE KEY UPDATE`。

这意味着：

| 场景 | 实际行为 | 影响 |
|------|---------|------|
| 阶段一（两表独立） | 互不干扰 | ✅ 无影响 |
| 阶段二同时写同一表 | **谁先写谁的数据保留**，后写的 IGNORE 跳过 | ❌ 必须只留一个写入源 |
| 回滚（想用 Wind 恢复） | Wind 的 INSERT IGNORE **被 data-sources 已有行挡住** | ❌ 回滚前必须先手动 DELETE |

### 3.6 回滚方案（修正后）

```sql
-- 必须先手动清空当日 data-sources 写入的数据
DELETE FROM t_futures_info WHERE date = '${TODAY}';
-- 然后再跑 Wind
bash update_if_exclude_wsi.sh
```

**如果不清空就直接跑 Wind**：Wind 的 INSERT IGNORE 遇到已存在的 `(code, date)` 行 → 静默跳过 → data-sources 的数据残留在表里 → 回滚失败。

### 3.7 Wind 不依赖 float 类型

`DataFrameSaver` 生成标准 `INSERT ... ON DUPLICATE KEY UPDATE` SQL，字段类型由表定义决定。pandas + pymysql 自动做类型转换。表字段改为 `decimal(20,2)` 完全兼容。

float → decimal(20,2) 示例：
```
原始 float: 1234.5678       → decimal: 1234.57（四舍五入）
价格场景:   12.3400000000001 → decimal: 12.34（消除浮点尾数）
```
**行情数据不会丢失精度**，反而会消除浮点数尾数问题。

---

## 四、Wind API 交叉验证

### 5.1 WSD API 调用详情

`t_futures_info.py` 中的数据获取全流程：

```
Wind WSD API
  ↓ w.wsd(code, fields, start, end, "futinstrtype=1")
原始 WindData 对象
  ↓ win2log._query_wind() → DataFrame
pandas DataFrame（15 列字段）
  ↓ pd.concat() 逐合约拼接
all_df（全市场合约数据）
  ↓ DataFrameSaver.save() → INSERT IGNORE
t_futures_info 表
```

| 环节 | 详情 |
|------|------|
| API 类型 | Wind WSD（日频时间序列） |
| 调用方式 | 本地 wind Python 库 或 RPC Wind（192.168.2.9:3801） |
| 逐合约调用 | 每个期货合约一次 WSD 调用（数千次调用/天） |
| 字段范围 | 15 个行情字段，全部来自 Wind 原始输出 |
| 数据处理 | **零处理**：不 merge、不 transform、不计算派生字段 |
| 代码还原 | CZCE 调 API 时用 3 位精简代码，写表时恢复 4 位 |

### 5.2 第二阶段交叉验证方案

部署到 db201 后，data-sources 自身调用 Wind API 拉取同合约同日数据作为交叉验证基准：

```
同一套 WSD API → 拉取相同的 15 个字段 →
  vs data-sources 的 t_futures_info 数据
  → 找出差异
```

这样不需要保留 Wind 的对照表，验证完即可。

---

## 五、两阶段部署策略

### 5.1 阶段一：写入过渡表，并行验证

```
db201 上：
  rutask 调度 → pipeline.sh
    ├── fetcher  → 拉取交易所数据到 data/raw/
    ├── writer   → 写入 t_futures_info_exchange
    └── reporter → 验证报告（每日邮件）

Wind 继续写入 t_futures_info（生产表，不受影响）
```

**写入目标**：`future_cn.t_futures_info_exchange`

**验证内容**（每天两份报告）：
1. `t_futures_info_exchange`（我们） vs `t_futures_info`（Wind） → 两表差异
2. `t_futures_info_exchange` vs Wind API（WSD）直接拉取的数据 → 交叉验证

**DB 连接**：
- 写目标：192.168.1.27:3306（MySQL 集群）
- 用户名/密码：tools / tools0512

### 5.2 阶段二：切换生产

```
db201 上：
  rutask 调度 → pipeline.sh
    ├── fetcher  → 拉取数据
    ├── writer   → 写入 t_futures_info（生产表！）
    └── reporter → 仅一份验证报告

  update_if_exclude_wsi：注释掉所有行（或直接禁用）
```

**写入目标**：`future_cn.t_futures_info`

**验证内容**（每天一份报告）：
1. `t_futures_info`（我们） vs Wind API（WSD）直接拉取的数据 → 交叉验证
2. 跳过两表比对（通过 `--skip-table-compare` 参数控制）

**字段类型**：阶段二切换时，将 `t_futures_info` 的字段从 float 改为 `decimal(20,2)`。

### 5.3 回滚方案

**前提**：Wind 使用 `INSERT IGNORE`，不会覆盖已有行。因此回滚前**必须先清空当日数据**。

```
1. 停止 data-sources 任务（rutask 禁用）
2. ⚠️ 清空当日 data-sources 写入的数据：
   DELETE FROM t_futures_info WHERE date = '${TODAY}';
   （关键步骤——不做这一步，Wind 的 INSERT IGNORE 会跳过所有已有行）
3. 重新启用 Wind 的 update_if_exclude_wsi 任务
4. Wind 重跑 → INSERT IGNORE → 当日行不存在 → 正常插入 → 数据恢复
```

**注意**：阶段一中不需要回滚——data-sources 只写 exchange 表，不影响生产表。

---

## 六、代码改动清单

### 5.1 writer：数据库/表名参数化

`db.py` 中当前硬编码：
```python
DB_CONFIG = {"host": "192.168.1.202", ...}
TABLE_NAME = "t_futures_info_exchange"
```

需要改为通过 `--host`、`--db`、`--table` 参数传入：

```bash
# 阶段一
python3 -m data_sources.writer --host 192.168.1.27 --table t_futures_info_exchange --date ${DATE}

# 阶段二
python3 -m data_sources.writer --host 192.168.1.27 --table t_futures_info --date ${DATE}
```

不需要 `--port` 参数（MySQL 默认 3306）。

### 5.2 reporter：阶段控制

新增 `--skip-table-compare` 参数：

| 阶段 | 参数 | 验证内容 |
|------|------|---------|
| 阶段一 | 不传 | exchange vs info（两表） + exchange vs Wind API |
| 阶段二 | `--skip-table-compare` | info vs Wind API（交叉验证） |

### 5.3 pipeline.sh

当前已支持生产/开发模式切换，基本不需要改。需要确认：
- 传参：`RUN_WRITER="${PIP} -m data_sources.writer --date ${TRADE_DATE} --table ${TABLE_NAME}"`
- `TABLE_NAME` 可通过环境变量或 rutask 的 `env` 配置传入

### 5.4 现有 reporter/verifier

pipeline.sh 的 Step 3 已有 reporter 做验证 + 邮件发送。当前邮件配置：

```bash
SENDER="robot@wendao.fund"
RECIPIENTS="fisher@wendao.fund,chendingzhong@wendao.fund,mxz@wendao.fund"
```

---

## 七、离线部署包准备

db201 无外网访问，需离线安装所有依赖。

### 7.1 需要打包的内容

```
offline-deploy.tar.gz
├── python3.13/            # Python 3.13 + venv（从 deadsnakes PPA 下载 .deb）
│   ├── python3.13.deb
│   ├── python3.13-venv.deb
│   └── libpython3.13*.deb
│
├── playwright-deps/       # Playwright 浏览器 + 系统库
│   ├── chromium-1208/     # 浏览器主程序
│   ├── chromium_headless_shell-1208/  # headless shell
│   ├── libnspr4.deb       # Chromium 系统依赖
│   ├── libnss3.deb
│   ├── libnssutil3.deb
│   └── libasound2.deb
│
└── install.sh             # 一键安装脚本
```

### 7.2 打包流程

在 WSL（有网络）上执行：

```bash
# 1. Python 3.13（从 deadsnakes PPA）
sudo add-apt-repository ppa:deadsnakes/ppa -y
apt download python3.13 python3.13-venv libpython3.13-stdlib

# 2. Playwright 浏览器（已在本地）
cp -r ~/.cache/ms-playwright/chromium-1208/            offline/playwright-deps/
cp -r ~/.cache/ms-playwright/chromium_headless_shell-1208/ offline/playwright-deps/

# 3. 系统依赖
apt download libnspr4 libnss3 libnssutil3 libasound2

# 4. 打包
tar czf offline-deploy.tar.gz offline/
```

### 7.3 db201 安装步骤

```bash
tar xzf offline-deploy.tar.gz
cd offline
bash install.sh  # 顺序安装 Python → 系统库 → 浏览器二进制到 ~/.cache
```

### 7.4 兼容性确认

- **目标 OS**：Ubuntu 22.04.5 LTS ✅
- **Playwright 1.58**：官方支持 Ubuntu 22.04 ✅
- **Python 3.13**：deadsnakes PPA 支持 Ubuntu 22.04 ✅
- 不会出现 WSL Ubuntu 26.04 的缺库问题

---

## 八、数据文件迁移

data-sources 运行时依赖 `data/raw/` 和 `data/trade_dates.txt`，部署时必须全量拷贝到 db201。

```bash
# 在 WSL 上打包
tar czf data-sources-data.tar.gz -C /mnt/e/projects/data-sources data/

# 通过 server202 跳板传到 db201
scp data-sources-data.tar.gz mini_apps@192.168.1.202:/tmp/
ssh mini_apps@192.168.1.202 "scp /tmp/data-sources-data.tar.gz maxinzhe@192.168.1.201:~/"

# 在 db201 上解压
tar xzf ~/data-sources-data.tar.gz -C ~/data-sources/
```

**数据规模**：274MB，367 个文件。

---

## 九、部署流程总览

### 阶段一部署

```
1. [WSL] 准备离线包（Python 3.13 + Playwright + dependencies）
2. [WSL] 打包 data/ 目录
3. [WSL] 构建 .whl 文件
4. [WSL] scp 离线包 + data.tar.gz + .whl + deploy.sh + pipeline.sh → db201
5. [db201] 安装离线包（Python 3.13 + Playwright + 系统库）
6. [db201] 解压 data/ 到项目目录
7. [db201] 执行 deploy.sh 安装 data-sources
8. [db201] 配置 rutask JSON5，添加一个新的 pipeline 任务
9. [rutask] 手动触发一次测试运行，检查日志
10. [验证] 确认 t_futures_info_exchange 有数据、验证报告正常
```

### 阶段二切换

```
1. [rutask] 禁用 update_if_exclude_wsi 任务
2. [writer] --table 改为 t_futures_info
3. [reporter] 加上 --skip-table-compare
4. [表字段] float → decimal(20,2)
5. [rutask] 继续运行 data-sources pipeline
6. [验证] 对比 info（我们）vs Wind API 数据
```

### 回滚

```
1. 停 data-sources
2. ⚠️ DELETE FROM t_futures_info WHERE date = TODAY（必须先清空！Wind 用 INSERT IGNORE 不会覆盖已有行）
3. 重跑 update_if_exclude_wsi（此时表里无当日行，正常插入）
```

---

## 十、待办清单

| # | 事项 | 负责人 | 状态 |
|---|------|--------|------|
| 1 | Python 3.13 离线包准备 | Agent4 | ⏳ |
| 2 | Playwright + Chromium 离线包准备 | Agent4 | ⏳ |
| 3 | writer 参数化（--host --table --db） | Agent4 | ⏳ |
| 4 | reporter 加 --skip-table-compare | Agent4 | ⏳ |
| 5 | db201 rutask 配置添加 | 新哲 | ⏳ |
| 6 | 数据文件 scp 到 db201 | 新哲 | ⏳ |
| 7 | 离线包 + whl + deploy.sh 拷贝到 db201 | 新哲 | ⏳ |
| 8 | db201 上执行安装 + 测试运行 | 新哲 | ⏳ |

---

## 十一、相关文档

- `AgentMemories/db201-ops.md` — db201 操作手册（含系统信息）
- `AgentMemories/server202-ops.md` — server202 跳板机操作手册
- `AgentMemories/cq-test05-ops.md` — test05 + stock-event-guardian
- `AgentMemories/stock-event-guardian-ocrflux-architecture.md` — OCRFlux 架构方案
