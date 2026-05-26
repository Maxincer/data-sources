# data-sources 生产部署方案

> 版本: v2 | 更新: 2026-05-25  
> 参与: 新哲、Main  
> 目标: 将 data-sources 逐步替换 Wind 数据源写入 `future_cn.t_futures_info`

---

## 一、方案变更记录

### 2026-05-25: 部署环境使用系统 Python 3.10

**原方案 (v1)**: 离线打包 Python 3.13（deadsnakes PPA .deb）  
**调整后 (v2)**: 使用 db201 系统 Python 3.10

**原因**: cpp_py（Wind RPC 必需的 C 扩展）已编译为 Python 3.10 ABI，位于 `/usr/lib/python3.10/cpp_py.so`。为 Python 3.13 重新编译需在 db201 上部署 vcpkg + 10+ C++ 依赖：

```
cpp_py 源码: git.corp.wendao.fund/lixungeng/cpp_py
├── 构建: py-cxx-builder + setuptools + GCC
├── vcpkg 依赖: boost-regex, boost-container, openssl, zlib, zstd,
│                libmysql, libpq, mongo-c-driver, sqlite3, utf8proc, jansson
├── utillib (同仓库 C++ 静态库)
└── 产出: python3 setup.py build → cpp_py.so
```

DB 连接/文件下载等 IO 密集型任务协程收益有限，暂不为此投入。`pyproject.toml` 声明 `requires-python = ">=3.10"`，完全兼容。

**继承方案**: `pip install --user` 安装到 `~/.local/`，系统 Python 的 cpp_py 在 sys.path 中自然可见。

**后续迁移**: 当 cpp_py 提供 Python 3.13 wheel 后，`pip install --user` 即可切换，代码无需改动。

### 离线包简化

| 维度 | v1 | v2 |
|------|----|----|
| Python | 3.13 .deb 离线打包 | **不需要**（系统 3.10） |
| Playwright Chromium | 离线打包 | 离线打包 |
| 系统库 .deb | 离线打包 | 离线打包 |

---

## 二、项目现状

### 2.1 项目结构

```
data-sources/
├── src/data_sources/
│   ├── fetcher.py       # 从 6 家交易所 API 拉取原始数据
│   ├── writer.py        # 解析并写入 MySQL（支持 --host/--db/--table）
│   ├── parser.py        # 原始数据解析
│   ├── modifier.py      # 数据修正（maxup/maxdown 计算等）
│   ├── reporter.py      # 验证 + 邮件（支持 --skip-table-compare）
│   ├── verifier.py      # 数据验证 + Wind API 交叉验证
│   ├── wind_client.py   # Wind RPC 客户端（通过 cpp_py）
│   ├── db.py            # 数据库连接（支持 config_override 参数化）
│   └── configs.py       # 交易所 API URL 配置
├── data/
│   ├── raw/             # 原始行情文件（274MB，367 个文件）
│   └── trade_dates.txt  # 交易日历
├── scripts/
│   ├── pipeline.sh      # 三步流水线（支持 WRITER_HOST/DB/TABLE 环境变量）
│   └── deploy.sh        # pip install --user 安装脚本
├── docs/
│   └── deployment-plan.md   # 本文件
└── pyproject.toml       # requires-python = ">=3.10"
```

### 2.2 代码改动汇总（已完成 ✅）

| 改动 | 文件 | 说明 |
|------|------|------|
| DB 参数化 | `db.py` | 新增 `config_override`，所有函数支持 host/port/database/table |
| Writer CLI | `writer.py` | 新增 `--host` `--port` `--db` `--table` |
| Pipeline 环境变量 | `pipeline.sh` | 支持 `WRITER_HOST` / `WRITER_DB` / `WRITER_TABLE` |
| Wind 交叉验证 | `wind_client.py` | 通过 cpp_py RPC 调 Wind WSD API |
| 阶段控制 | `reporter.py` | `--skip-table-compare` 跳过两表对比 |
| 验证器 | `verifier.py` | `compare_with_wind_api()` |

### 2.3 数据流

```
6 家交易所 API (INE/SHFE/DCE/CZCE/CFFEX/GFEX)
    ↓ fetcher (Playwright + requests)
  data/raw/（原始行情文件）
    ↓ writer (parser + modifier)
  future_cn.t_futures_info_exchange（阶段一）/ t_futures_info（阶段二）
    ↓ reporter (verifier + Wind API + email)
  每日验证报告
```

---

## 三、目标环境

### 3.1 db201 服务器

| 项目 | 值 |
|------|-----|
| IP | `192.168.1.201` |
| 用户 | `data_ops` |
| OS | Ubuntu 22.04.5 LTS |
| 系统 Python | 3.10.12 |
| cpp_py | `/usr/lib/python3.10/cpp_py.so`（已部署 ✅） |
| 安装方式 | `pip install --user` → `~/.local/` |
| cpp_py | 系统 Python 3.10 site-packages，pip --user 自然可见 |
| CPU / 内存 | 72 核 / 251 GB |

### 3.2 MySQL 集群（生产数据库）

| 项目 | 值 |
|------|-----|
| 接入点 | `192.168.1.27:3306` |
| 用户 / 密码 | `tools` / `tools0512` |
| 数据库 | `future_cn` |

### 3.3 任务调度

rutask（Rust Job Manager），Web: `http://192.168.1.201:8877`，JSON5 配置。

---

## 四、Wind 写入分析

### 4.1 写入方式

Wind 对 `t_futures_info` 使用 `INSERT IGNORE INTO`（DataFrameSaver 默认 build_query）。

| 场景 | 行为 |
|------|------|
| 新合约/新日期 | ✅ 正常插入 |
| 已有 code+date | ❌ 静默跳过（IGNORE） |

### 4.2 对迁移的影响

| 场景 | 影响 |
|------|------|
| 阶段一（两表独立） | ✅ 互不干扰 |
| 阶段二同时写 t_futures_info | ❌ 谁先写谁的数据保留，后写的被跳过 |
| 回滚（恢复 Wind） | ❌ 必须先 DELETE 当日 data-sources 行 |

### 4.3 回滚方案

```sql
-- 必须先清空！否则 Wind INSERT IGNORE 跳过已有行
DELETE FROM t_futures_info WHERE date = '${TODAY}';
-- 然后重跑
bash update_if_exclude_wsi.sh
```

---

## 五、Wind API 交叉验证

部署到 db201 后，data-sources 通过 cpp_py RPC（192.168.2.9:3801）调用 Wind WSD API 作为交叉验证基准。

```
Wind WSD API → 拉取同合约同日 15 个字段
  vs data-sources 写入的数据 → 找出差异
```

不需要保留 Wind 对照表，验证完即可。

---

## 六、两阶段部署策略

### 6.1 阶段一: 写入过渡表，并行验证

```
db201:
  rutask → pipeline.sh (16:30)
    ├── fetcher  → data/raw/
    ├── writer   → t_futures_info_exchange (192.168.1.27)
    └── reporter → 两表对比 + Wind API 交叉验证 + 邮件

Wind 继续写 t_futures_info（不受影响）
```

**Writer 参数**: `--host 192.168.1.27 --table t_futures_info_exchange`

### 6.2 阶段二: 切换生产

```
1. rutask 禁用 update_if_exclude_wsi
2. writer --table 改为 t_futures_info
3. reporter 加 --skip-table-compare（跳过两表对比，仅 Wind API 交叉验证）
4. 表字段 float → decimal(20,2)
```

---

## 七、离线部署包

### 7.1 需要打包

| # | 文件 | 来源 | db201 目标路径 |
|---|------|------|---------------|
| 1 | Playwright Chromium | WSL `~/.cache/ms-playwright/chromium-1208/` | `~/.cache/ms-playwright/chromium-1208/` |
| 2 | Headless Shell | WSL `~/.cache/ms-playwright/chromium_headless_shell-1208/` | `~/.cache/ms-playwright/chromium_headless_shell-1208/` |
| 3 | 系统库 .deb | `apt download libnspr4 libnss3 libasound2t64` | `dpkg -i` 安装 |
| 4 | data/ 目录 | 项目已有 (274MB) | `~/data-sources/data/` |
| 5 | data-sources .whl | `pip wheel` 构建 | `pip install --user` |
| 6 | scripts/ | pipeline.sh + deploy.sh | `~/data-sources/scripts/` |

**不需要 Python .deb** — db201 已有 Python 3.10 + cpp_py，`pip install --user` 自然可见系统 cpp_py。

### 7.2 打包命令 (WSL)

```bash
cd /mnt/e/projects/data-sources

# Playwright 浏览器（本地已有）
mkdir -p offline/playwright
cp -r ~/.cache/ms-playwright/chromium-1208/ offline/playwright/
cp -r ~/.cache/ms-playwright/chromium_headless_shell-1208/ offline/playwright/

# 系统 .deb
mkdir -p offline/sys-libs && cd offline/sys-libs
apt download libnspr4 libnss3 libasound2t64
cd ../..

# 构建 whl
pip wheel --no-deps -w dist/ .

# 打包
tar czf /tmp/data-sources-offline.tar.gz offline/ data/ scripts/
```

### 7.3 传输到 db201（通过 server202 跳板）

```bash
scp /tmp/data-sources-offline.tar.gz mini_apps@192.168.1.202:/tmp/
scp dist/data_sources-*.whl mini_apps@192.168.1.202:/tmp/
ssh mini_apps@192.168.1.202 \
  "scp /tmp/data-sources-offline.tar.gz /tmp/data_sources-*.whl data_ops@192.168.1.201:~/"
```

### 7.4 db201 上安装

```bash
ssh -J mini_apps@192.168.1.202 data_ops@192.168.1.201
cd ~ && tar xzf data-sources-offline.tar.gz

# 系统库
sudo dpkg -i offline/sys-libs/*.deb 2>/dev/null || true

# Playwright
mkdir -p ~/.cache/ms-playwright
cp -r offline/playwright/chromium-1208 ~/.cache/ms-playwright/
cp -r offline/playwright/chromium_headless_shell-1208 ~/.cache/ms-playwright/

# 安装 data-sources
pip install --user --force-reinstall data_sources-*.whl

# 部署项目文件
mkdir -p ~/data-sources && mv data scripts ~/data-sources/
```

---

## 八、数据文件迁移

```bash
# 已在 7.2 打包中一起处理
# data/ 目录 (274MB, 367 文件) 随 offline.tar.gz 一起传输
```

---

## 九、rutask 配置

### 阶段一

```json5
data_sources_exchange: {
  start_cron: "00 30 16 * * 1-5",
  cmd: "bash scripts/pipeline.sh",
  cwd: "/home/data_ops/data-sources",
  env: {
    WRITER_HOST: "192.168.1.27",
    WRITER_TABLE: "t_futures_info_exchange",
    SMTP_PASSWORD: "***",
  },
  output: "/home/data_ops/data-sources/logs/pipeline-$datetime.log",
  safe_start: true,
  life_time: 7200,
}
```

### 阶段二

```json5
data_sources: {
  start_cron: "00 30 16 * * 1-5",
  cmd: "bash scripts/pipeline.sh",
  cwd: "/home/data_ops/data-sources",
  env: {
    WRITER_HOST: "192.168.1.27",
    WRITER_TABLE: "t_futures_info",
    SMTP_PASSWORD: "***",
  },
  output: "/home/data_ops/data-sources/logs/pipeline-$datetime.log",
  safe_start: true,
  life_time: 7200,
}
```

pipeline.sh 通过 `SKIP_TABLE_COMPARE=1` 环境变量控制，rutask 的 env 中设置即可。

---

## 十、待办清单

| # | 事项 | 负责人 | 状态 |
|---|------|--------|------|
| 1 | writer/db/reporter/pipeline 参数化 | Main | ✅ |
| 2 | wind_client.py (Wind RPC) | Main | ✅ |
| 3 | Playwright Chromium + sys-libs 离线打包 | 新哲 | ⏳ |
| 4 | data/ + scripts/ + whl 打包传输 | 新哲 | ⏳ |
| 5 | db201 上安装 + 测试运行 | 新哲 | ⏳ |
| 6 | rutask JSON5 配置 | 新哲 | ⏳ |
| 7 | cpp_py Python 3.13 预编译 | 后续 | 📅 |

---

## 十一、相关文档

- `AgentMemories/db201-ops.md` — db201 操作手册
- `AgentMemories/server202-ops.md` — server202 跳板机操作手册
- `git.corp.wendao.fund/lixungeng/cpp_py` — cpp_py 源码及构建

---

## 十二、已知问题

### 12.1 DCE 到期合约缺少涨跌停价

每月最后交易日，交易所会在当日交易参数文件中摘除到期合约。导致:
- DailyMarketData 仍有行情数据（close/volume/amt/oi）
- TradingParameters 已无该合约记录 → maxup/maxdown 为 NULL

影响范围: 到期月份的合约（如 5 月 26 日为 2605 合约最后交易日），通常 5-10 个合约。
这些合约在 `t_futures_info`（Wind 源）中也有数据，因此两表对比时会出现差异。

**临时验证**: 昨天（5/25）的 TradingParameters 中包含今日缺失的 7 个合约（eg2605/bz2605/jd2605/lh2605/eb2605/lg2605/pg2605），确认是到期摘牌导致。
