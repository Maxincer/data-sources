# data-sources 生产部署方案

> 版本: v3 | 更新: 2026-05-29  
> 参与: 新哲、Agent4  
> 目标: 将 data-sources 逐步替换 Wind 数据源写入 `future_cn.t_futures_info`

---

## 一、方案变更记录

### 2026-05-29: 打包策略 + 脚本可移植性 + Python 3.10 兼容

**打包策略重构**：
- `data-sources-pipeline` 通过 `[tool.setuptools.script-files]` 打入 whl，pip 安装到 `~/.local/bin/`
- 部署包不再含 `src/`（已在 whl 中）、`pyproject.toml`（仅构建用）、`scripts/`（已在 whl 中）
- 仅打 `data/` + `deploy/offline/`，解压到 `/tmp/` 消费，不留 `~/data-sources/` 项目文件夹
- `sys-libs/` 并入 `chromium-deps/`（重复冗余）

**脚本可移植性**：
- `#!/usr/bin/env bash` — 适配 db201 `/usr/bin/bash` 和 WSL `/bin/bash`
- `cd` 移入 `DEV_MODE` 块 — prod 绝对路径无需切换 cwd
- `export VAR="val"` 一行风格统一
- rutask 统一 `cwd: "/home/data_ops"` + `cmd: "data-sources-pipeline"`

**Python 3.10 兼容修复**：
- `verifier.py`: f-string `{}` 内 `\|` 提取为变量（3.12 前不支持）
- `wind_client.py` / `fetcher.py`: 模块级 `os.environ[]` 改为函数内懒加载 — 保持硬失败（KeyError），`--help` 不触发

**依赖声明**：`pyproject.toml` 补上 `mxz-utils~=0.1.0`

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
│   ├── wind_client.py   # Wind RPC 客户端（cpp_py）
│   ├── db.py            # 数据库连接（支持 config_override 参数化）
│   └── configs.py       # 交易所 API URL 配置
├── data/
│   ├── raw/             # 原始行情文件
│   └── trade_dates.txt  # 交易日历（8797 行）
├── scripts/
│   └── data-sources-pipeline   # 流水线入口（打入 whl，pip 安装到 ~/.local/bin/）
├── deploy/offline/
│   ├── chromium-deps/   # 77 .deb（Chromium 系统依赖，含 nss3/nspr4/asound2）
│   ├── pip-deps/        # 16 .whl（data_sources + mxz_utils + 传递依赖）
│   └── playwright/      # chromium-1208 + headless_shell-1208 浏览器
├── docs/
│   ├── deployment-guide.md   # 部署操作手册
│   └── deployment-plan.md    # 本文件
└── pyproject.toml       # 构建配置 + 依赖声明
```
```

### 2.2 代码改动汇总（已完成 ✅）

| 改动 | 文件 | 说明 |
|------|------|------|
| DB 参数化 | `db.py` | 新增 `config_override`，所有函数支持 host/port/database/table |
| Writer CLI | `writer.py` | 新增 `--host` `--port` `--db` `--table` |
| Pipeline 环境变量 | `data-sources-pipeline` | 支持 `WRITER_HOST` / `WRITER_DB` / `WRITER_TABLE` |
| Wind 交叉验证 | `wind_client.py` | 通过 cpp_py RPC 调 Wind WSD API |
| 阶段控制 | `reporter.py` | `--skip-table-compare` 跳过两表对比 |
| 验证器 | `verifier.py` | `compare_with_wind_api()` |
| 依赖声明 | `pyproject.toml` | 补入 `mxz-utils~=0.1.0`；`[tool.setuptools.script-files]` 声明入口脚本 |
| 脚本重命名 | `data-sources-pipeline` | `pipeline.sh` → `data-sources-pipeline`，避免同名冲突 |
| Python 3.10 兼容 | `verifier.py` | f-string 内 `\|` 提取为变量（3.12 前不支持） |
| 懒加载 env | `wind_client.py` / `fetcher.py` | 模块级 `os.environ[]` 移入函数，`--help` 不触发 KeyError |

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

rutask（Rust Job Manager），配置文件：`/home/data_ops/rustask/rustask.local.json5`

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
  rutask → data-sources-pipeline (16:30)
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

### 7.1 打包内容

部署包仅含两个入口路径，其余通过 whl 分发：

| # | 内容 | 目标 | 何时安装 |
|---|------|------|---------|
| 1 | `data/` | `/data2_backup/data_sources_data/` | 首次部署 + 日历更新 |
| 2 | `deploy/offline/chromium-deps/` (77 .deb) | `dpkg -i` 系统安装 | 首次部署 |
| 3 | `deploy/offline/pip-deps/` (16 .whl) | `pip3 install --user --no-index` | 每次更新 |
| 4 | `deploy/offline/playwright/` | `/home/data_ops/playwright-browsers/` | 首次部署 |

**不在包中**：
- `src/` — 已通过 whl 安装到 `~/.local/lib/python3.10/site-packages/`
- `scripts/` — 已通过 whl 安装到 `~/.local/bin/`
- `pyproject.toml` — 仅构建时使用

### 7.2 打包命令 (WSL)

```bash
cd /mnt/e/projects/data-sources

# 1. 构建 whl
python3 -m build --wheel
cp dist/data_sources-0.1.0-py3-none-any.whl deploy/offline/pip-deps/

# 2. 打包部署包（白名单，仅 data + offline deps）
tar czf "/tmp/$(date +%Y%m%d)-$(git rev-parse --short HEAD).tar.gz" \
  data/ \
  deploy/offline/

# 3. 上传
scp /tmp/*.tar.gz mini_apps@192.168.1.202:/data3/releases/data-sources/
```

### 7.3 传输到 db201

```bash
# server202 → db201（跳板）
scp /data3/releases/data-sources/${RELEASE}.tar.gz \
  data_ops@192.168.1.201:/tmp/
```

### 7.4 db201 上安装

```bash
ssh data_ops@192.168.1.201

# 解压到 /tmp（用完即弃）
mkdir -p /tmp/data-sources
cd /tmp/data-sources
tar -xzf /tmp/${RELEASE}.tar.gz

# 首次部署：数据目录 + 系统依赖 + Playwright
mkdir -p /data2_backup/data_sources_data
cp -rn /tmp/data-sources/data/* /data2_backup/data_sources_data/
sudo apt install -f -y
sudo apt install -y /tmp/data-sources/deploy/offline/chromium-deps/*.deb
mkdir -p /home/data_ops/playwright-browsers
cp -rn /tmp/data-sources/deploy/offline/playwright/chromium-1208 /home/data_ops/playwright-browsers/
cp -rn /tmp/data-sources/deploy/offline/playwright/chromium_headless_shell-1208 /home/data_ops/playwright-browsers/

# 每次更新：Python 依赖
pip3 install --user --no-index --find-links=/tmp/data-sources/deploy/offline/pip-deps data_sources mxz_utils pymysql playwright fire pyee greenlet typing_extensions termcolor requests beautifulsoup4 soupsieve certifi charset_normalizer idna urllib3

# 验证
python3 -c "import data_sources, pymysql, playwright" && echo OK

# 清理
rm -rf /tmp/data-sources /tmp/${RELEASE}.tar.gz
```

---

## 八、数据文件

`data/` 随部署包传输，解压后 `cp -rn` 到 `/data2_backup/data_sources_data/`。

- `trade_dates.txt`（8797 行交易日历）— 每月更新一次（tushare）
- `raw/` — fetcher 运行生成，不在部署包中

## 九、rutask 配置

配置文件：`/home/data_ops/rustask/rustask.local.json5`

### 阶段一

```json5
data_sources_exchange: {
  start_cron: "00 40 16 * * 1-5",
  cmd: "data-sources-pipeline",
  cwd: "/home/data_ops",
  output: "/home/data_ops/log/pipeline-$datetime.log",
  safe_start: true,
  life_time: 7200,
}
```

### 阶段二

```json5
data_sources: {
  start_cron: "00 40 16 * * 1-5",
  cmd: "data-sources-pipeline",
  cwd: "/home/data_ops",
  output: "/home/data_ops/log/pipeline-$datetime.log",
  safe_start: true,
  life_time: 7200,
}
```

`data-sources-pipeline` 根据是否设置 `DEV_MODE` 自动选择 DB 连接参数。阶段切换通过 `WRITER_TABLE` 和 `SKIP_TABLE_COMPARE` 控制，在 `data-sources-pipeline` 中修改。

---

## 十、待办清单

| # | 事项 | 负责人 | 状态 |
|---|------|--------|------|
| 1 | writer/db/reporter/pipeline 参数化 | Agent4 | ✅ |
| 2 | wind_client.py (Wind RPC) | Agent4 | ✅ |
| 3 | Python 3.10 兼容修复 | Agent4 | ✅ |
| 4 | pyproject.toml 依赖 + script-files | Agent4 | ✅ |
| 5 | 脚本重命名 + 打包策略（whl + 仅 offline deps） | Agent4 | ✅ |
| 6 | sys-libs 并入 chromium-deps | Agent4 | ✅ |
| 7 | deployment-guide.md（/tmp 部署 + 日期变量 + 回滚） | Agent4 | ✅ |
| 8 | server202 部署测试（mini_apps 模拟 data_ops） | Agent4 | ✅ |
| 9 | db201 上正式安装 + 测试运行 | 新哲 | ⏳ |
| 10 | rutask JSON5 配置 | 新哲 | ⏳ |
| 11 | cpp_py Python 3.13 预编译 | 后续 | 📅 |

---

## 十一、相关文档

- `AgentMemories/db201-ops.md` — db201 操作手册
- `AgentMemories/server202-ops.md` — server202 跳板机操作手册
- `git.corp.wendao.fund/lixungeng/cpp_py` — cpp_py 源码及构建
