# data-sources 部署日志

> 记录部署过程中的讨论、决策、问题和进展。
> 创建时间：2026-06-17

---

## 2026-06-18 文档同步：b34f683 版本更新

### 发现：文档与代码版本不一致

`deployment-guide.latest.md` 基于 `f4a1939` 编写，但当前 HEAD 是 `b34f683`（在 `f4a1939` 之后又有代码去硬编码 + 配置外置清理提交）。

**差异清单**：
1. 版本号：文档 `f4a1939` → HEAD `b34f683`
2. 敏感信息：分散硬编码 → 统一 env var fallback（`${VAR:-default}`）
3. 新增 Zhipu API key（公告 LLM 解析用）
4. 时间门禁：注释掉 → 启用（< 16:30 跳过）
5. Step 1/3：注释掉 → 取消注释正式运行
6. 硬编码 `TRADE_DATE="20260616"` → 删除
7. `pyproject.toml` 新增 `tushare>=1.4.0`
8. `PRODUCT_CONFIG_DIR` 路径独立于 structured/
9. `DB_PASSWORD`/`DATA_DIR` 从可选 → 必填

**操作**：已将 `deployment-guide.latest.md` 更新到 HEAD 版本 `20260617-b34f683`。

---

## 2026-06-17 初始讨论 + 部署准备

### 目标

将 data-sources 部署到 db201 (`192.168.1.201`)，逐步替换 Wind 数据源。

**关键发现**：db201 上已有之前部署的痕迹（数据从 4/20 更新到 6/15），本次是**阶段一升级部署**，不是首次部署。

### 两阶段策略确认

| 阶段 | 写入表 | 说明 |
|------|--------|------|
| 阶段一 | `future_cn.t_futures_info_exchange` | 过渡表，Wind 继续写 `t_futures_info`，并行验证 |
| 阶段二 | `future_cn.t_futures_info` | 切换生产，停 Wind |

---

## 决策记录

| 日期 | 事项 | 决策 | 备注 |
|------|------|------|------|
| 2026-06-17 | Q01 部署方式 | 离线包（本地 build whl + scp） | 内部 releases 站点未经验证 |
| 2026-06-17 | Q02 用户身份 | `data_ops` | 与 rutask 运行用户一致 |
| 2026-06-17 | Q03 公告管线 | 纳入部署 | 本次升级的重要功能 |
| 2026-06-17 | Q04 凭据齐全 | ✅ 新哲确认全部都有 | — |
| 2026-06-17 | Q05 报告收件人 | `mxz@wendao.fund` 一人 | rutask 可手动触发 |
| 2026-06-17 | Q07 数据目录结构 | 按当前设计迁移 flat → structured/ | mv 方案 |
| 2026-06-17 | Q08 新依赖 | aiohttp/pymupdf/openpyxl 已在 pip-deps 中 | cp310 wheels 齐全 |
| 2026-06-17 | Q09 data_ops 权限 | ✅ 新哲有账户，可直接登录 | 部署时新哲操作 data_ops 侧 |

---

## 问题追踪

| 编号 | 描述 | 状态 | 解决日期 |
|------|------|------|----------|
| Q01 | 部署方式选择 | ✅ 离线包 | 2026-06-17 |
| Q02 | 用户身份（maxinzhe vs data_ops） | ✅ data_ops | 2026-06-17 |
| Q03 | 公告管线是否同步部署 | ✅ 纳入 | 2026-06-17 |
| Q04 | 全部环境变量凭据是否齐备 | ✅ 有 | 2026-06-17 |
| Q05 | 阶段一报告收件人范围 | ✅ mxz@wendao.fund | 2026-06-17 |
| Q06 | db201 当前环境就绪程度 | ⚠️ 见下方详细状态 | 2026-06-17 |
| Q07 | 数据目录结构差异（旧 flat vs 新 structured/） | 🔴 待定 | — |

---

## db201 环境状态

> 检查时间：2026-06-17 17:33
> 操作说明：仅检查，未做任何改动

### 系统信息

| 项目 | 值 |
|------|------|
| 主机名 | db201 |
| OS | Ubuntu 22.04.5 LTS (Kernel 5.15.0-134-generic) |
| 硬件 | Dell Precision 7920 Tower |
| CPU | 72 核 |
| 内存 | 251 GiB（可用 111 GiB） |
| 磁盘（系统） | 467G / 84G 可用（18%） |
| 磁盘（数据） | 15T / 8.6T 可用（/data2_backup） |
| Swap | 0 |
| Python | 3.10.12（内置），pip 25.2，venv OK |
| MySQL | 8.0.41（本机）| |

### data_ops 用户

| 项目 | 状态 |
|------|------|
| 用户存在 | ✅ uid=1007(data_ops) |
| home 权限 | `drwxr-x---`（maxinzhe 无法访问）| |

### 已有部署

| 项目 | 状态 | 说明 |
|------|------|------|
| 数据目录 | ✅ | `/data2_backup/data_sources_data/` 存在，属主 data_ops |
| 原始数据 | ✅ | 632 个文件，2026-04-20 到 2026-06-15 持续更新 |
| 目录结构 | ⚠️ **旧 flat 布局** | 文件在 `raw/` 下，非 `raw/structured/`（新旧代码差异） |
| `.metadata.jsonl` | ✅ | 3862 行，更新到 6月15日 |
| `product_configs/` | ✅ | 包含 SHFE/INE 每日 HTML 配置页 |
| `trade_dates.txt` | ✅ | 79KB |
| `order_limits.csv` | ✅ | 5KB |

### rutask（任务调度）

| 项目 | 状态 | 说明 |
|------|------|------|
| 进程 | ✅ 运行中 | `data_ops` 用户，`/home/data_ops/rustask/rustask.exe --port 8877` |
| 端口 | ✅ 8877 | Web UI → `http://192.168.1.201:8877` |
| 配置 | ✅ | `/home/data_ops/rustask/config.local.json5` |
| 名称 | `data_ops` | rustask `-n T201` 也同时运行（root） |
| 配置文件 | ⚠️ **内容未知** | maxinzhe 无 data_ops home 访问权限 |

### 系统依赖

| 类别 | 状态 |
|------|------|
| libnss3/libnspr4 | ✅ 已装 |
| libgtk-3 | ✅ 已装 |
| libcairo/libpango/libx11 | ✅ 已装 |
| libgbm/libatk/libgdk-pixbuf | ✅ 已装 |
| libwayland/libxcb | ✅ 已装 |
| Chromium 浏览器 | ⚠️ 未知（`/home/data_ops/` 不可读） |
| Playwright 浏览器 | ⚠️ 同上 |

### 网络连通

| 目标 | 状态 |
|------|------|
| MySQL 集群 (192.168.1.27:3306) | ✅ 可达 |

### 注意事项

1. **数据目录结构差异**：旧部署使用 `raw/` 扁平目录，当前代码 (`fetcher.py`) 要求 `raw/structured/` 子目录。部署前需解决此差异。
2. **data_ops home 不可读**：maxinzhe 无 sudo 权限，无法查看 data_ops 的用户配置（pip 已装包、rutask 配置、Playwright 浏览器）。这些需要在以 data_ops 身份操作时才能确认。
3. **已有 rutask 配置**：数据库中可能已有 data_sources 旧任务，需确认是否保留/覆盖。

---

## 参考文档归档

以下内容摘自项目历史部署文档，整理归档供参考。

### A. `deployment-plan.md`（2026-05-29 v3）

**核心要点**：

**打包策略**：
- `data-sources-pipeline` 通过 `[tool.setuptools.script-files]` 打入 whl
- 部署包仅含 `data/` + `deploy/offline/`，不留项目文件夹
- `sys-libs/` 并入 `chromium-deps/`

**Python 3.10 兼容修复**（已完成 ✅）：
- `verifier.py`: f-string 内 `\|` 提取为变量
- `wind_client.py` / `fetcher.py`: 模块级 `os.environ[]` 改为函数内懒加载

**代码改动汇总**（已完成 ✅）：
- `db.py` — `config_override` 参数化
- `writer.py` — `--host` `--port` `--db` `--table`
- `reporter.py` — `--skip-table-compare`
- `wind_client.py` — cpp_py RPC 调 Wind WSD
- `pyproject.toml` — 补 `mxz-utils` 依赖 + `script-files`

**两阶段策略**：
- 阶段一：写 `t_futures_info_exchange`，Wind 并行
- 阶段二：切换 `t_futures_info`，停 Wind

**Wind 写入方式影响**：
- Wind 用 `INSERT IGNORE`，已有行静默跳过
- 回滚时必须先 `DELETE` 当日 data-sources 行再重跑 Wind

**待办清单**（v3 版本）：
- #1-8 Agent4 ✅
- #9-10 新哲 ⏳（db201 安装 + rutask 配置）
- #11 cpp_py Python 3.13 📅

---

### 本次操作

| 操作 | 状态 |
|------|------|
| 阅读 `deployment-plan.md` + `phase1-deployment.md`，合并入本日志 | ✅ |
| `deployment-guide.md` → `deployment-guide.20260529-9602a79.md` | ✅ |
| 新建 `deployment-guide.latest.md`（面向部署人员的操作手册） | ✅ |
| 确定版本号：`20260617-f4a1939` | ✅ |
| pipeline 脚本调整（修复时间门禁注释、env var fallback、补充 DAILY_START_DATE） | ✅ |
| rebuild whl + 打入最新 pipeline 脚本 | ✅ |
| 打包部署包 `/tmp/20260617-f4a1939.tar.gz`（419MB） | ✅ |
| 确认 pip-deps 包含 aiohttp/pymupdf/openpyxl | ✅ |
| 解决 Q07/Q08/Q09 | ✅ |

### 待定事项

**Q07 — 数据目录结构迁移**

- 旧数据在 `raw/` 下扁平存储（`YYYYMMDD.EXCHANGE.TYPE.ext`）
- 新代码写 `raw/structured/` 子目录
- 部署手册中暂写入 `mv` 方案，可讨论是否改为 `rsync`/`cp` 保留副本
- 需确认新代码的 parser 是否兼容两种路径

**Q08 — `data_ops` home 目录不可读**

- `maxinzhe` 用户无 `data_ops` home 权限
- 验证安装、查看 rutask 现有配置、查看日志都需要 data_ops 身份
- 部署时需新哲帮忙操作 data_ops 侧

---

### B. `phase1-deployment.md`（历史阶段一部署文档）

**核心要点**：

**数据流**（阶段一）：
```
交易所 API → fetcher → data/raw/ → writer → t_futures_info_exchange
Wind → 继续写 t_futures_info（不受影响）
reporter → 两表比对 + Wind API 交叉验证
```

**关键参数**：
- 用户：`data_ops`，项目路径：`/home/data_ops/data-sources/`
- MySQL：`192.168.1.27:3306`，`tools` / `tools0512`，`future_cn`
- rutask：`http://192.168.1.201:8877`，工作日 16:45

**前置条件**（WSL 已就绪）：
- Playwright 浏览器（616M）、chromium-deps（18M 77 .deb）、pip-deps（80M 16 .whl）

**待执行改动**（当时列出，可能已过时）：
- `db.py` 默认 host → `192.168.1.27`
- pipeline 时间门禁 16:27 → 16:45

**server202 Wind 交叉验证**：
- 在 server202 上先测试 WindClient 连通性
- 然后全量比对测试

**验证清单**：
- 表存在、行数 400~450、6 家交易所覆盖、无 NULL 关键字段
- 飞书收到报告、差异 < 1%

**日常运维**：
- 日志 `/home/data_ops/log/YYYYMMDD.log`
- `trade_dates.txt` 每月更新

**阶段一完成标准**：
- 连续 5 个交易日正常运行
- 每日 400~450 行
- 差异率 < 1%
- Wind API 交叉验证通过

---

## 问题追踪补充

| 编号 | 描述 | 状态 | 解决日期 |
|------|------|------|----------|
| Q07 | 数据目录结构差异（flat → structured） | ✅ 按当前设计迁移 | 2026-06-17 |
| Q08 | 公告管线新增依赖（aiohttp/pymupdf/openpyxl）在 pip-deps 中是否存在 | ✅ 全部存在（cp310 wheels） | 2026-06-17 |
| Q09 | data_ops 账户权限 | ✅ 新哲有账户，可直接登录 | 2026-06-17 |

---

## 操作步骤（待执行）

- [✅] Q01 部署方式 → 离线包
- [✅] Q02 用户身份 → data_ops
- [✅] Q03 公告管线 → 纳入
- [✅] Q04 凭据 → 新哲都有
- [✅] Q05 收件人 → mxz@wendao.fund
- [✅] Q06 db201 环境检查 → 已完成
- [✅] Q08 文档归档 & 新建 deploy-guide → 已完成
- [✅] Q07 数据目录 → 按当前设计迁移
- [✅] Q08 pip-deps → aiohttp/pymupdf/openpyxl 齐全
- [✅] Q09 data_ops → 新哲有账户
- [✅] pipeline 脚本调整（修复时间门禁注释、env var fallback、补充 DAILY_START_DATE）
- [✅] build whl + 打入最新 pipeline 脚本
- [✅] 打包部署包 `/tmp/20260617-f4a1939.tar.gz`（419MB）
- [✅] 部署手册更新为 `deployment-guide.latest.md`（面向部署人员）
- [ ] scp 到 db201（需要：从当前机器 scp 到 server202）
- [ ] 部署人员执行：
  - [ ] server202 → db201 scp
  - [ ] 解压 + 数据目录迁移
  - [ ] Playwright 浏览器检查
  - [ ] Chromium 系统库检查
  - [ ] pip 安装新 whl + 依赖
  - [ ] 验证安装
  - [ ] 查看现有 rutask 配置
  - [ ] 更新 rutask 配置
  - [ ] 重载 rutask
  - [ ] 手动测试运行
  - [ ] 数据库验证
- [ ] 在 WSL 上 build whl + 打包
- [ ] scp 到 db201
- [ ] 新哲登录 data_ops 安装 + 配置
- [ ] 测试运行
- [ ] 进入阶段一观察
