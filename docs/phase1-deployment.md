# data-sources 阶段一部署文档

> 目标：将 data-sources 部署到 db201，写入过渡表 `t_futures_info_exchange`，与 Wind 并行运行验证。

---

## 1. 架构

### 1.1 数据流

```
交易所 API (6 家)               Wind
    │                              │
    ▼                              │
fetcher ──► data/raw/              │
    │                              │
    ▼                              ▼
writer ──► t_futures_info_exchange (ours)   t_futures_info (Wind)
               │                              │
               └──────── reporter ────────────┘
                          │
                  ┌───────┴───────┐
                  ▼               ▼
            两表比对报告    Wind API 交叉验证
```

### 1.2 关键参数

| 参数 | 值 |
|------|-----|
| 运行用户 | `data_ops` |
| 项目路径 | `/home/data_ops/data-sources/` |
| 目标 MySQL | `192.168.1.27:3306` |
| 用户名/密码 | `tools` / `tools0512` |
| 写入表 | `future_cn.t_futures_info_exchange` |
| 对比表 | `future_cn.t_futures_info`（Wind 写的） |
| 调度系统 | rutask（Web UI: `http://192.168.1.201:8877`） |
| 调度时间 | 工作日 16:45 |

### 1.3 与 Wind 的关系

- **并行运行**：Wind 继续写 `t_futures_info`，data-sources 写 `t_futures_info_exchange`，互不干扰
- **不做回退**：阶段一不影响生产表，无回退需求
- **对比验证**：reporter 每天自动比对两表差异

---

## 2. 前置条件

### 2.1 已就绪（WenDaoInvPC-200 / WSL）

| 事项 | 路径 | 大小 |
|------|------|------|
| Chromium 浏览器 | `deploy/offline/playwright/` | 616M |
| Chromium 系统 .deb | `deploy/offline/chromium-deps/` | 18M |
| pip 依赖 .whl | `deploy/offline/pip-deps/` | 80M |
| 部署脚本 | `scripts/deploy.sh` | — |
| 流水线脚本 | `scripts/pipeline.sh` | — |
| 项目源码 | `src/` | — |

### 2.2 待执行代码改动

> ⚠️ 以下改动需在部署前完成：

1. **`src/data_sources/db.py`**：默认 host 改为 `192.168.1.27`，user → `tools`，password → `tools0512`

2. **`scripts/pipeline.sh`**：时间门禁 16:27 → 16:45，删除 16:40 等待逻辑

### 2.3 db201 环境要求

| 检查项 | 命令 | 预期 |
|--------|------|------|
| Python 3.10 | `python3 --version` | `3.10.12` |
| pip 可用 | `python3 -m pip --version` | 有输出 |
| cpp_py 可用 | `python3 -c "import cpp_py"` | 无报错 |
| MySQL 连通 | `mysql -h 192.168.1.27 -u tools -ptools0512 -e "SELECT 1"` | `1` |
| rutask 运行 | `curl http://192.168.1.201:8877` | 有响应 |
| sudo 权限 | `sudo -v` | 无报错 |
| `data_ops` 用户存在 | `id data_ops` | 有输出 |

---

## 3. server202 Wind 交叉验证（在部署 db201 之前先做）

server202 上已有系统级 `cpp_py`，可在部署前验证 Wind 比对逻辑是否正常。

### 3.1 连通性测试

```bash
ssh server202
cd /path/to/data-sources   # 实际项目路径

python3 -c "
from data_sources.wind_client import WindClient
c = WindClient()
print('Wind available:', c.available)
if c.available:
    # 测试一个合约的当日数据
    from datetime import datetime
    d = c.get_wsd('CU2606.SHF', '20260523', ['open','close','volume','settle'])
    print('CU2606.SHF:', d)
"
```

### 3.2 全量比对测试

```bash
# 跑一天的完整 reporter（含 Wind 交叉验证）
python3 -m data_sources.reporter 20260523 --skip-table-compare

# 预期输出：
# Wind cross-validation: X/Y matched, Z diffs
```

> 如果 `available: False`，检查 cpp_py 是否安装：`python3 -c "import cpp_py; print(cpp_py.__file__)"`

---

## 4. db201 部署步骤

### 4.1 打包 + 传输（在 WenDaoInvPC-200 WSL）

```bash
cd /mnt/e/projects/data-sources

# 构建 whl
pip install build  # 如未装
python3 -m build --wheel
cp dist/data_sources-0.1.0-py3-none-any.whl scripts/

# 打包 data/
tar czf /tmp/data-sources-data.tar.gz data/

# 打包离线依赖
tar czf /tmp/data-sources-offline.tar.gz deploy/offline/

# 传输：WSL → server202 → db201
# Step 1: 上传到 server202
scp /tmp/data-sources-data.tar.gz     mini_apps@192.168.1.202:/tmp/
scp /tmp/data-sources-offline.tar.gz  mini_apps@192.168.1.202:/tmp/
scp -r scripts/                       mini_apps@192.168.1.202:/tmp/data-sources-scripts/

# Step 2: 从 server202 传到 db201
ssh mini_apps@192.168.1.202 "
  scp /tmp/data-sources-data.tar.gz     data_ops@192.168.1.201:/tmp/ &&
  scp /tmp/data-sources-offline.tar.gz  data_ops@192.168.1.201:/tmp/ &&
  scp -r /tmp/data-sources-scripts/     data_ops@192.168.1.201:/tmp/
"
```

### 4.2 安装（在 db201，以 data_ops 用户）

```bash
# 切换到 data_ops 用户
sudo su - data_ops

# 创建目录结构
mkdir -p ~/data-sources/{deploy/offline,log}
cd ~/data-sources

# 解压
tar xzf /tmp/data-sources-offline.tar.gz -C deploy/offline/ --strip-components=2
tar xzf /tmp/data-sources-data.tar.gz
cp /tmp/data-sources-scripts/* scripts/

# 确认 Python 环境
python3 --version       # 3.10.12
python3 -m pip --version

# 安装 Chromium 系统依赖（需要 sudo）
sudo dpkg -i deploy/offline/chromium-deps/*.deb
sudo apt install -f -y   # 修复可能的依赖断裂

# 执行部署脚本
bash scripts/deploy.sh

# 验证
fetcher --help
writer --help
reporter --help
```

### 4.3 配置 rutask

编辑 rutask 任务配置文件，新增：

```json5
data_sources_exchange: {
  start_cron: "00 45 16 * * 1-5",
  cmd: "bash scripts/pipeline.sh",
  cwd: "/home/data_ops/data-sources",
  output: "/home/data_ops/data-sources/log/$datetime.log",
  safe_start: true,
  life_time: 7200,
}
```

> 注意：不要动 `update_if_exclude_wsi` 任务（Wind），阶段一保持运行。

### 4.4 手动首跑测试

```bash
cd ~/data-sources

# 用某已知交易日测试（例如最近一个交易日）
bash scripts/pipeline.sh 20260523

# 检查日志
tail -50 log/$(date +%Y%m%d)*.log
```

---

## 5. 验证清单

### 5.1 数据库验证

```bash
mysql -h 192.168.1.27 -u tools -ptools0512 future_cn

-- 1. 表存在
SHOW TABLES LIKE 't_futures_info_exchange';

-- 2. 测试日有数据
SELECT date, COUNT(*) as cnt FROM t_futures_info_exchange
WHERE date = '20260523' GROUP BY date;
-- 预期：400~450 行

-- 3. 交易所覆盖
SELECT
  CASE
    WHEN code LIKE '%.SHF' OR code LIKE '%.INE' THEN 'SHFE/INE'
    WHEN code LIKE '%.DCE' THEN 'DCE'
    WHEN code LIKE '%.CZC' THEN 'CZCE'
    WHEN code LIKE '%.CFE' THEN 'CFFEX'
    WHEN code LIKE '%.GFE' THEN 'GFEX'
  END as exchange,
  COUNT(*) as cnt
FROM t_futures_info_exchange WHERE date = '20260523'
GROUP BY exchange ORDER BY cnt DESC;
-- 预期：6 家

-- 4. 无 NULL 关键字段
SELECT COUNT(*) FROM t_futures_info_exchange
WHERE date = '20260523' AND (open IS NULL OR close IS NULL);
-- 预期：0

-- 5. 与 Wind 表行数对比
SELECT 'ours' as src, COUNT(*) FROM t_futures_info_exchange WHERE date='20260523'
UNION ALL
SELECT 'wind' as src, COUNT(*) FROM t_futures_info WHERE date='20260523';
-- 预计接近（可能出现 0~10 行差异）
```

### 5.2 报告验证

- 飞书收到 📊 数据验证报告
- 报告包含：文件大小统计、字段覆盖率、两表对比差异、Wind 交叉验证
- 差异数据 < 1%

---

## 6. 日常运维

### 6.1 每日检查（建议前 5 个交易日手动确认）

```bash
# 1. rutask 状态
curl -s http://192.168.1.201:8877 | grep data_sources

# 2. 当天日志
tail -30 ~/data-sources/log/$(date +%Y%m%d)*.log

# 3. 飞书报告是否有异常标记
```

### 6.2 日志位置

```
/home/data_ops/data-sources/log/
├── 20260527.log
├── 20260528.log
└── ...
```

### 6.3 异常处理

| 症状 | 可能原因 | 操作 |
|------|---------|------|
| rutask exited(1) | pipeline 报错 | 查看日志定位 |
| 行数 < 300 | 某交易所未拉取 | 查看 fetcher 阶段日志 |
| 飞书未收到报告 | SMTP/DNS 问题 | 检查网络 |
| 两表差异 > 5% | 数据源变更 | 对比具体差异合约 |
| Wind 比对 unavailable | cpp_py 问题 | 检查 `import cpp_py` |

### 6.4 交易日历更新

`trade_dates.txt` 来自 tushare，需定期更新（每月一次）：

```bash
python3 -c "from data_sources.trade_date import update; update()"
```

---

## 7. 阶段一完成标准

进入阶段二的**必要条件**（全部满足）：

- [ ] 连续 **5 个交易日** rutask 正常运行
- [ ] 每日行数 400~450，无 0 行异常
- [ ] 两表对比差异率 < 1%（稳定）
- [ ] Wind API 交叉验证通过（`wind_available=True`，对比无异常）
- [ ] `t_futures_info_exchange` 与 `t_futures_info` 行数差异可解释（如缺失指数代码）

---

## 8. 附录

### A. rutask 任务文件位置

```
/home/rutask/config.json5   # 或通过 Web UI 的配置文件路径
```

### B. 相关文档

| 文档 | 位置 |
|------|------|
| 完整部署方案 | `AgentMemories/AgentDiaries/data-sources-deployment-plan.md` |
| db201 操作手册 | `AgentMemories/Ops/db201-ops.md` |
| server202 操作手册 | `AgentMemories/Ops/server202-ops.md` |
