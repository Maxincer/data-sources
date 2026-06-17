# data-sources 阶段一部署手册

> 版本：20260617-f4a1939 | 本次是**升级部署**（从 `20260529-9602a79` 升级）
> 变更：纳入公告采集管线、数据目录重构（`raw/structured/`）、tushare 指数取数
> 入库：`future_cn.t_futures_info_exchange`（过渡表，与 Wind 并行）
> 面向部署人员：请严格按以下步骤操作。有疑问请联系 mxz@wendao.fund。

---

## 0. 前置条件

> 部署包已由开发人员准备好并上传到 server202。

| 条件 | 值 |
|------|-----|
| 部署包位置 | `/tmp/20260617-f4a1939.tar.gz`（server202 上） |
| 安装包大小 | ~419MB（含 data/ + deploy/offline/） |
| 目标服务器 | `192.168.1.201`（db201） |
| 操作用户 | `data_ops` |
| 预估时间 | 20-30 分钟 |

---

## 1. 环境

| 项目 | 值 |
|------|-----|
| 服务器 | `192.168.1.201` |
| 操作系统 | Ubuntu 22.04.5 LTS |
| 用户 | `data_ops` |
| 数据目录 | `/data2_backup/data_sources_data/` |
| 日志目录 | `/home/data_ops/log/` |
| Python | 系统 3.10.12（已安装 pip + venv） |
| 目标 MySQL | `192.168.1.27:3306`（`tools` / `future_cn`） |
| rutask Web UI | `http://192.168.1.201:8877` |
| rutask 配置文件 | `/home/data_ops/rustask/config.local.json5` |

---

## 2. 传输部署包到 db201

以有 scp 权限的用户登录 server202，执行：

```bash
# server202 → db201
RELEASE="20260617-f4a1939"
scp /tmp/${RELEASE}.tar.gz data_ops@192.168.1.201:/tmp/
```

> 如 server202 上无此文件，请联系开发人员重新生成。

---

## 3. 安装步骤

### 3.1 登录 db201 并解压

```bash
ssh data_ops@192.168.1.201
# 或 sudo su - data_ops

# 解压部署包
mkdir -p /tmp/data-sources
cd /tmp/data-sources
tar -xzf /tmp/20260617-f4a1939.tar.gz
```

### 3.2 数据目录迁移（旧 flat → 新 structured）

> **背景**：新版本将数据文件写入 `raw/structured/` 子目录。旧版数据文件在 `raw/` 下扁平存储，需要迁移。

```bash
# 创建 structured 目录
mkdir -p /data2_backup/data_sources_data/raw/structured

# 将旧扁平文件迁移到 structured 子目录
mv /data2_backup/data_sources_data/raw/*.{dat,csv,json,txt} \
  /data2_backup/data_sources_data/raw/structured/ 2>/dev/null

# product_configs 目录保留在原位（它不在 structured/ 中）
# .metadata.jsonl 也保留在原位
```

**验证迁移结果**（应看到 600+ 文件移过去了）：

```bash
ls /data2_backup/data_sources_data/raw/structured/ | wc -l    # 预期 ~630
ls /data2_backup/data_sources_data/raw/product_configs/ | wc -l  # 预期 ~290
ls /data2_backup/data_sources_data/raw/.metadata.jsonl          # 应存在
```

### 3.3 Playwright 浏览器（仅首次部署/缺失时执行）

```bash
ls /home/data_ops/playwright-browsers/chromium-1208 2>/dev/null && \
  echo "浏览器已存在，跳过" || {
  mkdir -p /home/data_ops/playwright-browsers
  cp -rn /tmp/data-sources/deploy/offline/playwright/chromium-1208 \
    /home/data_ops/playwright-browsers/
  cp -rn /tmp/data-sources/deploy/offline/playwright/chromium_headless_shell-1208 \
    /home/data_ops/playwright-browsers/
  echo "Playwright 浏览器已安装"
}
```

### 3.4 Chromium 系统依赖（缺失时执行）

```bash
# 检查关键库是否已装
dpkg -l | grep libnss3

# 如未安装（缺少 ii），执行：
sudo apt install -f -y
sudo apt install -y /tmp/data-sources/deploy/offline/chromium-deps/*.deb
```

### 3.5 Python 依赖安装（每次更新都必须执行）

```bash
pip3 install --user --no-index \
  --find-links=/tmp/data-sources/deploy/offline/pip-deps \
  data_sources mxz_utils pymysql playwright fire pyee greenlet \
  typing_extensions termcolor requests beautifulsoup4 soupsieve \
  certifi charset_normalizer idna urllib3 aiohttp pymupdf openpyxl
```

### 3.6 验证安装

```bash
# 1. Python 版本
python3 --version                    # 应显示 3.10.12

# 2. 所有依赖可导入
python3 -c "import pymysql; print('pymysql OK')"
python3 -c "import playwright; print('playwright OK')"
python3 -c "import data_sources; print('data_sources OK')"
python3 -c "import aiohttp; print('aiohttp OK')"
python3 -c "import fitz; print('pymupdf OK')"
python3 -c "import openpyxl; print('openpyxl OK')"
python3 -c "import cpp_py; print('cpp_py OK')"

# 3. pipeline 命令就位
which data-sources-pipeline          # 应显示 ~/.local/bin/data-sources-pipeline
```

### 3.7 清理临时文件

```bash
rm -rf /tmp/data-sources /tmp/20260617-f4a1939.tar.gz
```

---

## 4. rutask 配置

> ⏰ **重要时机提醒**：`data_sources_pipeline` 每天 **16:40** 定时执行（工作日）。升级操作请避开这个时间段，**建议在 17:00 之后操作**，确保当日 pipeline 已跑完。

### 4.1 查看现有配置

```bash
cat /home/data_ops/rustask/config.local.json5
```

确认已有 `update_if_exclude_wsi`（Wind）任务及 `data_sources_pipeline` 任务。

### 4.2 更新/添加 data_sources_pipeline 任务

编辑 `/home/data_ops/rustask/config.local.json5`，添加或确认以下内容：

```json5
data_sources_pipeline: {
  "cmd": "/home/data_ops/.local/bin/data-sources-pipeline",
  "hide_log": false,
  "output": ">>/home/data_ops/log/DataSourcesPipeline.$date.log",
  "safe_start": true,
  "start_cron": "00 40 16 * * 1-5"
}
```

> ⚠️ **注意事项**：
> - `cmd` 已确认使用完整路径 `${HOME}/.local/bin/data-sources-pipeline`，无需修改
> - **不要删除或修改** `update_if_exclude_wsi` 任务（阶段一保持 Wind 并行运行）
> - 所有密码和 API key 已嵌入 `data-sources-pipeline` 脚本本身，rutask 无需额外传 env

### 4.3 重载 rutask

```bash
curl http://192.168.1.201:8877/reload
```

---

## 5. 手动测试

### 5.1 全流程测试

选择一个最近的工作日测试（如今天或上一个交易日）：

```bash
# 测试全流程（会发邮件到 mxz@wendao.fund）
data-sources-pipeline 20260616
```

**预期输出**：
```
[HH:MM:SS] ===== pipeline started for 20260616 =====
[HH:MM:SS] Step 1/5: Collecting announcements...
... (公告采集日志)
[HH:MM:SS] Step 2/5: Analyzing announcements...
... (公告解析日志)
[HH:MM:SS] Step 3/5: Fetching...
... (交易所数据下载日志)
[HH:MM:SS] Step 4/5: Writing...
... (数据写入日志)
[HH:MM:SS] Step 5/5: Reporting + email...
... (验证报告 + 邮件)
[HH:MM:SS] ===== pipeline finished (status=0) =====
```

> 如果 `status=0` 但有 `[WARN]` 日志，只要数据写入成功就 OK。WARN 通常来自公告采集（非所有交易所都可访问）。

### 5.2 数据库验证

```bash
mysql -h 192.168.1.27 -u tools -ptools0512 future_cn
```

在 MySQL 提示符下执行：

```sql
-- 1. 表是否存在
SHOW TABLES LIKE 't_futures_info_exchange';

-- 2. 当日行数（预期 400~450）
SELECT COUNT(*) FROM t_futures_info_exchange WHERE date = '20260616';

-- 3. 交易所覆盖（预期 6 家）
SELECT SUBSTRING_INDEX(code, '.', -1) AS ex, COUNT(*) as cnt
FROM t_futures_info_exchange WHERE date = '20260616'
GROUP BY ex ORDER BY cnt DESC;

-- 4. 检查关键字段无 NULL
SELECT COUNT(*) FROM t_futures_info_exchange
WHERE date = '20260616' AND (open IS NULL OR close IS NULL);
-- 预期结果：0

-- 5. 与 Wind 表行数对比
SELECT 'ours' as src, COUNT(*) FROM t_futures_info_exchange WHERE date='20260616'
UNION ALL
SELECT 'wind' as src, COUNT(*) FROM t_futures_info WHERE date='20260616';
```

### 5.3 检查日志

```bash
# 查看全流程日志
tail -50 /home/data_ops/log/DataSourcesPipeline.20260616.log

# 各阶段详细日志
tail -30 /home/data_ops/log/Fetcher.20260616.log
tail -30 /home/data_ops/log/Writer.20260616.log
tail -30 /home/data_ops/log/Reporter.20260616.log
tail -30 /home/data_ops/log/Collector.20260616.log
tail -30 /home/data_ops/log/Analyser.20260616.log
```

### 5.4 检查邮件/飞书

部署后第一个交易日运行完后，确认：
- `mxz@wendao.fund` 收到每日验证报告邮件
- 飞书收到告警/报告（如配置了 `FEISHU_WEBHOOK`）

---

## 6. 单步调试（如需）

如果全流程报错，可以分步运行排查问题：

```bash
# Step 1: 仅下载交易所数据
python3 -m data_sources.fetcher run 20260616

# Step 2: 仅写入数据库
python3 -m data_sources.writer --date 20260616 --table t_futures_info_exchange

# Step 3: 仅生成报告
python3 -m data_sources.reporter --sender robot@wendao.fund \
  --recipients mxz@wendao.fund 20260616
```

---

## 7. 回滚

> **阶段一回滚很安全**，不影响生产表 `t_futures_info`（Wind 写入的表）。

```bash
# 1. 禁用 data_sources_pipeline 任务
#    在 rutask Web UI (http://192.168.1.201:8877) 禁用该任务即可

# 2. 清理当日数据（可选）
DATE="20260616"
mysql -h 192.168.1.27 -u tools -ptools0512 future_cn -e "
  SELECT date, COUNT(*) as cnt FROM t_futures_info_exchange WHERE date='${DATE}' GROUP BY date;
  DELETE FROM t_futures_info_exchange WHERE date = '${DATE}';
"

# 3. Wind 继续正常写 t_futures_info，不受任何影响
```

---

## 8. 日志文件位置

```
/home/data_ops/log/
├── DataSourcesPipeline.20260616.log     # 每日全流程
├── Fetcher.20260616.log                 # 交易所拉取
├── Writer.20260616.log                  # 解析/写入
├── Reporter.20260616.log                # 报告/邮件
├── Collector.20260616.log               # 公告采集
└── Analyser.20260616.log                # 公告解析（LLM）
```

---

## 9. 阶段一完成标准

进入阶段二（切换写 `t_futures_info` + 停 Wind）的必要条件（全部满足）：

- [ ] 连续 **5 个交易日** rutask 正常运行
- [ ] 每日行数 400~450，无 0 行异常
- [ ] 两表对比差异率 < 1%（稳定）
- [ ] Wind API 交叉验证通过（`wind_available=True`）
- [ ] 飞书/邮件报告正常送达

---

## 附录 A：环境变量速查

以下变量已内嵌在 `data-sources-pipeline` 脚本中。如需覆写（如切换收件人），可通过 rutask 的 `env` 配置或在执行前 export：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DB_HOST` | `192.168.1.27` | MySQL 地址 |
| `DB_USER` | `tools` | |
| `DB_PASSWORD` | `tools0512` | |
| `DB_DATABASE` | `future_cn` | |
| `DATA_DIR` | `/data2_backup/data_sources_data` | 数据目录 |
| `LOG_DIR` | `/home/data_ops/log` | 日志目录 |
| `SMTP_PASSWORD` | (内嵌) | 邮箱密码 |
| `RECIPIENTS` | `mxz@wendao.fund` | 报告收件人 |
| `DCE_API_KEY` | `ofxc69rpmd59` | DCE API |
| `DCE_API_SECRET` | (内嵌) | |
| `TUSHARE_TOKEN` | (内嵌) | |
| `TAVILY_API_KEY` | (内嵌) | |
| `DEEPSEEK_API_KEY` | (内嵌) | |
| `FEISHU_WEBHOOK` | (内嵌) | 飞书告警 |
| `WIND_RPC_HOST` | `192.168.2.9` | Wind RPC |

---

## 附录 B：关于首次部署

如需在全新服务器上首次部署（无已有数据），请参考 `/home/data_ops/deployment-guide.20260529-9602a79.md`（旧版手册），再按本手册 **3.2 数据目录迁移** 步骤操作。新服务器还需额外安装 `python3-pip` 和 `python3-venv`。
