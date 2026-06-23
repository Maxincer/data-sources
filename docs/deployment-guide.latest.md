# data-sources 阶段一部署手册

> 入库 `future_cn.t_futures_info_exchange`
> 面向部署人员（db201: `data_ops`）
> 本次是**升级部署**（从 `20260529-9602a79`(v0.1.0) 升级到 `20260622-38c02d4`(v0.1.1)）
> 变更：纳入公告采集管线（LLM 解析）、数据目录重构（`raw/structured/`）、tushare 指数取数、配置外置化、mxz_utils升级

---

## 1. 环境

| 项目 | 值 |
|------|-----|
| 服务器 | `192.168.1.201` |
| 用户 | `data_ops` |
| 数据目录 | `/data2_backup/data_sources_data/` |
| 日志目录 | `/home/data_ops/log/` |
| Python | 系统 3.10+ |
| MySQL | `192.168.1.27`（`tools` / `future_cn`） |
| rutask 配置 | `/home/data_ops/rustask/rustask.local.json5` |
| 可执行文件 | `~/.local/bin/data-sources-pipeline`（pip 安装） |
| site-packages | `~/.local/lib/python3.10/site-packages/data_sources/` |

---

## 2. 部署步骤

### 2.1 下载与解压

```bash
RELEASE="20260622-38c02d4"

wget -O /tmp/${RELEASE}.tar.gz http://releases.corp.wendao.fund/data-sources/${RELEASE}.tar.gz

mkdir -p /tmp/data-sources && cd /tmp/data-sources && tar -xzf /tmp/${RELEASE}.tar.gz
```

### 2.2 数据目录迁移（升级必做）

> **背景**：新版将数据文件写入 `raw/structured/` 子目录。旧版数据文件在 `raw/` 下扁平存储，需迁移。

```bash
mkdir -p /data2_backup/data_sources_data/raw/structured

# 迁移数据文件
mv /data2_backup/data_sources_data/raw/*.{dat,csv,json,txt} /data2_backup/data_sources_data/raw/structured/ 2>/dev/null

# .metadata.jsonl 也要迁（旧版 reporter 读 raw/，新版读 raw/structured/）
mv /data2_backup/data_sources_data/raw/.metadata.jsonl /data2_backup/data_sources_data/raw/structured/ 2>/dev/null || echo "无旧 metadata，新版自建"

# product_configs 目录不动（新版代码已将其路径独立于 structured/）
```

### 2.3 首次部署：Chromium 系统依赖

> 仅首次部署执行。检查依赖是否已安装，缺了再装。

```bash
sudo apt install -f -y
sudo apt install -y /tmp/data-sources/deploy/offline/chromium-deps/*.deb
```

### 2.4 首次部署：Playwright 浏览器

> 仅首次部署执行。CFFEX 网页抓取需要。

```bash
mkdir -p /home/data_ops/playwright-browsers
cp -rn /tmp/data-sources/deploy/offline/playwright/chromium-1208 /home/data_ops/playwright-browsers/
cp -rn /tmp/data-sources/deploy/offline/playwright/chromium_headless_shell-1208 /home/data_ops/playwright-browsers/
```

### 2.5 安装 Python 依赖

> 每次更新都需执行。新增依赖：`aiohttp`、`pymupdf`、`openpyxl`、`tushare`。
> **本次升级**：`mxz_utils` 从 `0.1.0` 升级到 `0.1.1`（已含在 pip-deps 中）。

```bash
pip3 install --user --no-index --find-links=/tmp/data-sources/deploy/offline/pip-deps aiohttp pymupdf openpyxl tushare

pip3 install --user --no-index --upgrade mxz_utils data_sources --find-links=/tmp/data-sources/deploy/offline/pip-deps
```

### 2.6 首次部署：配置 rutask

> 仅首次部署执行。后续更新无需修改。

`/home/data_ops/rustask/rustask.local.json5`

```json5
data_sources_pipeline: {
  "start_cron": "00 40 16 * * 1-5",
  "cmd": "/home/data_ops/.local/bin/data-sources-pipeline",
  "output": ">>/home/data_ops/log/DataSourcesPipeline.$date.log",
  "safe_start": true,
  "hide_log": false
}
```

重载：

```bash
curl http://192.168.1.201:8877/reload
# 或
rustask --sock cmd-rustask.sock reload
```

### 2.7 清理

```bash
rm -rf /tmp/data-sources /tmp/${RELEASE}.tar.gz
```
