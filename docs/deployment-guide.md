# data-sources 阶段一部署手册

> 面向部署人员（db201: `data_ops`）

---

## 1. 环境

| 项目 | 值 |
|------|-----|
| 服务器 | `192.168.1.201` |
| 用户 | `data_ops` |
| 项目路径 | `/home/data_ops/data-sources/` |
| 数据目录 | `/data2_backup/data_sources_data/` |
| 日志目录 | `/home/data_ops/log/` |
| Python | 系统 3.10+ |
| MySQL | `192.168.1.27`（`tools` / `future_cn`） |
| rutask 配置 | `/home/data_ops/rustask/rustask.local.json5` |

---

## 2. 部署步骤

### 2.1 下载与解压

```bash
RELEASE="20260528-f3b9e79"   # 替换为实际版本
wget -O /tmp/${RELEASE}.tar.gz \
  http://releases.corp.wendao.fund/data-sources/${RELEASE}.tar.gz

cd /home/data_ops
tar -xzf /tmp/${RELEASE}.tar.gz
```

### 2.2 首次部署：初始化数据目录

> 仅首次部署执行，后续更新跳过。

```bash
mkdir -p /data2_backup/data_sources_data
cp -rn data-sources/data/* /data2_backup/data_sources_data/
```

### 2.3 首次部署：系统依赖

> 仅首次部署执行。Chromium 运行所需的系统库。

```bash
# Chromium 依赖（约 77 个 .deb）
sudo dpkg -i data-sources/deploy/offline/chromium-deps/*.deb 2>/dev/null || {
    sudo apt install -f -y
    sudo dpkg -i data-sources/deploy/offline/chromium-deps/*.deb
}

# 额外系统库
sudo dpkg -i data-sources/deploy/offline/sys-libs/*.deb 2>/dev/null || true
```

### 2.4 首次部署：Playwright 浏览器

> 仅首次部署执行。CFFEX 网页抓取需要。

```bash
mkdir -p /home/data_ops/playwright-browsers
cp -rn data-sources/deploy/offline/playwright/chromium-1208 /home/data_ops/playwright-browsers/
cp -rn data-sources/deploy/offline/playwright/chromium_headless_shell-1208 /home/data_ops/playwright-browsers/
```

### 2.5 安装 Python 依赖

> 每次更新都需执行。

```bash
cd data-sources
pip3 install --user --no-index --find-links=deploy/offline/pip-deps -r requirements.txt
```

### 2.6 首次部署：配置 rutask

> 仅首次部署执行。后续更新无需修改。

编辑 `/home/data_ops/rustask/rustask.local.json5`，在文件末尾加入：

```json5
data_sources_pipeline: {
  start_cron: "00 40 16 * * 1-5",       // 工作日 16:40
  cmd: "bash data-sources/scripts/pipeline.sh",
  cwd: "/home/data_ops",
  output: "/home/data_ops/log/pipeline-$datetime.log",
  safe_start: true,
  life_time: 7200,                        // 超时 2h
}
```

rutask 自动监听配置文件变更，无需手动重启。

> `pipeline.sh` 根据是否设置 `DEV_MODE` 自动选择 DB 连接参数，无需 rutask 额外传 env。

### 2.7 验证

```bash
python3 --version                    # → 3.10+
python3 -c "import pymysql"          # 无报错
python3 -c "import cpp_py"           # 无报错
python3 -c "import playwright"       # 无报错
python3 -c "import data_sources"     # 无报错
```

```sql
-- 行数（预期 ~870 条/日）
SELECT COUNT(*) FROM t_futures_info_exchange WHERE date = '20260527';

-- 交易所覆盖（预期 6 家）
SELECT SUBSTRING_INDEX(code, '.', -1) AS ex, COUNT(*)
FROM t_futures_info_exchange WHERE date = '20260527'
GROUP BY ex;

-- margin 格式（百分数，12.0 = 12%）
SELECT ROUND(MIN(long_margin),2), ROUND(MAX(long_margin),2)
FROM t_futures_info_exchange WHERE date = '20260527';
```

---

## 3. 手动操作

### 3.1 单日测试

```bash
cd ~/data-sources

# 全流程（需先确认 raw data 已就绪）
bash scripts/pipeline.sh 20260527

# 仅写入（跳过下载和报告）
python3 -m data_sources.writer --date 20260527 --table t_futures_info_exchange
```

### 3.2 回滚

```sql
DELETE FROM t_futures_info WHERE date = '20260527';
-- 联系 Wind 管理员重跑 update_if_exclude_wsi.sh
```

---

## 4. 日志

```
/home/data_ops/log/
├── pipeline-20260527.log       # 每日全流程（rutask 重定向）
├── Fetcher.20260527.log        # 下载
├── Writer.20260527.log         # 解析/写入
└── Reporter.20260527.log       # 报告 / 邮件
```
