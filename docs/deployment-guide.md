# data-sources 阶段一部署手册

> 入库`future_cn`.`t_futures_info_exchange`
> 面向部署人员（db201: `data_ops`）
> 部署后不留项目文件夹，所有运行时文件在目标路径中

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
RELEASE="20260529-f3b9e79"   # 替换为实际版本
wget -O /tmp/${RELEASE}.tar.gz http://releases.corp.wendao.fund/data-sources/${RELEASE}.tar.gz

mkdir -p /tmp/data-sources
cd /tmp/data-sources
tar -xzf /tmp/${RELEASE}.tar.gz
```

### 2.2 首次部署：初始化数据目录

> 仅首次部署执行，后续更新跳过。

```bash
mkdir -p /data2_backup/data_sources_data
cp -rn /tmp/data-sources/data/* /data2_backup/data_sources_data/
```

### 2.3 首次部署：Chromium 系统依赖

> 仅首次部署执行。检查依赖是否已安装，缺了再装。

```bash
# 修复依赖
sudo apt install -f -y

# 安装 chromium 系统库（apt 自动跳过已安装、不会降级）
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

> 每次更新都需执行。

```bash
pip3 install --user --no-index --find-links=/tmp/data-sources/deploy/offline/pip-deps data_sources mxz_utils pymysql playwright fire pyee greenlet typing_extensions termcolor requests beautifulsoup4 soupsieve certifi charset_normalizer idna urllib3
```

### 2.6 首次部署：配置 rutask

> 仅首次部署执行。后续更新无需修改。

编辑 `/home/data_ops/rustask/rustask.local.json5`，在文件末尾加入：

```json5
data_sources_pipeline: {
  "cmd": "data-sources-pipeline",
  "hide_log": false,  
  "output": "/home/data_ops/log/DataSourcesPipeline.$date.log",
  "safe_start": true,
  "start_cron": "00 40 16 * * 1-5"
}
```

配置后 reload 生效

```bash
curl http://192.168.1.201:8877/reload
# 或者
rustask --sock cmd-rustask.sock reload
```

> 无需 rutask 额外传 env：`data-sources-pipeline` 根据是否设置 `DEV_MODE` 自动选择 DB 连接参数，

### 2.7 验证

```bash
python3 --version                    # → 3.10+
python3 -c "import pymysql"          # 无报错
python3 -c "import cpp_py"           # 无报错
python3 -c "import playwright"       # 无报错
python3 -c "import data_sources"     # 无报错
```

```sql
-- 设定测试日期
SET @d = '2026-05-27';

-- 行数（预期 ~870 条/日）
SELECT COUNT(*) FROM t_futures_info_exchange WHERE date = @d;

-- 交易所覆盖（预期 6 家）
SELECT SUBSTRING_INDEX(code, '.', -1) AS ex, COUNT(*)
FROM t_futures_info_exchange WHERE date = @d
GROUP BY ex;
```

### 2.8 清理

> 部署完成后执行。

```bash
rm -rf /tmp/data-sources /tmp/${RELEASE}.tar.gz
```

---

## 3. 手动操作

### 3.1 单日测试

> 推荐方式：直接编辑 `~/.local/bin/data-sources-pipeline` 调试，再运行。

```bash
# 调试 pipeline（如注释掉 fetcher/reporter，或调整 WRITER_TABLE）
vi ~/.local/bin/data-sources-pipeline

# 全流程一键
data-sources-pipeline 20260529
```

> 如需单步调试（env 未自动载入时先手动设）：
> ```bash
> export DB_HOST="192.168.1.27" DB_USER="tools" ...  # 见 data-sources-pipeline 内 prod block
> python3 -m data_sources.fetcher run 20260529
> ```

### 3.2 回滚

```bash
# 连接 MySQL
mysql -h 192.168.1.27 -u tools -ptools0512 future_cn
```

```sql
-- 1. 确认待删除数据
SET @d = '2026-05-27';
SELECT date, COUNT(*) AS cnt
FROM t_futures_info_exchange
WHERE date = @d
GROUP BY date;

-- 2. 确认无误后删除
DELETE FROM t_futures_info_exchange WHERE date = @d;

-- 3. 验证已清空（应为 0）
SELECT COUNT(*) FROM t_futures_info_exchange WHERE date = @d;
```

> 回滚后重跑：`data-sources-pipeline $TRADE_DATE`

---

## 4. 日志

```
/home/data_ops/log/
├── DataSourcesPipeline.$TRADE_DATE.log       # 每日全流程（rutask 重定向）
├── Fetcher.$TRADE_DATE.log        # 下载
├── Writer.$TRADE_DATE.log         # 解析/写入
└── Reporter.$TRADE_DATE.log       # 报告 / 邮件
```
