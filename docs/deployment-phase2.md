# Deployment Guide: data-sources

## 0. 本次部署改动

1. 切换生产表：从 `t_futures_info_exchange` → `t_futures_info`（阶段二）
2. 增加验收机制（DEV=1，与`t_futures_info`比较；DEV!=1，与wss比较）
3. 自动熔断回滚：验收不通过时自动删除当日数据
4. 完善反爬探测机制
5. 本次为升级部署：v0.1.1 -> v0.1.2

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
| 可执行文件 | `~/.local/bin/data-sources-pipeline` |
| site-packages | `~/.local/lib/python3.10/site-packages/data_sources/` |

## 2. 部署步骤

### 2.1 下载与解压

```bash
RELEASE="20260623-3f27876"

wget -O /tmp/${RELEASE}.tar.gz http://releases.corp.wendao.fund/data-sources/${RELEASE}.tar.gz

mkdir -p /tmp/data-sources && cd /tmp/data-sources && tar -xzf /tmp/${RELEASE}.tar.gz
```





### 自动回滚

```
data_sources_pipeline Step 5: reporter —skip-table-compare
  │
  ├── 验收通过 ✅ → exit(0)
  │      └── pipeline exit(0) → update_if_exclude_wsi.sh 继续执行后续脚本
  │
  └── 验收不通过 🔄 → reporter sys.exit(2)
         │
         ├── pipeline 捕获 STATUS=2
         │     └── DELETE FROM t_futures_info WHERE date=$TRADE_DATE
         │
         ├── pipeline exit(2)
         │
         └── update_if_exclude_wsi.sh 检测 $? != 0
               └── 执行 t_futures_info.py（Wind 回退）
```
