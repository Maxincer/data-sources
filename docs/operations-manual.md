# 项目运维手册

## 1. 环境变量配置

### 数据库连接

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `DB_HOST` | 主库 Host | `192.168.1.202`（DEV）/ `192.168.1.27`（PROD） |
| `DB_USER` | 主库用户 | `root`（DEV）/ `tools`（PROD） |
| `DB_PASSWORD` | 主库密码 | — |
| `DB_DATABASE` | 主库名称 | `future_cn` |
| `REF_DB_HOST` | 比对库 Host（t_futures_info on Wind） | `192.168.1.27` |
| `REF_DB_USER` | 比对库用户 | `tools` |
| `REF_DB_PASSWORD` | 比对库密码 | — |
| `REF_DB_DATABASE` | 比对库名称 | `future_cn` |

### 运行模式

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `DEV` | 开发模式（1=启用）：加载 .venv、跳过 WSS 验证、使用 dev DB | `0` |
| `SKIP_TABLE_COMPARE` | 跳过两表对比（1=启用），生产模式自动设 `1` | `0`（DEV）/ `1`（PROD） |
| `DAILY_START_DATE` | 历史数据起始日期 | `20251201` |

### 飞书告警

| 变量 | 说明 |
|------|------|
| `FEISHU_WEBHOOK` | 飞书机器人 Webhook URL |

### 邮件发送

| 变量 | 说明 |
|------|------|
| `SMTP_HOST` | SMTP 服务器地址 |
| `SMTP_PORT` | SMTP 端口 |
| `SMTP_PASSWORD` | SMTP 密码 |
| `SENDER` | 发件人邮箱 |
| `RECIPIENTS` | 收件人列表（逗号分隔） |

### 数据与日志

| 变量 | 说明 | 默认值（DEV） |
|------|------|-------------|
| `DATA_DIR` | 原始数据存放目录 | `<项目根目录>/data` |
| `LOG_DIR` | 日志输出目录 | `<项目根目录>/logs` |

### 交易所 API

| 变量 | 说明 |
|------|------|
| `DCE_API_KEY` | DCE CMS API Key |
| `DCE_API_SECRET` | DCE CMS API Secret |

### 行情数据 API

| 变量 | 说明 |
|------|------|
| `TUSHARE_TOKEN` | Tushare API Token |

### AI 模型 API

| 变量 | 说明 |
|------|------|
| `ZHIPU_API_KEY` | 智谱 GLM API Key（公告解析/修正用） |
| `DEEPSEEK_API_KEY` | DeepSeek API Key（数据解析/修正用） |

### 搜索 API

| 变量 | 说明 |
|------|------|
| `TAVILY_API_KEY` | Tavily 搜索 API Key（公告采集用） |

### 运行环境

| 变量 | 说明 |
|------|------|
| `LD_LIBRARY_PATH` | Playwright Chromium 系统库路径（含 libnspr4.so 等） |

> **注**：上述变量在 启动脚本`data_sources_pipeline` 中按 DEV/PROD 模式自动设默认值，
> 部署人员/运维人员可通过该脚本直接读写
> 部署人员/运维人员可通过 rutask 的 `env` 字段覆写。

## 2. 启动/中止方式

- 启动方式：执行启动脚本`data_sources_pipeline`
- 启动脚本路径可从部署文档获取
- 中止方式：kill 命令即可 
- 终止方式：run once脚本，自动终止

## 3. DCE CMS API 间歇性 403

### 现象
- **2026-06-10**：Fetcher 调用 DCE token 端点 (`/dceapi/cms/auth/accessToken`) 时返回 HTTP 403，两轮重试均失败
- 错误信息：`系统暂时无法提供服务，请稍后再试!`（DCE 应用层拒绝，非 WAF 拦截）

### 根因
DCE CMS API 对请求频率敏感，短时间内多次 token 请求会触发限流。Fetcher 每轮运行 3 个 DCE 任务（Settlement / Market / TradingParameters），各任务独立调用 `_get_dce_token()`，导致 3 次 token 请求在数秒内发出。

### 修复（2026-06-10）
- `fetch.py`：`_get_dce_token()` 增加模块级缓存（30 分钟 TTL），同一轮 3 个任务共享 1 个 token
- 同时增加 `Content-Type: application/json` 和 `User-Agent` 请求头（对齐 `collect_announcements_service` 的可用实现）

### 排查方法
```bash
# 手动测试 token 端点
curl -X POST http://www.dce.com.cn/dceapi/cms/auth/accessToken \
  -H "apikey: $DCE_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"secret\": \"$DCE_API_SECRET\"}"

# 查看 Fetcher 日志中 DCE 相关错误
grep "DCE\|403\|token" logs/Fetcher.$(date +%Y%m%d).log | tail -20
```

**注意：403 与 DCE 凭证（apikey/secret）无关。** 凭证未过期或失效，同一套凭证在手动 curl 测试和 `collect_announcements_service` 中均可正常签发 token。403 完全由请求频率触发 DCE 服务端限流导致。

- `collect_announcements_service` 也已同步增加 token 缓存（同 30 分钟 TTL）
- 不同进程（Fetcher vs Announcements Service）的缓存独立，不会共享 token

---

## 4. announcements_metadata.json 生命周期

### 写入

`upsert_record(key, record)` — 仅新增或覆盖指定 key 的记录。不会删除任何已有条目。

```python
# collect_announcements_service.py
meta[key] = record  # 只增改, 不删
```

### 删除

**自动：永远不会被删除。** 代码中无任何 `del meta[key]` 或 `meta.pop()` 操作。

以下操作不会影响 metadata：
- `clean_orphans(meta)` — 仅删除磁盘上不在 metadata 中的孤儿 **文件**，不修改 metadata
- `_check_download()` 失败 — 跳过写入，不产生新条目，但不影响已有条目

---

## 5. 交易所列表页链接失效

### 现象（2026-06-11）
SHFE 品种细则列表页 (`productrules/`) 中提取到的两个老链接返回 404：

```
SHFE_20240903_802267 | 石油沥青期货业务细则
SHFE_20240903_802266 | 丁二烯橡胶期货业务细则
```

### 根因
交易所网站偶尔会在不更新列表页的情况下删除或移动详情页内容。链接仍挂在列表页中，但服务端已返回 404 页面。

### 当前处理
`_check_download()` 在下载后检查 `<title>` 中的 404，拦截后跳过（不写文件、不写 metadata）。

### 影响
- 每次采集运行都会尝试下载这些链接，每次都判定为 404
- 在 `[下载验证]` 日志中持续出现，属于无害噪声

### 未来改进方向
- 在 Parse Service 中识别 404 条目后，可标记为 `status: dead` 写入 metadata，后续采集跳过
- 或手动清理 metadata 中对应的 URL 来源记录（需确认该条目的来源 URL 后移除）

---

## 6. DCE 新上市合约无前日交易参数

### 现象
Writer 日志中出现以下 warning：

```
DCE BZ2706.DCE 无前日交易参数，涨跌停/保证金置空
DCE EB2706.DCE 无前日交易参数，涨跌停/保证金置空
DCE EG2706.DCE 无前日交易参数，涨跌停/保证金置空
DCE JD2706.DCE 无前日交易参数，涨跌停/保证金置空
DCE PG2706.DCE 无前日交易参数，涨跌停/保证金置空
```

对应数据验证报告中合约的 `open=—, close=—, settle=—` 标记。

### 根因
DCE 涨跌停价修正逻辑（`fix_dce_limit_prices`）使用**开盘限价口径**：需用前一交易日的 DCE API 原始值来替代当日值。当一个合约首次出现在 DCE 交易参数 API 中时（即只有 1 天数据），无法进行此替换，因此将 maxup/maxdown/margin 置空。

### 判断标准
新合约即将上市的典型特征：
| 指标 | 正常合约 | 新上市合约 |
|------|---------|-----------|
| `selfTotBuyPosiQuota` | > 0 | **0.0** |
| `riseLimitRate` | 6-9% | **12-14%**（首日涨跌停幅度） |
| 历史天数 | ≥ 2 天 | **1 天**（首次出现） |

### 发生场景
DCE 现有品种（EB、EG、JD、PG 等）在远月合约进入合约链时，或新品种（如 BZ/纯苯）新增合约月时，会在 API 中预先注册该合约。该合约通常在**下一个交易日**正式挂盘上市。

- 示例：2026-06-25 出现的 BZ2706/EB2706/EG2706/JD2706/PG2706 → **2026-06-26 上市**
- 此后第二个交易日开始，前一日的交易参数会出现在 API 中，涨跌停/保证金恢复正常计算

### 处理方案
**无需处理，属于正常行为。** 代码逻辑是正确的：
1. `fix_dce_limit_prices` 检测到只有 1 天数据 → 置空并告警（由 `modifier.py` 负责）
2. 次日该合约有前日数据后，涨跌停/保证金自动恢复
3. 数据验证报告中的 `—` 标记反映了真实的合约状态（上市首日无前收盘价）

### 排查方法
```bash
# 检查 DCE 交易参数数据中的新合约特征
python3 -c "
import json
with open('data/raw/structured/2026MMDD.DCE.TradingParameters.json') as f:
    data = json.load(f)
# 查找持仓限额为 0 的合约
for item in data['data']:
    if item.get('selfTotBuyPosiQuota', -1) == 0:
        print(f\"新合约: {item['contractId']}, "
              f\"涨跌停率: {item.get('riseLimitRate')}, "
              f\"持仓限额: {item.get('selfTotBuyPosiQuota')}\")
"

# 查看 Writer 日志中的无前日警告
grep "无前日" logs/Writer.*.log | tail -10
```

---

## 7. Writer "无前日保证金数据" 告警

### 现象
Writer 日志中出现以下 warning：

```
.CZC ZC2607.CZC 无前日保证金数据，long_margin/short_margin 置空
.CZC ZC2707.CZC 无前日保证金数据，long_margin/short_margin 置空
```

### 根因
writer 只加载当日和前一日的原始数据文件，`_fix_margin_inherit` 按合约代码分组后检查记录数，只有 1 条记录时无前日可继承，将 margin 置 NULL 并告警。

两种场景触发：

| 场景 | 合约 | 原因 | 最终影响 |
|------|------|------|---------|
| **已到期合约** | ZC2607.CZC（ZC607） | 今日已摘牌，仅前日文件有数据 | margin 置 NULL → 日期过滤后排除，**最终表不包含** |
| **新上市合约** | ZC2707.CZC（ZC707） | 今日首次出现，无前日记录 | margin 置 NULL → **写入表时 margin 为空** |

### 判断标准
- 已到期合约：`date_str` 文件中不存在该合约，`prev_date` 文件中存在
- 新上市合约：`date_str` 文件中首次出现，`prev_date` 文件中不存在

### 处理方案
**无需处理，属于正常行为。**
1. `_fix_margin_inherit` 检测到只有 1 天数据 → 置空并告警
2. 已到期合约被 `target_records` 过滤，不会写入数据库
3. 新上市合约的 margin 会在第二个交易日自动恢复（届时有两日数据可继承）

### 排查方法
```bash
# 查看原始数据中合约的存在性
grep "^ZC607\|^ZC707" data/raw/structured/${date_str}.CZCE.*.txt

# 查看 Writer 日志中的无前日警告
grep "无前日" logs/Writer.${date_str}.log
```
