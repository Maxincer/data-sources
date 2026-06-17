# 项目运维手册

## 1. DCE CMS API 间歇性 403

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

## 2. announcements_metadata.json 生命周期

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

## 3. 交易所列表页链接失效

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
