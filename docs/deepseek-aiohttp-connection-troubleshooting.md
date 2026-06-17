# DeepSeek aiohttp 连接超时问题排查记录

> 2026-06-16 · `analyse_announcements_service` · WenDaoInvPC-200 (WSL2)

---

## 问题现象

`analyse_announcements_service` 调用 DeepSeek API 时出现大量连接错误：

- **60%+** 请求返回 `Connection timeout`（连接 `api.deepseek.com` 超时）
- 连锁产生 `Broken pipe` / `Server disconnected`（复用已关闭的空闲连接时）
- 少量 `401 Authorization Required`（限流触发）

但 `curl api.deepseek.com` 直连仅需 **0.19s**，响应正常。

---

## 根因

### 主因：WSL2 IPv6 路由缺失 + aiohttp Happy Eyeballs 策略

```
WSL2 环境：
  - IPv6 socket: available（socket.AF_INET6 可用）
  - IPv6 路由: 无（操作系统没有 IPv6 默认网关）
  - DNS 解析: 返回 AAAA（IPv6）+ A（IPv4）双记录
```

aiohttp 遵循 [RFC 6555 Happy Eyeballs](https://datatracker.ietf.org/doc/html/rfc6555)：

1. 先尝试 IPv6 连接 → 30s 无响应才超时
2. 超时后 fallback 到 IPv4 → 连接成功（0.2s）
3. 每次请求都重复这个流程

**结论**：700 条公告 × 每连接浪费 30s = 灾难。

`curl` 不受影响是因为 libcurl 在 WSL2 上默认直接走 IPv4（DNS 解析顺序不同）。

### 次要因素：Windows 代理关闭空闲连接

在去掉 `force_close=True`、启用连接池复用时：
- 请求复用了连接池中的 **keep-alive 连接**
- 但 Windows 上的代理（Clash/V2Ray TUN 模式）关闭了空闲连接
- 后续请求往已关闭的 socket 写数据 → `Broken pipe`

回退 `force_close=True` 后不再复用连接，此现象消失。

---

## 排查过程

### 阶段一：定位到 event loop 崩溃（无关，但修了）

| 发现 | 修复 |
|------|------|
| `_do_retry()` 内嵌 `asyncio.run()` 导致 Semaphore/Lock 绑在已销毁的 loop | 重试合并进 `_main()` 的单 event loop |
| `async with sem:` 在 `try` 块之外，错误不被捕获 | 移入 try 块 |
| `async with lock` 在 `except` 块外，同款隐患 | 去掉锁（单线程不需要） |

### 阶段二：定位到连接超时

- 加了 `DS_REQ` / `DS_RESP` / `DS_RAW` 三级日志，区分"没发请求"、"发了没回"、"回了但解析失败"
- 发现 694/700 条请求都是 `Connection timeout`
- 尝试 `force_close=True` vs 连接池复用，反复摇摆

| 尝试 | 结果 |
|------|------|
| `force_close=True`（原始） | 每条请求独立建连，慢但稳定 |
| 去掉 `force_close=True` | 多 `Broken pipe`（代理关空闲连接），更差 |
| 加大 timeout `total=600, connect=30, sock_read=300` | 无效 |
| 回退 `force_close=True` | 回归原始状态 |

### 阶段三：发现 IPv6 根因（最终解决）

- `curl` 秒回 vs aiohttp 超时 → 排查差异
- 发现 WSL2 无 IPv6 路由，aiohttp 默认尝试 IPv6

最终修复：

```python
conn = aiohttp.TCPConnector(family=socket.AF_INET, force_close=True)
```

- `family=socket.AF_INET`：跳过 IPv6，直接建立 IPv4 TCP 连接
- `force_close=True`：不复用连接池（避免代理侧关空闲连接导致的 Broken pipe）

---

## 修复清单（2026-06-16）

| # | 问题 | 严重程度 | 修复 |
|---|------|----------|------|
| 1 | IPv6 无路由，aiohttp Happy Eyeballs 空等 30s | 🔴 致命 | `TCPConnector(family=AF_INET)` 强制 IPv4 |
| 2 | 代理关闭空闲 keep-alive 连接 → Broken pipe | 🔴 严重 | 保留 `force_close=True` |
| 3 | retry 跨 event loop 崩溃 | 🔴 致命 | 重试并入单 event loop |
| 4 | `async with sem/lock` 在 try 外 | 🔴 致命 | 移入 try |
| 5 | Exception 没有 full traceback | 🟡 降低可调试性 | 加 `exc_info=True` |
| 6 | 无法区分"没发请求"和"发了没回" | 🟡 降低可调试性 | 加 `DS_REQ` / `DS_RESP` 日志 |
| 7 | timeouts 未显式配置 connect/sock_read | 🟢 可优化 | `ClientTimeout(total=600, connect=30, sock_read=300)` |

---

## 教训

1. **不要假设 aiohttp 的 DNS/连接策略与 curl 一致**。Happy Eyeballs 在双栈环境下可能导致巨大的连接延迟。
2. **WSL2 的 IPv6 陷阱**：socket available ≠ 路由可达，必须在 WSL2 上显式验证 IPv6 连通性。
3. **`force_close=True` 的取舍**：连接复用提高效率但引入连接池污染风险。在外部网络环境（代理、防火墙）不可控时，每次独立建连更安全。
4. **日志分层**：请求前（DS_REQ）、请求完成（DS_RESP）、解析完成（DS_RAW）三级日志大大缩短了排查周期。

---

## 验证

当前配置已通过实测验证：`analyse_announcements_service` 在 `family=AF_INET` + `force_close=True` 配置下运行正常。
