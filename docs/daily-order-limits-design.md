# 每日最大最小合约手数获取方案

## 1. 需求

每日获取期货各合约的 **最小开仓量（minoq）** 和 **最大下单量（maxoq）**，写入 `t_futures_info_exchange` 表。

## 2. 数据源分析

### 2.1 交易所 TradingParameters 含 minoq/maxoq

| 交易所 | 文件 | 包含 minoq/maxoq? | 当前解析状态 |
|--------|------|-------------------|-------------|
| **CZCE** | `FutureTradeParam.txt` (col 8,9) | ✅ 全合约逐级 | ✅ parser 已提取 |
| **CFFEX** | `SettlementParameters.csv` | ❌ 仅保证金 | ❌ 无此字段 |
| **DCE** | `TradingParameters.json` (dayTradPara) | ❌ 仅有 position limit | ❌ 无此字段 |
| **DCE** | `VarietyTradingParam.json` (tradingParam) **← 新增** | ✅ `maxHand` (品种级) | ✅ fetcher + parser 已添加 |
| **GFEX** | `TradingParameters.json` | ❌ 仅有 position limit | ❌ 无此字段 |
| **SHFE** | `TradingParameters.dat` | ❌ 仅 margin/limit | ❌ 无此字段 |
| **INE** | `TradingParameters.dat` | ❌ 仅 margin/limit | ❌ 无此字段 |

**结论：只有 CZCE 的日频文件包含 minoq/maxoq，其余交易所的 TradingParameters 均不含此信息。**

### 2.2 各交易所 minoq/maxoq 的性质

| 交易所 | minoq | maxoq | 变动频率 |
|--------|-------|-------|---------|
| CZCE | 来自 TradingParameters，有 1 手和 10 手两档 | 限价单 max 通常 1000，临近交割月逐级下调 | **逐合约逐日变动**（交割月前/中逐级缩窄） |
| CFFEX | 固定 1 手 | 限价单 100/200 手，市价单 50 手 | **静态**（品种级固定） |
| SHFE/INE | 固定 1 手（部分品种除外） | 品种级固定值（如 500/5000 等），临近交割月逐级下调 | **接近静态**（交割月才变） |
| DCE | 固定 1 手 | **`tradingParam` API → `maxHand`**（品种级） | **API 实时获取** |
| GFEX | 固定 1 手 | 品种级固定值 | **静态** |

## 3. 方案设计

### 3.1 分层架构

```
交易所日频文件
├── CZCE TradingParameters.txt ──→ parser 逐合约提取 minoq/maxoq
├── 其他交易所 ──→ 不提取（文件不含此字段）
│
└── modifier.inject_order_limits()
    └── 从静态 CSV 补充缺失的 minoq/maxoq
        ├── CFFEX / DCE / GFEX → 品种级静态值
        ├── SHFE / INE → 品种级静态值
        └── CZCE → 跳过（已有 parser 提取的精确值不覆盖）
```

### 3.2 当前实现

| 组件 | 实现 | 位置 |
|------|------|------|
| CZCE parser | 逐合约提取 minoq/maxoq | `parser.py:440-449` |
| 静态 CSV | 品种级配置表 | `data/order_limits.csv` |
| 静态注入 | 对缺乏 minoq/maxoq 的记录补充注入 | `modifier.py:753-776` |
| 注入触发 | writer 写入前调用 `inject_order_limits()` | `writer.py:48` |

### 3.3 CZCE 的特殊处理

CZCE 的 TradingParameters 文件在每个交易日提供**当前所有活跃合约**的 minoq/maxoq，是唯一真正"每日获取"的来源。parser 逐合约提取后进入 pipeline，**不经过静态 CSV 覆盖**（`inject_order_limits` 的 `if "minoq" not in rec` 条件天然跳过已有值）。

### 3.3 DCE maxoq 精准获取

DCE 提供 `/tradepara/tradingParam` API，返回品种级参数表，包含 `maxHand`（最大下单手数）。

| 组件 | 实现 | 位置 |
|------|------|------|
| Fetcher | `_fetch_dce_tradingparam()` → POST tradingParam | `fetcher.py` 新增 |
| Config | 注册为 DCE/VarietyTradingParam 任务 | `configs.py` 新增 |
| Parser | `_parse_dce_tradingparam()` → 提取 `maxHand` | `parser.py` 新增 |
| CSV 导入 | `import_dce_tradingparam_to_csv()` → 更新 order_limits.csv | `order_limits_updater.py` 新增 |

**文件命名：** 品种级数据文件明确命名为 `VarietyTradingParam.json`，与合约级 `TradingParameters.json` 区分。

**效果：** DCE 各品种的 maxoq 不再依赖静态 CSV，而是每天从交易所 API 实时获取。

#### `-F` 月均价合约处理 (2026-06-12)

DCE 月均价合约（如 `l2607F`）的品种 ID 为 `l-F`。`_parse_dce_tradingparam()` 解析 `VarietyTradingParam.json` 后产出 `l-F.DCE`（品种级 code），而合约级记录来自 `_parse_dce_tradepara()` 产出 `l2607f.DCE`。两者 code 不同，maxoq 无法通过 `merge_by_code_date()` 合并。

**解决：** 在 `merge_by_code_date()` 中增加 DCE 专有步骤：提取品种级记录的字母前缀（如 `lf`），按前缀向合约级记录传播 `maxoq`。

```python
# DCE 品种级 maxHand → 合约级 maxoq 传播
dce_variety: dict[str, dict] = {}
for rec in merged.values():
    ...
    if not any(c.isdigit() for c in raw):  # 品种级：无数字
        prefix = "".join(c for c in raw if c.isalpha()).upper()
        dce_variety[prefix] = dict(rec)

for rec in merged.values():
    ...
    if any(c.isdigit() for c in raw):  # 合约级：有数字
        prefix = "".join(c for c in raw if c.isalpha()).upper()
        if prefix in dce_variety:
            rec["maxoq"] = dce_variety[prefix]["maxoq"]
```

**效果：** `-F` 合约（`L2606F.DCE`、`PP2607F.DCE`、`V2607F.DCE` 等）的 maxoq 与常规合约走完全相同的流程，无需额外配置或静态 CSV 条目。

### 3.4 交割月动态调整

min/max 下单量在临近交割月时会被交易所调低，当前处理方式：

| 情形 | 处理方式 | 说明 |
|------|---------|------|
| CZCE 交割月下调 | ✅ parser 每日精确获取 | TradingParameters 会自动反映 |
| SHFE/INE 交割月下调 | ❌ 静态 CSV 无法反映 | 需要额外手段 |
| 其他所交割月下调 | ❌ 静态 CSV 无法反映 | 需要额外手段 |

**交割月相关合约范围很小（通常仅当月合约受影响），静态值的误差在可接受范围内。** 如需精确反映，有以下扩展方案（见第 5 节）。

## 4. 静态 CSV 维护

### 4.1 数据来源

| 交易所 | 来源 | URL |
|--------|------|-----|
| CFFEX | 交易细则 | http://www.cffex.com.cn/jyxx/ |
| SHFE | 业务参数 | https://www.shfe.com.cn/products/ |
| INE | 业务参数 | https://www.ine.cn/products/ |
| DCE | 交易参数 | https://www.dce.com.cn/dalianshangpin/ywfw/ |
| GFEX | 交易参数 | https://www.gfex.com.cn/gfex/ywfw/ |

### 4.2 变更监控

交易所调整 minoq/maxoq 时（比如 SHFE 针对特定品种提高最小开仓量），通过已有公告监控机制发现：

1. web_search 搜索各交易所公告
2. LLM 分析是否涉及 minoq/maxoq 变更
3. 有变更时飞书告警，提示更新 `order_limits.csv`

### 4.3 CSV 格式

```csv
exchange,variety_id,variety_name,product_type,maxoq_limit,maxoq_market,minoq,source,notes
CFFEX,IF,沪深300股指,期货,100,50,1,交易细则,限价100手 市价50手
SHFE,CU,铜,期货,500,100,1,交易细则,
...
```

**字段说明：**
- `maxoq_limit` — 限价单最大下单量
- `maxoq_market` — 市价单最大下单量
- `minoq` — 最小开仓量

## 5. 扩展方案（可选）

如需**精确反映所有交易所的每日 minoq/maxoq**（包括交割月临近时的动态下调），有以下路径：

### 方案 A: 解析交易所公告（✅ 已实现）

在 `announcement-monitoring-plan.md` 已有的公告监控基础上，增加对 minoq/maxoq 变更的解析，**自动更新静态 CSV**。

**实现：**
- `scripts/order_limits_updater.py` — 解析 LLM 输出 → 自动更新 CSV
- `scripts/announcement_watcher.py` — 集成为 Step 4.5，检测变更即触发
- 飞书告警同步展示 CSV 变更记录（标准格式）

**工作流：**
```
公告搜索 → LLM 分析 → 检测 minoq/maxoq 变更
                            ↓
                  order_limits_updater
                            ↓
                  更新 order_limits.csv
                            ↓
                  飞书告警（含变更记录 + 标准格式）
```

**自动更新效果示例：**
```
- 调整合约：MA<2026>，调整项目：最小开仓量，调整前：1手，调整后：8手，生效时间：20260520
- 调整合约：LC<2026>，调整项目：最小开仓量，调整前：5手，调整后：8手，生效时间：20260301
- 调整合约：SC<2026>，调整项目：最大下单量，调整前：500手，调整后：200手，生效时间：20260615
```

### 方案 B: 对接 Wind API

通过 Wind WSS 接口获取 `minoq`/`maxoq` 字段：

```python
# Wind WSS 支持多代码+多字段
wind.wss(codes, "minoq,maxoq", f"tradeDate={date}")
```

**成本：** 低（Wind 客户端已就绪）
**收益：** 精确逐日获取全市场
**问题：** minoq/maxoq 不是标准行情字段，Wind 可能不支持或数据不完整（verifier 中已验证此类字段时出现大量 unavailable）

### 方案 C: 爬取交易所合约规格页面

各交易所官网公布品种合约规格（含最小变动价位、下单量限制等），可通过 Playwright/HTTP 获取。

**成本：** 中（需要为每家交易所写解析器）
**收益：** 精确且最新
**问题：** 静态页面内容 = 品种级规则，非逐合约逐日变化。实际收益与静态 CSV 接近。

## 6. 推荐方案

**当前阶段采用「CZCE 日频 + 静态 CSV + 公告告警」组合：**

```
每日 pipeline 执行时：
├── CZCE 合约 → parser 逐合约精确提取 minoq/maxoq ✅
├── 其他合约 → static CSV 注入 ✅
└── 公告监控 → 发现 minoq/maxoq 变更 → 飞书告警 → 手动更新 CSV
```

**理由：**
1. minoq/maxoq 在绝大部分交易所是品种级静态参数，非逐日变动
2. CZCE 是唯一提供逐合约日频数据的交易所，已精确提取
3. 交割月下调影响面小（仅临近交割合约，通常 1-2 个合约受影响）
4. 公告监控为 CSV 维护提供安全网

## 7. 输出格式

记录每日 minoq/maxoq 变动时，按以下格式飞书输出：

```
- 调整合约：MA<2604>，调整项目：最小开仓量，调整前：1手，调整后：8手，生效时间：20260520
- 调整合约：XX<06DD>，调整项目：最大下单量，调整前：1000手，调整后：200手，生效时间：20260602
```
