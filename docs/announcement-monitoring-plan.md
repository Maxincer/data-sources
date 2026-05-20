# 交易所公告监控方案

## 1. 需要监控的字段（我们自行计算的）

| 交易所 | 自行计算的字段 | 计算方式 | 依赖的原始数据 |
|--------|--------------|---------|--------------|
| **INE/SHFE** | maxup, maxdown | floor(前结算 × (1±幅度) / tick) × tick | 前结算(DailyMarketData) + 幅度(TradingParameters) + tick(产品配置HTML) |
| **CZCE** | maxup, maxdown | ceil/floor(前结算 × (1±幅度) / tick) × tick | 同上，但取整规则不同 |
| **DCE** | (无) | 直接从TradingParameters读取 | — |
| **CFFEX** | (无) | 直接从结算CSV读取 | — |
| **GFEX** | (无) | 直接从TradingParameters读取 | — |

**关键风险**: 只有 INE/SHFE 和 CZCE 的涨跌停是计算出来的，其余交易所直接读取 API。

## 2. 公告页面可访问性测试结果

| 交易所 | 列表页 | 状态 | 单页(已知URL) | 说明 |
|--------|-------|------|-------------|------|
| INE | `/publicnotice/` | 🟡 JS渲染 | ✅ 可达 | 列表页需JS，单页直接访问 |
| SHFE | `/publicnotice/` | 🟡 JS渲染 | ✅ 可达 | 同上，同一套CMS |
| DCE | `/dce/channel/list/244.html` | ❌ 412 | ? | 被CDN拦截 |
| CZCE | 公告列表页 | ❌ 412 | ? | 被CDN拦截 |
| CFFEX | `/cn/jysgg.html` | 🟡 JS渲染 | ? | 需进一步测试 |
| GFEX | `/gfex/tzts/list_yw.shtml` | 🟡 JS渲染 | ? | 同CFFEX |

## 3. 监控方案

```
搜索(web_search) → 发现公告URL → 获取单页内容 → LLM分析 → 有影响则飞书告警
                ↑                    ↑
         搜索引擎索引的        直接HTTP可达的
         公告列表              静态HTML
```

### 工作流程

```python
def run_watch():
    # Step 1: 对各交易所搜索近期公告
    queries = [
        "上海国际能源交易中心 公告 site:ine.cn 涨跌停|保证金|最小变动",
        "上海期货交易所 公告 site:shfe.com.cn",
        "大连商品交易所 调整 涨跌停板 site:dce.com.cn",
        "郑州商品交易所 调整 site:czce.com.cn",
        "中国金融期货交易所 公告 site:cffex.com.cn",
        "广州期货交易所 公告 site:gfex.com.cn",
    ]
    for q in queries:
        results = web_search(q, days_back=30)
        announcements.extend(results)
    
    # Step 2: 对每个公告，获取全文
    for url in announcement_urls:
        text = fetch_text(url)  # 直接HTTP GET
    
    # Step 3: 构造LLM prompt
    prompt = f"""
    我们是期货数据系统，以下字段是自行计算的：
    - INE/SHFE maxup/maxdown: floor(前结算×(1±幅度)/tick)×tick
    - CZCE maxup/maxdown: ceil/floor(前结算×(1±幅度)/tick)×tick
    
    请分析以下公告是否会改变上述计算逻辑中的任意输入参数:
    {announcement_texts}
    """
    
    # Step 4: 交给LLM
    result = llm_analyze(prompt)
    if result.has_impact:
        feishu_alert(result.summary)
```

### 依赖的外部工具

| 工具 | 用途 | 频率 |
|------|------|------|
| web_search | 发现新公告URL | 每日 |
| HTTP GET | 获取公告全文 | 每次发现新公告时 |
| LLM | 分析公告对算法的潜在影响 | 每次发现新公告时 |
| 飞书Webhook | 告警 | 有影响时 |
