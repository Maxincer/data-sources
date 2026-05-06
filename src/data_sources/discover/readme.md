# 数据源发现 (Discovery)

本目录包含各交易所缺失字段的数据源探索脚本。

## 缺失字段

| 字段 | 说明 | 可能的来源 |
|------|------|-----------|
| maxup | 涨停价 | 交易所交易参数表(涨跌停板幅度) |
| maxdown | 跌停价 | 同上 |
| if_basis | 交割月标志 | 合约月份 vs 当前月份 |
| minoq | 最小下单量 | 各交易所合约规则(静态配置) |
| maxoq | 最大下单量 | 各交易所合约规则(静态配置) |

## 运行方法

```bash
cd /mnt/e/projects/data-sources
python3 -m data_sources.discover.<name>
```
