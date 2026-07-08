# data-sources 阶段二部署手册

## 0. 本次部署改动

1. 切换生产表：从 `t_futures_info_exchange` → `t_futures_info`
2. 增加验收机制
3. 自动熔断回滚：验收不通过时自动删除数据 + 回退到 Wind

## 1. 架构变更

### 阶段一（当前）

```
update_if_exclude_wsi.sh
  ├── t_futures.py              → t_futures
  ├── t_futures_info.py         → t_futures_info          # Wind 原始脚本
  ├── t_main_contracts.py       → t_main_contracts
  └── t_futures_tradable.py     → t_futures_tradable

data_sources_pipeline（独立调度 16:40）
  ├── 采集/分析公告
  ├── fetcher → data/raw/
  └── writer  → t_futures_info_exchange （过渡表）
```

### 阶段二（切换后）

```
update_if_exclude_wsi.sh（调度方式不变）
  ├── t_futures.py              → t_futures               # 不变
  ├── data_sources_pipeline     → t_futures_info           # ← 替换 t_futures_info.py
  │     ├── 采集/分析公告
  │     ├── fetcher → data/raw/
  │     ├── writer  → t_futures_info（生产表！）
  │     ├── reporter —skip-table-compare（exchange vs WSS 验证）
  │     │     ├── 验收通过 ✅ → exit(0)
  │     │     └── 验收不通过 🔄 → exit(2) → DELETE 当日行 → 飞书告警
  │     └── pipeline 退出
  ├── [回滚时] t_futures_info.py                           # ← 仅 pipeline 非零时执行
  ├── t_main_contracts.py       → t_main_contracts         # 不变
  └── t_futures_tradable.py     → t_futures_tradable       # 不变
```

### 核心变化

| 项目 | 阶段一 | 阶段二 |
|------|--------|--------|
| 写入表 | `t_futures_info_exchange` | `t_futures_info` |
| reporter 参数 | 两表对比 + WSS | 仅 WSS（`--skip-table-compare`） |
| update_if_exclude_wsi.sh | 直接执行 t_futures_info.py | 调用 data_sources_pipeline，失败时回退 |
| 数据源 | 独立过渡表，与 Wind 并行 | 替换 Wind，成为生产数据唯一入口 |

---

## 2. 前置条件

### 2.1 阶段一稳定运行

阶段一已稳定运行 ___ 个交易日，每日 ~869 行，无异常退出。

确认方法：

```bash
# 查询 exchange 表数据量
mysql -h 192.168.1.27 -u tools -ptools0512 future_cn -e '
  SELECT date, COUNT(*) FROM t_futures_info_exchange
  GROUP BY date ORDER BY date DESC LIMIT 10;
'

# 查看 pipeline 日志
ls -lt /home/data_ops/log/DataSourcesPipeline.*.log | head -5
tail -5 /home/data_ops/log/DataSourcesPipeline.$(date +%Y%m%d).log
```

### 2.2 代码已部署

阶段二代码在 `feat/rollback` 分支（commit `65ea3dd`），已合入 `main`。需更新部署：

```bash
# 在 db201 上
RELEASE="..."  # 对应打包版本

wget -O /tmp/${RELEASE}.tar.gz http://releases.corp.wendao.fund/data-sources/${RELEASE}.tar.gz
mkdir -p /tmp/data-sources && cd /tmp/data-sources && tar -xzf /tmp/${RELEASE}.tar.gz

pip3 install --user --no-index --upgrade \
  --find-links=/tmp/data-sources/deploy/offline/pip-deps \
  data_sources

rm -rf /tmp/data-sources /tmp/${RELEASE}.tar.gz
```

---

## 3. 切换步骤

### 3.1 修改 rutask 配置

**`/home/data_ops/rustask/rustask.local.json5`**

```json5
data_sources_pipeline: {
  "start_cron": "00 40 16 * * 1-5",
  "cmd": "/home/data_ops/.local/bin/data_sources_pipeline",
  "output": ">>/home/data_ops/log/DataSourcesPipeline.$date.log",
  "safe_start": true,
  "hide_log": false,
  "env": {
    "SKIP_TABLE_COMPARE": "1"
  }
}
```

重载：

```bash
curl http://192.168.1.201:8877/reload
# 或
rustask --sock cmd-rustask.sock reload
```

### 3.2 修改 update_if_exclude_wsi.sh

**`/home/data_ops/data-crawler/bash_script/update_if_exclude_wsi.sh`**

```diff
 cd ${self_dir}/../future_cn/src
 run_pyrun -e ~/bin/reporterr.py -o ~/log/%n-%D-%T.log -3 t_futures.py
-run_pyrun -e ~/bin/reporterr.py -o ~/log/%n-%D-%T.log -3 t_futures_info.py
+
+# data_sources_pipeline 替代 t_futures_info.py
+bash ~/.local/bin/data_sources_pipeline
+PIPELINE_EXIT=$?
+if [ $PIPELINE_EXIT -ne 0 ]; then
+    # data-sources 验收不通过，回退到 Wind 原始脚本
+    run_pyrun -e ~/bin/reporterr.py -o ~/log/%n-%D-%T.log -3 t_futures_info.py
+fi
+
 run_pyrun -e ~/bin/reporterr.py -o ~/log/%n-%D-%T.log -3 t_main_contracts.py
 run_pyrun -e ~/bin/reporterr.py -o ~/log/%n-%D-%T.log -3 t_futures_tradable.py
```

### 3.3 首次测试

选择一个非交易日手动测试：

```bash
# 手动触发（跳过交易日判定和时间门禁）
TRADE_DATE="20260708"
cd /home/data_ops && \
  SKIP_TABLE_COMPARE=1 \
  bash ~/.local/bin/data_sources_pipeline $TRADE_DATE
```

确认 pipeline 有完整输出后，检查数据：

```bash
# 检查写入行数
mysql -h 192.168.1.27 -u tools -ptools0512 future_cn -e "
  SELECT COUNT(*) FROM t_futures_info WHERE date='20260708';
"

# 抽样检查数据质量
mysql -h 192.168.1.27 -u tools -ptools0512 future_cn -e "
  SELECT code, open, high, low, close, volume, oi
  FROM t_futures_info WHERE date='20260708' LIMIT 10;
"
```

---

## 4. 验收与回滚机制

### 4.1 验收条件

reporter 将 exchange 源数据与 WSS 数据逐合约逐字段对比，判定规则（定义于 `verifier.py` + `_evaluate_rollback()`）：

| 条件 | 定义 | 回滚？ |
|------|------|--------|
| **a** | exchange=空、WSS≠空 | ✅ **回滚**（TAS 合约豁免） |
| **b1** | 行情字段（open/high/low/close/settle/maxup/maxdown）非空但不精确相等 | ✅ **回滚** |
| **b2** | volume/amt/oi 比例偏差 \|a-b\|/\|b\| > 0.001 | ✅ **回滚** |
| **b2** | if_basis 绝对值偏差 \|a-b\| ≥ 0.001 | ✅ **回滚** |
| **c** | exchange≠空、WSS=空 | ❌ 不触发（exchange 比 WSS 多数据是正常的） |

### 4.2 自动回滚流程

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

### 4.3 回滚报告

触发回滚时 reporter 会发送飞书消息：

```
🔄 回滚触发 20260708
回滚原因：
[a] open：5 条 exchange=空 WSS≠空
[b1] close：3 条数值不一致
[b2] volume：2 条超 0.1% 阈值不一致
```

---

## 5. 回滚

### 5.1 自动回滚（首次失败）

如果阶段二切换后第一个交易日 pipeline 验收不通过，会自动执行：

```sql
DELETE FROM t_futures_info WHERE date = '${TRADE_DATE}';
```

然后 Wind 的 `t_futures_info.py` 会用 `INSERT IGNORE` 补回当日数据（与阶段一单日回滚相同，无需手动操作）。

### 5.2 手动回滚（连续失败/异常）

如果连续多日触发自动回滚，需手动排查：

```bash
# 1. 暂时切回阶段一：恢复 t_futures_info.py 的独立运行
#    修改 update_if_exclude_wsi.sh，恢复原始 t_futures_info.py 调用

# 2. 分析 reporter 日志中的差异明细
less /home/data_ops/log/DataSourcesPipeline.$(date +%Y%m%d).log

# 3. 修复后，重新执行阶段二切换步骤
```

### 5.3 完全回退（恢复阶段一状态）

```bash
# 1. 改回 update_if_exclude_wsi.sh（删除 data_sources_pipeline 调用，恢复 t_futures_info.py）
# 2. rutask 取消 env SKIP_TABLE_COMPARE（或删掉）
# 3. 确认 data_sources_pipeline 的 WRITER_TABLE 仍为 t_futures_info_exchange
# 4. 清理 t_futures_info 中 data-sources 写入的数据（如有）
mysql -h 192.168.1.27 -u tools -ptools0512 future_cn -e "
  DELETE FROM t_futures_info WHERE date >= '20260708';
"
```

---

## 6. 监控清单

切换后第一个交易日需检查：

| 检查项 | 方法 | 预期 |
|--------|------|------|
| pipeline 退出码 | `grep "pipeline finished" log/DataSourcesPipeline.*.log` | status=0 |
| 数据行数 | `SELECT date, COUNT(*) FROM t_futures_info GROUP BY date` | ~869/日 |
| 飞书报告 | 检查飞书群的消息 | ✅ 或 🔄（回滚时） |
| Wind 日志 | 检查 `t_futures_info.py` 是否执行 | 回滚时应有日志 |
