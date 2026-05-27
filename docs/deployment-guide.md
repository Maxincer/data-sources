# data-sources 阶段一部署手册

> 将 6 家交易所行情数据写入过渡表 `t_futures_info_exchange`，与 Wind 并行运行。

---

## 1. 环境

| 项目 | db201（生产） | server202（测试） |
|------|:-----------:|:-------------:|
| 用户 | `data_ops` | `mini_apps` |
| 项目路径 | `/home/data_ops/data-sources/` | `~/data-sources/` |
| MySQL | `192.168.1.27` | `192.168.1.202`（本地） |
| 写入表 | `t_futures_info_exchange` | 同上 |
| 调度 | rutask `16:00` | crontab（模拟） |
| 部署方式 | `pip install --user` | 同左（生产模式） |

---

## 2. 部署步骤

### 2.1 db201 生产部署

```bash
# 1) 上环境
ssh data_ops@192.168.1.201
python3 --version              # → 3.10+
python3 -c "import cpp_py"     # 无报错

# 2) 传输离线包（从 WenDaoInvPC-200）
# 见 scripts/deploy.sh

# 3) 安装
cd ~/data-sources
bash scripts/deploy.sh

# 4) 验证
fetcher --help && writer --help && reporter --help
```

### 2.2 server202 测试部署

```bash
ssh mini_apps@192.168.1.202

# 部署（生产模式）
cd ~/data-sources
bash scripts/deploy.sh

# 首跑（指定本地 MySQL，不发送邮件）
pipeline.sh 20260527 --host 192.168.1.202 --skip-email
```

---

## 3. 调度

### 3.1 db201：rutask

`http://192.168.1.201:8877` → 配置 `deploy/rutask.json5`：

```json5
data_sources_exchange: {
  start_cron: "00 00 16 * * 1-5",       // 工作日 16:00
  cmd: "bash scripts/pipeline.sh",
  cwd: "/home/data_ops/data-sources",
  output: "/home/data_ops/data-sources/logs/$datetime.log",
  safe_start: true,
  life_time: 7200,                        // 超时 2h
}
```

### 3.2 server202：crontab 模拟

```bash
crontab -e
```

```
00 16 * * 1-5  cd ~/data-sources && pipeline.sh >> logs/pipeline.log 2>&1
```

> 测试时 pipeline.sh 会连本地 MySQL（`192.168.1.202`），不影响 db201 生产。

---

## 4. 管线命令

### 4.1 单日手动跑

```bash
cd ~/data-sources

# 全流程（fetch → write → reporter）
bash scripts/pipeline.sh 20260527

# 分步
fetcher run 20260527                      # 仅下载
writer --date 20260527 --dry-run          # 预览写入
writer --date 20260527                    # 实际写入
reporter 20260527 --sender robot@wendao.fund --recipients mxz@wendao.fund
```

### 4.2 环境变量

| 变量 | 用途 | 阶段一默认值 |
|------|------|:----:|
| `DEV_MODE` | 设为 `1` 走 `192.168.1.202` 本地 MySQL，不设走 `192.168.1.27` | 不设（生产） |
| `SKIP_TABLE_COMPARE` | 设为 `1` 跳过与 Wind `t_futures_info` 的表比对（阶段二才全跳过） | 不设（做比对） |
| `WRITER_TABLE` | 覆盖写入表名 | `t_futures_info_exchange` |
| `SMTP_PASSWORD` | 邮件发送密码 | 在 `pipeline.sh` 内 |

---

## 5. 验证

```sql
-- 行数（预期 400~450 条/日）
SELECT COUNT(*) FROM t_futures_info_exchange WHERE date = '20260527';

-- 交易所覆盖（预期 6 家）
SELECT SUBSTRING_INDEX(code, '.', -1) AS ex, COUNT(*)
FROM t_futures_info_exchange WHERE date = '20260527'
GROUP BY ex;

-- 关键字段无 NULL
SELECT COUNT(*) FROM t_futures_info_exchange
WHERE date = '20260527' AND (open IS NULL OR close IS NULL);
```

### 下单量字段

`minoq`（最小下单量）和 `maxoq`（最大下单量）来自静态配置表 `config/order_limits.csv`，数据来源及证明文件见 `config/proofs/`。交易所调整下单量规则时，需同步更新配置表。

---

## 6. 日志

```
~/data-sources/logs/
├── pipeline-20260527.log       # 每日全流程
├── Fetcher.20260527.log        # 下载（crontab 重定向）
└── reporter-20260527.log       # 报告 / 邮件
```
