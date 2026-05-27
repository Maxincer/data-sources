# data-sources 阶段一部署手册

> 目标：db201 部署 data-sources，写入 `future_cn.t_futures_info_exchange`，与 Wind 并行运行。
> 执行用户：data_ops

---

## 1. 环境信息

| 项目 | 值 |
|------|-----|
| 目标服务器 | db201（192.168.1.201） |
| 运行用户 | `data_ops` |
| 项目路径 | `/home/data_ops/data-sources/` |
| MySQL | `192.168.1.27:3306`，用户 `tools`，密码 `tools0512` |
| 写入表 | `future_cn.t_futures_info_exchange` |
| 调度 | rutask，工作日 16:45 |
| rutask Web UI | `http://192.168.1.201:8877` |

---

## 2. 前置检查（在 db201）

```bash
ssh data_ops@192.168.1.201

# 环境检查
python3 --version          # → 3.10.12
python3 -m pip --version   # 有输出
python3 -c "import cpp_py" # 无报错
sudo -v                    # 有 sudo 权限
id data_ops                # 用户存在

# MySQL 连通性
mysql -h 192.168.1.27 -u tools -ptools0512 -e "SELECT 1"
```

如果 `pip` 缺失：
```bash
sudo apt install python3-pip python3-venv -y
```

---

## 3. 传输离线包（从 WenDaoInvPC-200）

```bash
cd /mnt/e/projects/data-sources

# 构建 whl
python3 -m build --wheel
cp dist/data_sources-0.1.0-py3-none-any.whl scripts/

# 打包
tar czf /tmp/data-sources-data.tar.gz data/
tar czf /tmp/data-sources-offline.tar.gz deploy/offline/

# 上传到 server202
scp /tmp/data-sources-data.tar.gz     mini_apps@192.168.1.202:/tmp/
scp /tmp/data-sources-offline.tar.gz  mini_apps@192.168.1.202:/tmp/
scp -r scripts/                       mini_apps@192.168.1.202:/tmp/data-sources-scripts/

# server202 → db201
ssh mini_apps@192.168.1.202 "
  scp /tmp/data-sources-data.tar.gz     data_ops@192.168.1.201:/tmp/ &&
  scp /tmp/data-sources-offline.tar.gz  data_ops@192.168.1.201:/tmp/ &&
  scp -r /tmp/data-sources-scripts/     data_ops@192.168.1.201:/tmp/
"
```

---

## 4. 安装（在 db201）

```bash
sudo su - data_ops

mkdir -p ~/data-sources/{deploy/offline,log}
cd ~/data-sources

# 解压
tar xzf /tmp/data-sources-offline.tar.gz -C deploy/offline/ --strip-components=2
tar xzf /tmp/data-sources-data.tar.gz
cp /tmp/data-sources-scripts/* scripts/

# 安装 Chromium 系统依赖
sudo dpkg -i deploy/offline/chromium-deps/*.deb
sudo apt install -f -y

# 安装 data-sources
bash scripts/deploy.sh

# 验证
fetcher --help
writer --help
reporter --help
```

---

## 5. 配置 rutask

在 rutask 任务文件中新增：

```json5
data_sources_exchange: {
  start_cron: "00 45 16 * * 1-5",
  cmd: "bash scripts/pipeline.sh",
  cwd: "/home/data_ops/data-sources",
  output: "/home/data_ops/data-sources/log/$datetime.log",
  safe_start: true,
  life_time: 7200,
}
```

不要修改 `update_if_exclude_wsi`（Wind 任务保持运行）。

---

## 6. 手动首跑

```bash
cd ~/data-sources
bash scripts/pipeline.sh 20260523
tail -50 log/20260523*.log
```

---

## 7. 验证

```bash
mysql -h 192.168.1.27 -u tools -ptools0512 future_cn

-- 表存在
SHOW TABLES LIKE 't_futures_info_exchange';

-- 测试日行数（预期 400~450）
SELECT COUNT(*) FROM t_futures_info_exchange WHERE date='20260523';

-- 交易所覆盖（预期 6 家）
SELECT LEFT(code, LENGTH(code)-4) as ex, COUNT(*) as cnt
FROM t_futures_info_exchange WHERE date='20260523'
GROUP BY ex ORDER BY cnt DESC;

-- 关键字段无 NULL（预期 0）
SELECT COUNT(*) FROM t_futures_info_exchange
WHERE date='20260523' AND (open IS NULL OR close IS NULL);

-- 与 Wind 表行数对比
SELECT 'ours' as src, COUNT(*) FROM t_futures_info_exchange WHERE date='20260523'
UNION ALL
SELECT 'wind', COUNT(*) FROM t_futures_info WHERE date='20260523';
```
