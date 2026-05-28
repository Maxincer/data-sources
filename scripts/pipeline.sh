#!/usr/bin/env bash
# Data pipeline – works in both dev (DEV_MODE=1) and prod.
# Usage:
#  Production:                 bash data-sources/scripts/pipeline.sh [TRADE_DATE]
#  Development:                DEV_MODE=1 bash scripts/pipeline.sh [TRADE_DATE]
#
#  Rutask: cwd=/home/data_ops, cmd=bash data-sources/scripts/pipeline.sh
#  Cron:   00 40 16 * * 1-5

# Resolve project root (works if script is in <project>/scripts/)
cd "$(dirname "$0")/.." || exit 1

TRADE_DATE="${1:-$(date +%Y%m%d)}"

# ---- 环境自适应 ----
if [ -n "${DEV_MODE}" ]; then
    # 开发模式
    export DB_HOST="192.168.1.202"
    export DB_USER="root"
    export DB_PASSWORD="root0808"
    export DB_DATABASE="future_cn"

    # 比对库 (旧表 t_futures_info on 1.27)
    export REF_DB_HOST="192.168.1.27"
    export REF_DB_USER="tools"
    export REF_DB_PASSWORD="tools0512"
    PIP="$(dirname "$0")/../.venv/bin/python3"
    export PYTHONPATH="${PWD}/src:${PWD}/libs/mxz-utils/src"
    export DATA_DIR="./data"
    export LOG_DIR="./logs"
    export LD_LIBRARY_PATH="$HOME/.local/chrome-libs"
    RUN_FETCHER="${PIP} -m data_sources.fetcher run ${TRADE_DATE}"
    RUN_WRITER="${PIP} -m data_sources.writer --date ${TRADE_DATE} ${WRITER_OPTS}"
    RUN_REPORTER="${PIP} -m data_sources.reporter ${TRADE_DATE}"
else
    # 生产模式：系统 Python 3.10
    export DB_HOST="192.168.1.27"
    export DB_USER="tools"
    export DB_PASSWORD="tools0512"
    export DB_DATABASE="future_cn"
    export DATA_DIR="/data2_backup/data_sources_data"
    export LOG_DIR="/home/data_ops/log"
    export PLAYWRIGHT_BROWSERS_PATH="/home/data_ops/playwright-browsers"
    # 生产不设 REF_DB_*，比对走主库
    RUN_FETCHER="python3 -m data_sources.fetcher run ${TRADE_DATE}"
    RUN_WRITER="python3 -m data_sources.writer --date ${TRADE_DATE} ${WRITER_OPTS}"
    RUN_REPORTER="python3 -m data_sources.reporter ${TRADE_DATE}"
fi

# ---- 交易日判定 ----
TRADE_DATES_FILE="${DATA_DIR}/trade_dates.txt"
if [ -f "${TRADE_DATES_FILE}" ]; then
    if ! grep -qx "${TRADE_DATE}" "${TRADE_DATES_FILE}" 2>/dev/null; then
        echo "[$(date '+%F %T')] ${TRADE_DATE} 非交易日，跳过 pipeline"
        exit 0
    fi
else
    echo "[$(date '+%F %T')] ⚠ ${TRADE_DATES_FILE} 不存在，强制运行"
fi

# ---- 时间门禁 ----
CURRENT_HM=$(date +%H%M)
if [ "$CURRENT_HM" -lt 1640 ]; then
    echo "[$(date '+%F %T')] 当前 ${CURRENT_HM} < 1640，跳过 pipeline"
    exit 0
fi

# ---- 阶段配置（阶段一切换阶段二时修改此处）----
WRITER_TABLE="t_futures_info_exchange"
# WRITER_TABLE="t_futures_info"  # 阶段二
WRITER_OPTS="--table $WRITER_TABLE"

# 阶段二需设 SKIP_TABLE_COMPARE=1
# SKIP_TABLE_COMPARE=1
REPORTER_OPTS=""
[ -n "${SKIP_TABLE_COMPARE:-}" ] && REPORTER_OPTS="--skip-table-compare"

# ---- 密码与收件人 ----
SMTP_PASSWORD="NBnDH84qZHZWFrTC"
export SMTP_PASSWORD
export FEISHU_WEBHOOK="https://open.feishu.cn/open-apis/bot/v2/hook/7f1c49ef-6e6b-4c19-8152-8e25dfb8d688"
export SMTP_HOST="smtp.exmail.qq.com"
export SMTP_PORT="465"
export WIND_RPC_HOST="192.168.2.9"
export WIND_RPC_PORT="3801"
export DCE_API_KEY="ofxc69rpmd59"
export DCE_API_SECRET="2UdFW^2G4!4^7#@URqWx"
SENDER="robot@wendao.fund"
#RECIPIENTS="fisher@wendao.fund,chendingzhong@wendao.fund,mxz@wendao.fund"
RECIPIENTS="mxz@wendao.fund"

# ---- 执行管线 ----
echo "[$(date '+%F %T')] ===== pipeline started for ${TRADE_DATE} ====="

echo "[$(date '+%F %T')] Step 1/3: Fetching..."
${RUN_FETCHER} 2>&1 || echo "[WARN] Fetcher completed with errors"

echo "[$(date '+%F %T')] Step 2/3: Writing..."
${RUN_WRITER} 2>&1 || echo "[WARN] Writer completed with errors"

echo "[$(date '+%F %T')] Step 3/3: Reporting + email..."
${RUN_REPORTER} --sender "${SENDER}" --recipients "${RECIPIENTS}" ${REPORTER_OPTS} 2>&1

STATUS=$?
echo "[$(date '+%F %T')] ===== pipeline finished (status=${STATUS}) ====="
