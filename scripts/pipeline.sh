#!/usr/bin/env bash
# Data pipeline – works in both dev (DEV_MODE=1) and prod (pip install --user)
# Usage:
#   Production / crontab:          bash scripts/pipeline.sh [TRADE_DATE]
#   Development:          DEV_MODE=1 bash scripts/pipeline.sh [TRADE_DATE]
#
# Log output goes to stdout/stderr. crontab/rutask handles redirection:
#   rutask cron: "00 45 16 * * 1-5"

# Ensure ~/.local/bin is on PATH (crontab may lack this)
export PATH="$HOME/.local/bin:$PATH"

# Resolve project root (works if script is in <project>/scripts/)
cd "$(dirname "$0")/.." || exit 1

TRADE_DATE="${1:-$(date +%Y%m%d)}"
TRADE_DATES_FILE="data/trade_dates.txt"

# ---- 交易日判定 ----
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
if [ "$CURRENT_HM" -lt 1645 ]; then
    echo "[$(date '+%F %T')] 当前 ${CURRENT_HM} < 1645，跳过 pipeline"
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

# ---- 环境自适应 ----
if [ -n "${DEV_MODE}" ]; then
    # 开发模式
    export DB_HOST="192.168.1.202"
    export DB_USER="root"
    export DB_PASSWORD="root0808"
    export DB_DATABASE="future_cn"
    # export DB_HOST="192.168.1.27"
    # export DB_USER="tools"
    # export DB_PASSWORD="tools0512"
    # export DB_DATABASE="future_cn"
    PIP="$(dirname "$0")/../.venv/bin/python3"
    export PYTHONPATH="src:libs/mxz-utils/src"
    export LD_LIBRARY_PATH="$HOME/.local/chrome-libs"
    RUN_FETCHER="${PIP} -m data_sources.fetcher run ${TRADE_DATE}"
    RUN_WRITER="${PIP} -m data_sources.writer --date ${TRADE_DATE} ${WRITER_OPTS}"
    RUN_REPORTER="${PIP} -m data_sources.reporter ${TRADE_DATE}"
else
    # 生产模式：pip install --user
    export DB_HOST="192.168.1.27"
    export DB_USER="tools"
    export DB_PASSWORD="tools0512"
    export DB_DATABASE="future_cn"
    RUN_FETCHER="fetcher run ${TRADE_DATE}"
    RUN_WRITER="writer --date ${TRADE_DATE}${WRITER_OPTS}"
    RUN_REPORTER="reporter ${TRADE_DATE}"
fi

# ---- 密码与收件人 ----
SMTP_PASSWORD="NBnDH84qZHZWFrTC"
export SMTP_PASSWORD
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
