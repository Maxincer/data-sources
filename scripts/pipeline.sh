#!/usr/bin/env bash
# Data pipeline – works in both dev (DEV_MODE=1) and prod (pipx installed)
# Usage:
#   Production / crontab: bash scripts/pipeline.sh [TRADE_DATE]
#   Development:          DEV_MODE=1 bash scripts/pipeline.sh [TRADE_DATE]

# Ensure pipx-installed commands are found (crontab may lack this)
export PATH="$HOME/.local/bin:$PATH"

# Resolve project root (works if script is in <project>/scripts/)
cd "$(dirname "$0")/.." || exit 1

TRADE_DATE="${1:-$(date +%Y%m%d)}"
LOG_FILE="${LOG_FILE:-${HOME}/logs/cron.log}"
TRADE_DATES_FILE="data/trade_dates.txt"

# ---- 交易日判定 ----
if [ -f "${TRADE_DATES_FILE}" ]; then
    if ! grep -qx "${TRADE_DATE}" "${TRADE_DATES_FILE}" 2>/dev/null; then
        echo "[$(date '+%F %T')] ${TRADE_DATE} 非交易日，跳过 pipeline" >> "${LOG_FILE}"
        exit 0
    fi
else
    echo "[$(date '+%F %T')] ⚠ ${TRADE_DATES_FILE} 不存在，强制运行" >> "${LOG_FILE}"
fi

# ---- 时间门禁 ----
CURRENT_HM=$(date +%H%M)
if [ "$CURRENT_HM" -lt 1627 ]; then
    echo "[$(date '+%F %T')] 当前 ${CURRENT_HM} < 1627，跳过 pipeline" >> "${LOG_FILE}"
    exit 0
fi

# ---- 环境自适应 ----
if [ -n "${DEV_MODE}" ]; then
    # 开发模式：强制使用本地源码（即使已安装）
    export PYTHONPATH="src:libs/mxz-utils/src"
    RUN_FETCHER="python3 -m data_sources.fetcher run ${TRADE_DATE}"
    RUN_WRITER="python3 -m data_sources.writer --date ${TRADE_DATE}"
    RUN_REPORTER="python3 -m data_sources.reporter ${TRADE_DATE}"
elif command -v fetcher &> /dev/null; then
    # 生产模式：通过 pipx 安装的命令已在 PATH
    RUN_FETCHER="fetcher run ${TRADE_DATE}"
    RUN_WRITER="writer --date ${TRADE_DATE}"
    RUN_REPORTER="reporter ${TRADE_DATE}"
else
    # 后备开发模式：未安装时自动使用源码
    export PYTHONPATH="src:libs/mxz-utils/src"
    RUN_FETCHER="python3 -m data_sources.fetcher run ${TRADE_DATE}"
    RUN_WRITER="python3 -m data_sources.writer --date ${TRADE_DATE}"
    RUN_REPORTER="python3 -m data_sources.reporter ${TRADE_DATE}"
fi

# ---- 密码与收件人 ----
SMTP_PASSWORD="reSZ2qAaKiAgyu5Q"
export SMTP_PASSWORD
SENDER="mxz@wendao.fund"
RECIPIENTS="fisher@wendao.fund,chendingzhong@wendao.fund"

# ---- 执行管线 ----
{
    echo "[$(date '+%F %T')] ===== pipeline started for ${TRADE_DATE} ====="

    echo "[$(date '+%F %T')] Step 1/3: Fetching..."
    ${RUN_FETCHER} 2>&1 || echo "[WARN] Fetcher completed with errors"

    echo "[$(date '+%F %T')] Step 2/3: Writing..."
    ${RUN_WRITER} 2>&1 || echo "[WARN] Writer completed with errors"

    echo "[$(date '+%F %T')] Step 3/3: Reporting + email..."
    ${RUN_REPORTER} --sender "${SENDER}" --recipients "${RECIPIENTS}" 2>&1

    STATUS=$?
    echo "[$(date '+%F %T')] ===== pipeline finished (status=${STATUS}) ====="
} >> "${LOG_FILE}" 2>&1
