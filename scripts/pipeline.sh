#!/usr/bin/env bash
# Data pipeline: fetch → parse/write → report → email
# Runs via crontab at 16:30 every trading day

cd /mnt/e/projects/data-sources || exit 1

TRADE_DATE="${1:-$(date +%Y%m%d)}"
LOG_FILE="${HOME}/logs/cron.log"
SCRIPT_NAME="pipeline.sh"

# 判定是否为交易日
TRADE_DATES_FILE="data/trade_dates.txt"
if [ -f "${TRADE_DATES_FILE}" ]; then
    if ! grep -qx "${TRADE_DATE}" "${TRADE_DATES_FILE}" 2>/dev/null; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${TRADE_DATE} 非交易日，跳过 pipeline" >> "${LOG_FILE}"
        exit 0
    fi
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ⚠ ${TRADE_DATES_FILE} 不存在，强制运行" >> "${LOG_FILE}"
fi

# 时间判定：16:27 前不执行（确保数据源已发布）
CURRENT_HM=$(date '+%H%M')
if [ "$CURRENT_HM" -lt 1627 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 当前 ${CURRENT_HM} < 1627，跳过 pipeline" >> "${LOG_FILE}"
    exit 0
fi

# SMTP 密码（企业微信邮箱 mxz@wendao.fund）
SMTP_PASSWORD="reSZ2qAaKiAgyu5Q"
export SMTP_PASSWORD

{
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ===== ${SCRIPT_NAME} started for ${TRADE_DATE} ====="
    
    # Step 1: Fetch raw data
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Step 1/3: Fetching raw data..."
    PYTHONPATH=src:libs/mxz-utils/src python3 -m data_sources.fetcher run "${TRADE_DATE}" 2>&1 || \
        echo "[WARN] Fetcher completed with some errors"
    
    # Step 2: Parse and write to DB
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Step 2/3: Parsing and writing to database..."
    PYTHONPATH=src python3 -m data_sources.writer --date "${TRADE_DATE}" 2>&1 || \
        echo "[WARN] Writer completed with some errors"
    
    # Step 3: Generate Feishu report + send email
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Step 3/3: Generating report and sending..."
    PYTHONPATH=src python3 -c "
import sys; sys.path.insert(0, 'src')
from data_sources.reporter import Reporter
Reporter().generate_daily('${TRADE_DATE}', email=True)
" 2>&1
    
    STATUS=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ===== ${SCRIPT_NAME} finished (status=${STATUS}) ====="
} >> "${LOG_FILE}" 2>&1
