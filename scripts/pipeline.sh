#!/usr/bin/env bash
# Data pipeline: fetch → parse/write → report → email
# Runs via crontab at 16:30 every trading day
# SMTP_PASSWORD must be set in environment for email delivery

cd /mnt/e/projects/data-sources || exit 1

# 确保 SMTP_PASSWORD 可用
if [ -z "${SMTP_PASSWORD}" ] && [ -f "${HOME}/.bashrc" ]; then
    source "${HOME}/.bashrc" 2>/dev/null
    export SMTP_PASSWORD
fi

# 若仍为空，从 bashrc 中直接提取
if [ -z "${SMTP_PASSWORD}" ]; then
    SMTP_PASSWORD=$(grep '^export SMTP_PASSWORD=' "${HOME}/.bashrc" 2>/dev/null | head -1 | cut -d'=' -f2- | tr -d '"')
    export SMTP_PASSWORD
fi

TRADE_DATE="${1:-$(date +%Y%m%d)}"
LOG_FILE="${HOME}/logs/cron.log"
SCRIPT_NAME="pipeline.sh"

{
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ===== ${SCRIPT_NAME} started for ${TRADE_DATE} ====="
    
    # Step 1: Fetch raw data
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Step 1/4: Fetching raw data..."
    PYTHONPATH=src:libs/mxz-utils/src python3 -m data_sources.fetcher run "${TRADE_DATE}" 2>&1 || \
        echo "[WARN] Fetcher completed with some errors"
    
    # Step 2: Parse and write to DB
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Step 2/4: Parsing and writing to database..."
    PYTHONPATH=src python3 -m data_sources.writer --date "${TRADE_DATE}" 2>&1 || \
        echo "[WARN] Writer completed with some errors"
    
    # Step 3: Generate and send Feishu report
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Step 3/4: Generating verification report..."
    PYTHONPATH=src python3 -m data_sources.reporter "${TRADE_DATE}" 2>&1
    
    # Step 4: Send email report
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Step 4/4: Sending email report..."
    PYTHONPATH=src python3 -c "
import sys; sys.path.insert(0, 'src')
from data_sources.reporter import send_email_report
send_email_report('${TRADE_DATE}')
" 2>&1
    
    STATUS=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ===== ${SCRIPT_NAME} finished (status=${STATUS}) ====="
} >> "${LOG_FILE}" 2>&1
