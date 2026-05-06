#!/usr/bin/env bash
# Daily data pipeline: fetch → parse/write → report
# Runs via crontab at 16:30 every trading day

cd /mnt/e/projects/data-sources || exit 1

TRADE_DATE="${1:-$(date +%Y%m%d)}"
LOG_FILE="${HOME}/logs/cron.log"
SCRIPT_NAME="daily_pipeline.sh"

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
    
    # Step 3: Generate and send report
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Step 3/3: Generating verification report..."
    PYTHONPATH=src python3 -m data_sources.reporter "${TRADE_DATE}" 2>&1
    
    STATUS=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ===== ${SCRIPT_NAME} finished (status=${STATUS}) ====="
} >> "${LOG_FILE}" 2>&1
