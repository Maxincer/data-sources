#!/usr/bin/env bash
# ================================================================
# update_future_cn_dot_t_futures_info.sh
#
# Daily data pipeline: download → parse → write → alert
# Scheduled via crontab at 16:30 (after market close).
#
# Exports env vars for cron (no login shell).
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd 2>/dev/null || echo "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Working directory: $(pwd)"

# ------------------------------------------------------------------
# 1. Environment (exported for cron, no login shell)
# ------------------------------------------------------------------
export FEISHU_ALERT_URL="https://open.feishu.cn/open-apis/bot/v2/hook/7f1c49ef-6e6b-4c19-8152-8e25dfb8d688"
export DCE_API_KEY="ofxc69rpmd59"
export DCE_API_SECRET="2UdFW^2G4!4^7#@URqWx"
export TZ="Asia/Shanghai"

# ------------------------------------------------------------------
# 2. Python / venv
# ------------------------------------------------------------------
VENV="$PROJECT_ROOT/.venv"
if [ -d "$VENV" ]; then
    PYTHON="$VENV/bin/python3"
else
    PYTHON="python3"
fi

if ! $PYTHON -m data_sources.fetcher --help >/dev/null 2>&1; then
    echo "[ERROR] data_sources.fetcher not importable. Run: pip install -e $PROJECT_ROOT"
    exit 1
fi

# ------------------------------------------------------------------
# 3. Download today's raw data from all exchanges
# ------------------------------------------------------------------
TRADE_DATE=$(date '+%Y%m%d')
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Fetching data for $TRADE_DATE ..."

$PYTHON -c "
from data_sources.fetcher import Fetcher
import sys

f = Fetcher()
try:
    f.run('$TRADE_DATE')
    print('Download complete.')
except Exception as e:
    print(f'FATAL: {e}')
    sys.exit(1)
"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Raw data download finished."

# ------------------------------------------------------------------
# 4. Parse → Write to DB
# ------------------------------------------------------------------
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Parsing and writing to DB ..."
$PYTHON -m data_sources.writer --date "$TRADE_DATE" 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] DB write complete."

# ------------------------------------------------------------------
# 5. Verify (field comparison) and alert
# ------------------------------------------------------------------
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running field comparison ..."
$PYTHON -c "
from data_sources.verifier import Verifier
from mxz_utils.logging_config import get_logger

logger = get_logger('Verifier', 'INFO', './logs', 'Verifier')
v = Verifier(logger)
result = v.compare_fields()

from data_sources.reporter import verifier_card
verifier_card(logger, '$TRADE_DATE', result)
" 2>&1 || true

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Pipeline complete."
