#!/usr/bin/env bash
export FEISHU_ALERT_URL="https://open.feishu.cn/open-apis/bot/v2/hook/7f1c49ef-6e6b-4c19-8152-8e25dfb8d688"
export DCE_API_KEY='ofxc69rpmd59'
export DCE_API_SECRET='2UdFW^2G4!4^7#@URqWx'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd 2>/dev/null || echo "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"
python -m data_sources.fetcher run --trade_date=20260427
