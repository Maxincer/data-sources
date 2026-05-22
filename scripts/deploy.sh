#!/usr/bin/env bash
# deploy.sh — Install/upgrade data-sources from local .whl
#
# Prerequisites:
#   pipx must be installed (pip install pipx)
#
# Usage:
#   Place deploy.sh and data-sources-*.whl in the same directory.
#   bash deploy.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ---- Locate whl ----
WHL=$(ls -1t "${SCRIPT_DIR}"/data-sources-*.whl 2>/dev/null | head -1)

if [ -z "${WHL}" ]; then
    echo "[deploy] ❌ No data-sources-*.whl found next to deploy.sh"
    exit 1
fi
echo "[deploy] 📦 Installing: $(basename "${WHL}")"

# ---- Pre-check ----
if ! command -v pipx &>/dev/null; then
    echo "[deploy] ❌ pipx not found. Install: pip install pipx"
    exit 1
fi

# ---- Install / upgrade (force overwrites existing) ----
# --system-site-packages: allow access to system-installed packages
# (cpp_py.so, etc.) while keeping pip deps isolated in the venv
pipx install --system-site-packages --force "${WHL}"

# ---- Playwright browser (chromium only) ----
echo "[deploy] 🌐 Installing playwright chromium..."
playwright install chromium

# ---- Verify ----
echo "[deploy] 🔍 Verifying commands..."
for cmd in fetcher writer reporter; do
    if command -v "${cmd}" &>/dev/null; then
        echo "  ✅ ${cmd}"
    else
        echo "  ❌ ${cmd} not found"
    fi
done

echo "[deploy] ✅ Deploy complete: $(basename "${WHL}")"
