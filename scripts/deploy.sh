#!/usr/bin/env bash
# deploy.sh — Install/upgrade data-sources from local .whl
#
# Installs to user site-packages (~/.local/). Scripts land in ~/.local/bin/.
# cpp_py is available because it's installed in the system Python.
#
# Usage:
#   bash deploy.sh
#   (Place data-sources-*.whl in the same directory)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ---- Locate whl ----
WHL=$(ls -1t "${SCRIPT_DIR}"/data-sources-*.whl 2>/dev/null | head -1)

if [ -z "${WHL}" ]; then
    echo "[deploy] ❌ No data-sources-*.whl found"
    exit 1
fi
echo "[deploy] 📦 Installing: $(basename "${WHL}")"

# ---- Install / upgrade ----
# --user: installs to ~/.local/, cpp_py is in system Python's path
pip install --user --force-reinstall "${WHL}"

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
