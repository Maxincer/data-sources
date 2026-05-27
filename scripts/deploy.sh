#!/usr/bin/env bash
# deploy.sh — Offline install/upgrade data-sources on db201
#
# Prerequisites:
#   - System Python 3.10 (built-in on Ubuntu 22.04)
#   - python3-pip and python3-venv installed
#   - cpp_py already installed system-wide
#   - sudo access for chromium-deps .deb installation
#
# Directory layout (relative to this script):
#   ../deploy/offline/
#     ├── pip-deps/        # 26 .whl files (80MB)
#     ├── playwright/      # chromium-1208 + headless shell (616MB)
#     ├── chromium-deps/   # 77 .deb system libs (18MB)
#     └── sys-libs/        # additional system libs
#
# Usage:
#   bash deploy.sh [--skip-chromium-deps]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OFFLINE_DIR="${SCRIPT_DIR}/../deploy/offline"
SKIP_CHROMIUM_DEPS="${1:-}"

# ---- Check Python ----
PYTHON="python3"
if ! command -v "${PYTHON}" &>/dev/null; then
    echo "[deploy] ❌ python3 not found"
    exit 1
fi
echo "[deploy] 🐍 $(${PYTHON} --version)"

# ---- Check pip ----
if ! ${PYTHON} -m pip --version &>/dev/null; then
    echo "[deploy] ❌ pip not available. Install python3-pip first:"
    echo "         sudo dpkg -i ${OFFLINE_DIR}/sys-libs/python3-pip*.deb || sudo apt install python3-pip"
    exit 1
fi

# ---- Chromium system deps ----
if [ "${SKIP_CHROMIUM_DEPS}" != "--skip-chromium-deps" ]; then
    DEPS_DIR="${OFFLINE_DIR}/chromium-deps"
    if [ -d "${DEPS_DIR}" ] && ls "${DEPS_DIR}"/*.deb &>/dev/null 2>&1; then
        echo "[deploy] 📦 Installing chromium system dependencies..."
        sudo dpkg -i "${DEPS_DIR}"/*.deb 2>/dev/null || {
            echo "[deploy] ⚠️  Some debs need apt --fix-broken, running apt install -f..."
            sudo apt install -f -y
            sudo dpkg -i "${DEPS_DIR}"/*.deb
        }
        echo "[deploy] ✅ Chromium deps installed"
    else
        echo "[deploy] ⚠️  chromium-deps/ not found, skipping (use --skip-chromium-deps to suppress)"
    fi
fi

# ---- Locate whl ----
WHL=$(ls -1t "${SCRIPT_DIR}"/data-sources-*.whl 2>/dev/null | head -1)
if [ -z "${WHL}" ]; then
    echo "[deploy] ❌ No data-sources-*.whl found in ${SCRIPT_DIR}"
    exit 1
fi
echo "[deploy] 📦 Installing: $(basename "${WHL}")"

# ---- Offline pip install ----
PIP_DEPS_DIR="${OFFLINE_DIR}/pip-deps"
if [ -d "${PIP_DEPS_DIR}" ]; then
    echo "[deploy] 📦 Installing from offline wheels..."
    ${PYTHON} -m pip install --user --no-index --find-links="${PIP_DEPS_DIR}" --force-reinstall "${WHL}"
else
    echo "[deploy] ⚠️  pip-deps/ not found, falling back to online install"
    ${PYTHON} -m pip install --user --force-reinstall "${WHL}"
fi

# ---- Playwright browser (offline) ----
PLAYWRIGHT_DIR="${OFFLINE_DIR}/playwright"
if [ -d "${PLAYWRIGHT_DIR}/chromium-1208" ]; then
    echo "[deploy] 🌐 Registering bundled playwright chromium..."
    # Set PLAYWRIGHT_BROWSERS_PATH so playwright finds our bundled browser
    export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_DIR}"
    # Symlink into standard cache so 'playwright install' sees it as installed
    PLAYWRIGHT_CACHE="${HOME}/.cache/ms-playwright"
    mkdir -p "${PLAYWRIGHT_CACHE}"
    for browser in chromium-1208 chromium_headless_shell-1208; do
        if [ -d "${PLAYWRIGHT_DIR}/${browser}" ] && [ ! -e "${PLAYWRIGHT_CACHE}/${browser}" ]; then
            ln -s "${PLAYWRIGHT_DIR}/${browser}" "${PLAYWRIGHT_CACHE}/${browser}"
            echo "  ✅ ${browser} → ~/.cache/ms-playwright/"
        fi
    done
else
    echo "[deploy] ⚠️  playwright/ not found, skipping chromium registration"
fi

# ---- Verify ----
echo "[deploy] 🔍 Verifying commands..."
for cmd in fetcher writer reporter; do
    if command -v "${cmd}" &>/dev/null; then
        echo "  ✅ ${cmd}"
    else
        echo "  ❌ ${cmd} not found (check ~/.local/bin/ is in PATH)"
    fi
done

echo "[deploy] ✅ Deploy complete: $(basename "${WHL}")"

# ---- Print env setup hint ----
if [ -d "${PLAYWRIGHT_DIR:-}" ]; then
    echo ""
    echo "💡 Add to pipeline.sh or crontab:"
    echo "   export PLAYWRIGHT_BROWSERS_PATH=${PLAYWRIGHT_DIR}"
fi
