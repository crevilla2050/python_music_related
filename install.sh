#!/usr/bin/env bash
set -e

echo "=== Pedro Organiza CLI Installer ==="

# 1. Python check
if ! command -v python3 >/dev/null 2>&1; then
  echo "[ERROR] python3 not found."
  echo "Please install Python 3.9+ and re-run this script."
  exit 1
fi

PY_VER=$(python3 - <<EOF
import sys
print(".".join(map(str, sys.version_info[:2])))
EOF
)

echo "[OK] Found Python $PY_VER"

# 2. venv
if [ ! -d "venv" ]; then
  echo "[INFO] Creating virtual environment..."
  python3 -m venv venv
fi

# 3. Activate
source venv/bin/activate

# 4. Upgrade pip
pip install --upgrade pip setuptools wheel

# 5. Install Pedro
pip install .

echo ""
echo "=== Installation complete ==="
echo ""
echo "Activate with:"
echo "  source venv/bin/activate"
echo ""
echo "Try:"
echo "  pedro status"
