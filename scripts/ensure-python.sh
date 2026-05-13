#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

VENV_PYTHON=".venv/bin/python"
REQ_FILE="requirements.txt"
STAMP_FILE=".venv/.requirements.sha256"

PYTHON_BIN=""
if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Error: python not found in PATH."
  exit 1
fi

if [ ! -x "$VENV_PYTHON" ]; then
  echo "Creating Python venv at .venv/ ..."
  "$PYTHON_BIN" -m venv .venv
fi

if [ ! -f "$REQ_FILE" ]; then
  echo "Error: $REQ_FILE not found (expected at repo root)."
  exit 1
fi

CURRENT_HASH="$($VENV_PYTHON -c "import hashlib, pathlib; print(hashlib.sha256(pathlib.Path('$REQ_FILE').read_bytes()).hexdigest())")"
INSTALLED_HASH=""
if [ -f "$STAMP_FILE" ]; then
  INSTALLED_HASH="$(cat "$STAMP_FILE")"
fi

if [ "$CURRENT_HASH" != "$INSTALLED_HASH" ]; then
  echo "Installing Python dependencies from $REQ_FILE ..."
  "$VENV_PYTHON" -m pip install --upgrade pip >/dev/null
  "$VENV_PYTHON" -m pip install -r "$REQ_FILE"
  echo "$CURRENT_HASH" > "$STAMP_FILE"
fi
