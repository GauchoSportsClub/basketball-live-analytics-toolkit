#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

WEB_DIR="apps/web"
MANIFEST_FILE="$WEB_DIR/package.json"
LOCK_FILE="$WEB_DIR/package-lock.json"
STAMP_FILE="$WEB_DIR/node_modules/.install-state"

if [ ! -d "$WEB_DIR" ]; then
  echo "Error: expected frontend at $WEB_DIR/."
  exit 1
fi

if ! command -v npm >/dev/null 2>&1; then
  echo "Error: npm not found in PATH."
  exit 1
fi

if [ -f "$LOCK_FILE" ]; then
  INSTALL_CMD=(npm --prefix "$WEB_DIR" ci)
  CURRENT_STATE="$(shasum -a 256 "$MANIFEST_FILE" "$LOCK_FILE" | awk '{print $1}' | tr -d '\n')"
else
  INSTALL_CMD=(npm --prefix "$WEB_DIR" install)
  CURRENT_STATE="$(shasum -a 256 "$MANIFEST_FILE" | awk '{print $1}')"
fi

INSTALLED_STATE=""
if [ -f "$STAMP_FILE" ]; then
  INSTALLED_STATE="$(cat "$STAMP_FILE")"
fi

NEEDS_INSTALL=0
if [ ! -d "$WEB_DIR/node_modules" ]; then
  NEEDS_INSTALL=1
fi
if [ "$CURRENT_STATE" != "$INSTALLED_STATE" ]; then
  NEEDS_INSTALL=1
fi

if [ "$NEEDS_INSTALL" = "1" ]; then
  echo "Installing web dependencies in $WEB_DIR/ ..."
  "${INSTALL_CMD[@]}"
  mkdir -p "$WEB_DIR/node_modules"
  echo "$CURRENT_STATE" > "$STAMP_FILE"
fi
