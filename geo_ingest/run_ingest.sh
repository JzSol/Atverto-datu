#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_PATH="${CONFIG_PATH:-$SCRIPT_DIR/config.yaml}"

python "$SCRIPT_DIR/ingest.py" --config "$CONFIG_PATH"
