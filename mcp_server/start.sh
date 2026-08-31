#!/bin/bash
# Wrapper script for LaunchAgent — sources .env before starting the server.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Source .env file
if [[ -f "$SCRIPT_DIR/.env" ]]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi

# mise manages uv/python — activate it so shims are on PATH
eval "$(/opt/homebrew/bin/mise activate bash)"

exec uv run --directory "$SCRIPT_DIR" --project . main.py --config config/config-local.yaml
