#!/bin/bash
# Starts the Claude Code → Anthropic API proxy.
# Maintains a warm Claude Code session. Zero file system permissions.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Ensure CLAUDECODE is unset so claude -p can spawn
unset CLAUDECODE

exec "$SCRIPT_DIR/.venv/bin/python3" "$SCRIPT_DIR/proxy.py"
