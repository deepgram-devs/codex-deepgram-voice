#!/usr/bin/env bash
# Start Codex CLI with its voice endpoints pointed at the local Flux shim.
# No edits to ~/.codex/config.toml: the two URL keys are passed as -c overrides,
# which Codex accepts (only project-local .codex/config.toml refuses them).
# Run the shim first in another terminal:  . .venv/bin/activate && python -m shim.server -v
set -euo pipefail
SHIM_HOST="${SHIM_HOST:-127.0.0.1}"
SHIM_PORT="${SHIM_PORT:-8765}"
if ! nc -z "$SHIM_HOST" "$SHIM_PORT" 2>/dev/null; then
  echo "shim is not listening on $SHIM_HOST:$SHIM_PORT; start it first:" >&2
  echo "  . .venv/bin/activate && python -m shim.server -v" >&2
  exit 1
fi
exec codex \
  -c "experimental_realtime_webrtc_call_base_url=\"http://$SHIM_HOST:$SHIM_PORT/v1\"" \
  -c "experimental_realtime_ws_base_url=\"ws://$SHIM_HOST:$SHIM_PORT/v1\"" \
  "$@"
