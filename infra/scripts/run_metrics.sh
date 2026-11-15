#!/usr/bin/env bash
# Observations of one simulator run (stored, lost, duplicates, p50/p95 latency, alerts).
#   infra/scripts/run_metrics.sh                 # latest finished run
#   infra/scripts/run_metrics.sh runs/<run_id>   # a specific run
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [ $# -ge 1 ]; then
  RUN_DIR="${1%/}"
else
  RUN_DIR="$(for r in "$REPO_ROOT"/runs/*/; do [ -f "$r/summary.json" ] && echo "$(basename "$r") ${r%/}"; done | sort | tail -n 1 | cut -d' ' -f2-)"
fi
if [ -z "$RUN_DIR" ] || [ ! -f "$RUN_DIR/summary.json" ]; then
  echo "No finished run found (runs/<run_id>/summary.json). Pass the path as an argument." >&2
  exit 1
fi
echo "using: $RUN_DIR"

PY=python
command -v python >/dev/null 2>&1 || PY=python3
"$PY" "$SCRIPT_DIR/metrics.py" "$RUN_DIR"
