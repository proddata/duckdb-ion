#!/usr/bin/env bash
set -euo pipefail

DUCKDB_BIN="${DUCKDB_BIN:-./build/release/duckdb}"
DATA_DIR="${DATA_DIR:-perf/data}"
OUT_DIR="${OUT_DIR:-perf/mem}"
RUNS="${RUNS:-3}"

ION_FILE="$DATA_DIR/data.ion"
ION_BINARY_FILE="$DATA_DIR/data_binary.ion"
ION_WIDE_FILE="$DATA_DIR/data_wide.ion"
ION_WIDE_BINARY_FILE="$DATA_DIR/data_wide_binary.ion"

mkdir -p "$OUT_DIR"

time_cmd=()
os_name="$(uname -s | tr '[:upper:]' '[:lower:]')"
if [[ "$os_name" == "darwin" ]]; then
  time_cmd=(/usr/bin/time -l)
else
  time_cmd=(/usr/bin/time -v)
fi

require_file() {
  if [[ ! -f "$1" ]]; then
    echo "Missing input file: $1" >&2
    exit 1
  fi
}

require_file "$ION_FILE"
require_file "$ION_WIDE_FILE"

has_binary=1
if [[ ! -f "$ION_BINARY_FILE" || ! -f "$ION_WIDE_BINARY_FILE" ]]; then
  echo "Binary Ion inputs missing; skipping binary mem checks." >&2
  has_binary=0
fi

run_case() {
  local label="$1"
  local sql="$2"
  local log_path
  log_path="$OUT_DIR/${label}.log"
  : > "$log_path"

  echo "== $label ==" | tee -a "$log_path"
  for run in $(seq 1 "$RUNS"); do
    echo "-- run $run" | tee -a "$log_path"
    "${time_cmd[@]}" "$DUCKDB_BIN" -c "$sql" 2>>"$log_path" 1>/dev/null || true
  done
}

run_case "read_count_text" \
  "LOAD ion; SELECT COUNT(*) FROM read_ion('$ION_FILE');"
if [[ "$has_binary" -eq 1 ]]; then
  run_case "read_count_binary" \
    "LOAD ion; SELECT COUNT(*) FROM read_ion('$ION_BINARY_FILE');"
fi
run_case "read_project_text" \
  "LOAD ion; SELECT id, category, amount::DOUBLE FROM read_ion('$ION_FILE');"
if [[ "$has_binary" -eq 1 ]]; then
  run_case "read_project_binary" \
    "LOAD ion; SELECT id, category, amount::DOUBLE FROM read_ion('$ION_BINARY_FILE');"
fi
run_case "read_wide_text" \
  "LOAD ion; SELECT id, w_int_00, w_str_00, w_dec_00 FROM read_ion('$ION_WIDE_FILE');"
if [[ "$has_binary" -eq 1 ]]; then
  run_case "read_wide_binary" \
    "LOAD ion; SELECT id, w_int_00, w_str_00, w_dec_00 FROM read_ion('$ION_WIDE_BINARY_FILE');"
fi

summary_path="$OUT_DIR/summary.txt"
: > "$summary_path"
for log in "$OUT_DIR"/*.log; do
  name="$(basename "$log" .log)"
  if [[ "$os_name" == "darwin" ]]; then
    rss_bytes="$(awk '/maximum resident set size/ {print $1}' "$log" | sort -n | tail -1)"
    if [[ -n "$rss_bytes" ]]; then
      rss_mb=$((rss_bytes / 1024 / 1024))
      echo "$name max_rss_mb=$rss_mb" >> "$summary_path"
    else
      echo "$name max_rss_mb=unknown" >> "$summary_path"
    fi
  else
    rss_kb="$(awk -F: '/Maximum resident set size/ {gsub(/ /, "", $2); print $2}' "$log" | sort -n | tail -1)"
    if [[ -n "$rss_kb" ]]; then
      rss_mb=$((rss_kb / 1024))
      echo "$name max_rss_mb=$rss_mb" >> "$summary_path"
    else
      echo "$name max_rss_mb=unknown" >> "$summary_path"
    fi
  fi
done

echo "Wrote logs to $OUT_DIR"
echo "Wrote summary to $summary_path"
