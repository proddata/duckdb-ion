#!/usr/bin/env bash
set -euo pipefail

UNITTEST_BIN="${UNITTEST_BIN:-./build/release/test/unittest}"

if [[ "$#" -lt 1 ]]; then
  echo "Usage: $0 <test-path> [test-path ...]" >&2
  exit 2
fi

for test_path in "$@"; do
  if "$UNITTEST_BIN" "$test_path"; then
    continue
  fi

  if ! command -v gdb >/dev/null 2>&1; then
    echo "gdb not available; install gdb to capture a backtrace." >&2
    exit 1
  fi

  echo "Test $test_path failed; running gdb for backtrace." >&2
  gdb --batch \
    -ex "set pagination off" \
    -ex "run" \
    -ex "bt" \
    --args "$UNITTEST_BIN" "$test_path"
  exit 1
done
