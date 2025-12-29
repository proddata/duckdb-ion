#!/usr/bin/env bash
set -euo pipefail

UNITTEST_BIN="${UNITTEST_BIN:-./build/release/test/unittest}"
TEST_PATH="${1:-}"

if [[ -z "$TEST_PATH" ]]; then
  echo "Usage: $0 <test-path>" >&2
  exit 2
fi

if "$UNITTEST_BIN" "$TEST_PATH"; then
  exit 0
fi

if ! command -v gdb >/dev/null 2>&1; then
  echo "gdb not available; install gdb to capture a backtrace." >&2
  exit 1
fi

echo "Test failed; running gdb for backtrace." >&2
gdb --batch \
  -ex "set pagination off" \
  -ex "run" \
  -ex "bt" \
  --args "$UNITTEST_BIN" "$TEST_PATH"
