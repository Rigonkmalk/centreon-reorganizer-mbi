#!/usr/bin/env bash
# Run all partition checker tests
# Usage: bash tests/run_tests.sh
#
# To regenerate expected files after code changes:
#   REGENERATE=1 bash tests/run_tests.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
REFERENCE_DATE="2026-02-20"
PASS=0
FAIL=0

cleanup() {
    rm -f "$REPO_ROOT/partition_analysis.txt" "$REPO_ROOT/partition_fix.sql"
}

run_python_test() {
    local name="$1"
    local input="$2"
    local expected="$3"

    echo "  Running: $name"
    cd "$REPO_ROOT"
    python3 missing_date.py "$input" --reference-date "$REFERENCE_DATE" > /dev/null

    if [ "${REGENERATE:-0}" = "1" ]; then
        cp partition_analysis.txt "$expected"
        echo "    ✅ Regenerated: $expected"
        PASS=$((PASS + 1))
        cleanup
        return
    fi

    if diff <(grep -vE "^(Generated:|Server timezone:)" partition_analysis.txt) \
            <(grep -vE "^(Generated:|Server timezone:)" "$expected") > /dev/null; then
        echo "    ✅ PASS"
        PASS=$((PASS + 1))
    else
        echo "    ❌ FAIL - diff:"
        diff <(grep -vE "^(Generated:|Server timezone:)" partition_analysis.txt) \
             <(grep -vE "^(Generated:|Server timezone:)" "$expected") || true
        FAIL=$((FAIL + 1))
    fi
    cleanup
}

run_shell_test() {
    local name="$1"
    local cmd="$2"
    local expect_success="${3:-true}"

    echo "  Running: $name"
    cd "$REPO_ROOT"
    if eval "$cmd" > /dev/null 2>&1; then
        if [ "$expect_success" = "true" ]; then
            echo "    ✅ PASS"
            PASS=$((PASS + 1))
        else
            echo "    ❌ FAIL (expected failure but succeeded)"
            FAIL=$((FAIL + 1))
        fi
    else
        if [ "$expect_success" = "false" ]; then
            echo "    ✅ PASS (expected failure)"
            PASS=$((PASS + 1))
        else
            echo "    ❌ FAIL (expected success but failed)"
            FAIL=$((FAIL + 1))
        fi
    fi
    cleanup
}

# ── Python analysis tests ─────────────────────────────────────────────────────
echo ""
echo "=== Python analysis tests ==="

run_python_test \
    "result_ok: no missing partitions" \
    "tests/result_ok.txt" \
    "tests/partition_analysis_ok.txt"

run_python_test \
    "result_NOK: missing partitions detected" \
    "tests/result_NOK.txt" \
    "tests/partition_analysis_NOK.txt"

# ── Shell script tests ────────────────────────────────────────────────────────
echo ""
echo "=== Shell script tests ==="

run_shell_test \
    "extract_result.sh -f works without .env" \
    "bash extract_result.sh -f tests/result_ok.txt"

run_shell_test \
    "extract_result.sh -f NOK file works without .env" \
    "bash extract_result.sh -f tests/result_NOK.txt"

run_shell_test \
    "extract_result.sh fails on missing file" \
    "bash extract_result.sh -f tests/does_not_exist.txt" \
    "false"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "=============================="
echo "Results: $PASS passed, $FAIL failed"
echo "=============================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
