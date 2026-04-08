#!/bin/bash
# Tests for cbm-code-discovery-gate

GATE="$(cd "$(dirname "$0")" && pwd)/cbm-code-discovery-gate"
PASS=0
FAIL=0

cleanup() {
    rm -f /tmp/cbm-gate-*
}

assert_exit() {
    local expected=$1
    local actual=$2
    local test_name=$3
    if [ "$actual" -eq "$expected" ]; then
        echo "  PASS: $test_name"
        ((PASS++))
    else
        echo "  FAIL: $test_name (expected exit $expected, got $actual)"
        ((FAIL++))
    fi
}

make_input() {
    local session_id="${1:-abc123}"
    local tool_name="${2:-Bash}"
    local command="${3:-npm test}"
    cat <<EOF
{"session_id":"$session_id","tool_name":"$tool_name","tool_input":{"command":"$command"},"cwd":"/home/user/my-project","permission_mode":"default","hook_event_name":"PreToolUse","transcript_path":"/tmp/t.jsonl"}
EOF
}

# --- Test 1: First call blocks ---
echo "Test 1: First call blocks with exit 2"
cleanup
echo "$(make_input)" | "$GATE" 2>/dev/null
assert_exit 2 $? "first call should exit 2"

# --- Test 2: Retry within 5s allows ---
echo "Test 2: Immediate retry allows with exit 0"
# gate file already exists from test 1
echo "$(make_input)" | "$GATE" 2>/dev/null
assert_exit 0 $? "immediate retry should exit 0"

# --- Test 3: Stale timestamp blocks ---
echo "Test 3: Stale timestamp (>5s) blocks with exit 2"
GATE_FILE=$(ls /tmp/cbm-gate-* 2>/dev/null | head -1)
echo $(( $(date +%s) - 10 )) > "$GATE_FILE"
echo "$(make_input)" | "$GATE" 2>/dev/null
assert_exit 2 $? "stale retry should exit 2"

# --- Test 4: Different tool_input is a separate request ---
echo "Test 4: Different tool_input blocks independently"
cleanup
echo "$(make_input abc123 Bash "npm test")" | "$GATE" 2>/dev/null
assert_exit 2 $? "first call with input A should exit 2"
echo "$(make_input abc123 Bash "npm run build")" | "$GATE" 2>/dev/null
assert_exit 2 $? "first call with input B should exit 2"

# --- Test 5: Different session_id is a separate request ---
echo "Test 5: Different session_id blocks independently"
cleanup
echo "$(make_input session1 Bash "npm test")" | "$GATE" 2>/dev/null
assert_exit 2 $? "first call with session1 should exit 2"
echo "$(make_input session2 Bash "npm test")" | "$GATE" 2>/dev/null
assert_exit 2 $? "first call with session2 should exit 2"

# --- Test 6: Different tool_name is a separate request ---
echo "Test 6: Different tool_name blocks independently"
cleanup
echo "$(make_input abc123 Bash "npm test")" | "$GATE" 2>/dev/null
assert_exit 2 $? "first call with Bash should exit 2"
echo "$(make_input abc123 Read "npm test")" | "$GATE" 2>/dev/null
assert_exit 2 $? "first call with Read should exit 2"

# --- Test 7: Retry refreshes timestamp (allows chained retries) ---
echo "Test 7: Chained retries keep allowing"
cleanup
echo "$(make_input)" | "$GATE" 2>/dev/null  # first call, blocks
echo "$(make_input)" | "$GATE" 2>/dev/null  # retry, allows
echo "$(make_input)" | "$GATE" 2>/dev/null  # third call, still within 5s
assert_exit 0 $? "third chained retry should exit 0"

# --- Test 8: Gate file is removed after stale timeout ---
echo "Test 8: Gate file is cleaned up on stale access"
cleanup
echo "$(make_input)" | "$GATE" 2>/dev/null
GATE_FILE=$(ls /tmp/cbm-gate-* 2>/dev/null | head -1)
echo $(( $(date +%s) - 10 )) > "$GATE_FILE"
echo "$(make_input)" | "$GATE" 2>/dev/null
# After stale block, a new gate file should exist with fresh timestamp
COUNT=$(ls /tmp/cbm-gate-* 2>/dev/null | wc -l)
if [ "$COUNT" -eq 1 ]; then
    echo "  PASS: gate file recreated after stale cleanup"
    ((PASS++))
else
    echo "  FAIL: expected 1 gate file, found $COUNT"
    ((FAIL++))
fi

# --- Test 9: Blocked message goes to stderr ---
echo "Test 9: Block message is written to stderr"
cleanup
STDERR=$(echo "$(make_input)" | "$GATE" 2>&1 1>/dev/null)
if echo "$STDERR" | grep -q "BLOCKED"; then
    echo "  PASS: stderr contains BLOCKED message"
    ((PASS++))
else
    echo "  FAIL: stderr missing BLOCKED message"
    ((FAIL++))
fi

# --- Test 10: Allowed retry produces no stderr ---
echo "Test 10: Allowed retry produces no stderr"
STDERR=$(echo "$(make_input)" | "$GATE" 2>&1 1>/dev/null)
if [ -z "$STDERR" ]; then
    echo "  PASS: no stderr on allowed retry"
    ((PASS++))
else
    echo "  FAIL: unexpected stderr on allowed retry: $STDERR"
    ((FAIL++))
fi

# --- Summary ---
cleanup
echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
