#!/usr/bin/env bash
# Test TRX CI-V frequency control on an SCS PACTOR modem.
#
# Usage:
#   ./scripts/test_trx.sh /dev/ttyUSB0 14079
#   ./scripts/test_trx.sh /dev/ttyUSB1 7080.5
#
# Requires: stty, bash

set -euo pipefail

PORT="${1:?Usage: $0 <serial-port> <frequency-khz>}"
FREQ="${2:?Usage: $0 <serial-port> <frequency-khz>}"
BAUD=829440
TIMEOUT=2

echo "=== TRX Frequency Test ==="
echo "Port:      $PORT"
echo "Baud:      $BAUD"
echo "Frequency: $FREQ kHz"
echo ""

# Configure serial port
stty -F "$PORT" "$BAUD" raw -echo -echoe -echok cs8 -parenb -cstopb -crtscts 2>/dev/null || {
    echo "ERROR: Failed to configure $PORT at $BAUD baud"
    echo "  Is the modem connected? Check: ls -la $PORT"
    exit 1
}

# Open a persistent fd for the serial port
exec 3<>"$PORT"

send_cmd() {
    local cmd="$1"
    local label="${2:-$cmd}"
    printf '%s\r' "$cmd" >&3
    sleep "$TIMEOUT"
    local resp
    resp=$(timeout 2 cat <&3 2>/dev/null || true)
    echo ">> $label"
    if [ -n "$resp" ]; then
        echo "   $resp"
    else
        echo "   (no response)"
    fi
    echo ""
}

read_pending() {
    timeout 1 cat <&3 2>/dev/null || true
}

echo "--- Step 1: Drain buffer ---"
read_pending >/dev/null
sleep 0.5

echo "--- Step 2: Exit any existing hostmode ---"
printf 'JHOST0\r' >&3
sleep 1
read_pending >/dev/null

echo "--- Step 3: Sync terminal ---"
send_cmd "" "CR (sync)"

echo "--- Step 4: Check modem identity ---"
send_cmd "MYcall" "MYcall"

echo "--- Step 5: Query TRX CI-V config ---"
send_cmd "TRX TYpe" "TRX TYpe"

echo "--- Step 6: Attempt frequency change ---"
printf 'TRX Frequency %s\r' "$FREQ" >&3
sleep 3
RESP=$(timeout 2 cat <&3 2>/dev/null || true)
echo ">> TRX Frequency $FREQ"
if echo "$RESP" | grep -qi "FREQUENCY CHANGED"; then
    echo "   *** SUCCESS: TRX FREQUENCY CHANGED ***"
    echo ""
    echo "=== RESULT: PASS ==="
else
    echo "   Response: $RESP"
    echo ""
    echo "=== RESULT: FAIL ==="
    echo "  Possible causes:"
    echo "    - CI-V cable not connected between modem and radio"
    echo "    - Radio powered off"
    echo "    - Wrong CI-V baud rate or address (check TRX TYpe output above)"
    echo "    - Radio does not support CI-V (Yaesu/Kenwood use CAT instead)"
fi

# Close fd
exec 3>&-

echo ""
echo "Done."
