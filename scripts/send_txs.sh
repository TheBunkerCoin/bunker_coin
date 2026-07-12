#!/usr/bin/env bash
# Submit a series of genesis transfers one after another over the plain RPC.
#
# The RPC server signs each tx (all-zero signature) and fills the nonce from the
# CURRENT FINALIZED state, so each tx must FINALIZE before the next is sent —
# otherwise they'd all reuse the same nonce. This loop submits one, polls until
# it's finalized, then submits the next.
#
# Built for unattended demo runs: it never gives up on a slow slot (finalization
# on HF can take minutes), retries transient submit/HTTP errors instead of
# exiting, and can run forever with a pause between transfers.
#
# Usage:
#   scripts/send_txs.sh [COUNT] [AMOUNT] [FEE] [TO] [URL]
# Defaults: COUNT=5 AMOUNT=1000 FEE=100 TO=00..aa URL=http://localhost:3001
#   COUNT=0 (or "inf") — run forever until Ctrl-C.
# Environment:
#   GEN    (required) genesis pubkey printed at node startup
#   PAUSE  seconds to wait between transfers (default 30; 0 = back-to-back)
#
# Examples:
#   GEN=3f3c...1d70 scripts/send_txs.sh 5
#   GEN=3f3c...1d70 PAUSE=60 scripts/send_txs.sh 0 500 50 \
#     00000000000000000000000000000000000000000000000000000000000000aa \
#     https://api.bunkercoin.com

set -u

COUNT="${1:-5}"
AMOUNT="${2:-1000}"
FEE="${3:-100}"
TO="${4:-$(printf '00%.0s' {1..31})aa}"
URL="${5:-http://localhost:3001}"
PAUSE="${PAUSE:-30}"
: "${GEN:?set GEN to the genesis pubkey printed at node startup}"

zsig=$(printf '0%.0s' {1..128})   # 64 zero bytes = all-zero signature

i=0
sent=0
while :; do
  i=$((i + 1))
  if [ "$COUNT" != "0" ] && [ "$COUNT" != "inf" ] && [ "$i" -gt "$COUNT" ]; then
    break
  fi
  if [ "$COUNT" = "0" ] || [ "$COUNT" = "inf" ]; then
    echo "--- tx $i ($(date -u '+%H:%M:%S')) ---"
  else
    echo "--- tx $i/$COUNT ($(date -u '+%H:%M:%S')) ---"
  fi

  # Submit; retry on transient failures (link blip, node restarting, nonce not
  # yet advanced after the previous finalization).
  hash=""
  while [ -z "$hash" ]; do
    resp=$(curl -s -m 15 -X POST "$URL/transactions" -H 'content-type: application/json' -d "{
      \"sender\":\"$GEN\", \"nonce\":0, \"fee\":$FEE, \"signature\":\"$zsig\",
      \"body\":{\"Transfer\":{\"to\":\"$TO\",\"amount\":$AMOUNT}}
    }" || true)
    hash=$(printf '%s' "$resp" | sed -n 's/.*"hash":"\([0-9a-f]*\)".*/\1/p')
    if [ -z "$hash" ]; then
      echo "  submit not accepted (${resp:-no response}); retrying in 10s..."
      sleep 10
    fi
  done
  echo "  submitted $hash; waiting for finalization..."

  # Poll until finalized or failed — no cap: an HF slot can take minutes.
  while :; do
    st=$(curl -s -m 10 "$URL/transactions/$hash" | sed -n 's/.*"location":"\([a-z]*\)".*/\1/p' || true)
    if [ "$st" = "finalized" ]; then
      sent=$((sent + 1))
      echo "  finalized ($sent total)."
      break
    fi
    sleep 5
  done

  if [ "$PAUSE" -gt 0 ] 2>/dev/null; then
    sleep "$PAUSE"
  fi
done

echo "=== done: $sent transfers finalized. genesis + recipient balances: ==="
curl -s "$URL/accounts/$GEN"; echo
curl -s "$URL/accounts/$TO"; echo
