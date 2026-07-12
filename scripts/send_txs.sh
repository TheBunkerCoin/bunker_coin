#!/usr/bin/env bash
# Submit a series of genesis transfers one after another over the plain RPC.
#
# The RPC server signs each tx (all-zero signature) and fills the nonce from the
# CURRENT FINALIZED state, so each tx must FINALIZE before the next is sent —
# otherwise they'd all reuse the same nonce. This loop submits one, polls until
# it's finalized, then submits the next.
#
# Usage:
#   scripts/send_txs.sh [COUNT] [AMOUNT] [FEE] [TO] [URL]
# Defaults: COUNT=5 AMOUNT=1000 FEE=100 TO=00..aa URL=http://localhost:3001
#
# Run the node with --rpc first; it prints the genesis pubkey at startup:
#   genesis account (funded ...): <GEN>
# Either export GEN=<that pubkey>, or this script will read /accounts to find it
# is not possible — so you must set GEN. Example:
#   GEN=3f3c...1d70 scripts/send_txs.sh 5

set -euo pipefail

COUNT="${1:-5}"
AMOUNT="${2:-1000}"
FEE="${3:-100}"
TO="${4:-$(printf '00%.0s' {1..31})aa}"
URL="${5:-http://localhost:3001}"
: "${GEN:?set GEN to the genesis pubkey printed at node startup}"

zsig=$(printf '0%.0s' {1..128})   # 64 zero bytes = all-zero signature

for i in $(seq 1 "$COUNT"); do
  echo "--- tx $i/$COUNT ---"
  resp=$(curl -s -X POST "$URL/transactions" -H 'content-type: application/json' -d "{
    \"sender\":\"$GEN\", \"nonce\":0, \"fee\":$FEE, \"signature\":\"$zsig\",
    \"body\":{\"Transfer\":{\"to\":\"$TO\",\"amount\":$AMOUNT}}
  }")
  hash=$(printf '%s' "$resp" | sed -n 's/.*"hash":"\([0-9a-f]*\)".*/\1/p')
  if [ -z "$hash" ]; then
    echo "submit failed: $resp"
    exit 1
  fi
  echo "submitted $hash; waiting for finalization..."

  # Poll the tx status until it is finalized (or fails).
  for _ in $(seq 1 60); do
    st=$(curl -s "$URL/transactions/$hash" | sed -n 's/.*"location":"\([a-z]*\)".*/\1/p')
    if [ "$st" = "finalized" ]; then
      echo "  finalized."
      break
    fi
    sleep 2
  done
done

echo "=== done. genesis + recipient balances: ==="
curl -s "$URL/accounts/$GEN"; echo
curl -s "$URL/accounts/$TO"; echo
