#!/usr/bin/env bash
# Installs hourly size-based rotation for the node/stream logs so they cannot
# fill the disk. copytruncate keeps the tmux-wrapper redirects valid. Idempotent.
# Usage: sudo ./setup_log_rotation.sh [user]
set -euo pipefail

RUN_USER="${1:-bnkr}"
HOME_DIR="$(eval echo "~${RUN_USER}")"
CONF=/etc/bunkercoin-logrotate.conf
STATE=/var/lib/logrotate/bunkercoin.status

cat > "$CONF" << EOF
${HOME_DIR}/bc-node0/run.log ${HOME_DIR}/bc-node1/run.log ${HOME_DIR}/stream.log {
    size 200M
    rotate 2
    copytruncate
    compress
    missingok
    notifempty
}
EOF

cat > /etc/cron.hourly/bunkercoin-logrotate << EOF
#!/bin/sh
/usr/sbin/logrotate --state ${STATE} ${CONF}
EOF
chmod +x /etc/cron.hourly/bunkercoin-logrotate

logrotate --state "$STATE" "$CONF"
echo "installed: hourly rotation at 200M, keeping 2 compressed generations per log"
