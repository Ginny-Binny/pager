#!/usr/bin/env bash
# Issues certificates for the two pager subdomains. Run with sudo, ONCE,
# and only after both A records are confirmed live.
#
# Let's Encrypt rate-limits failed challenges, so this does not retry. If it
# fails, read the error and fix the cause before running it again.
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: sudo $0 <acme-email>" >&2
  exit 2
fi
EMAIL="$1"

echo "==> pre-flight: both names must resolve to this host"
for h in pager.psyduck.in ntfy.psyduck.in; do
  ip=$(getent ahostsv4 "$h" | awk '{print $1}' | sort -u | tr '\n' ' ')
  echo "    $h -> ${ip:-NOT RESOLVING}"
  [ -n "$ip" ] || { echo "ABORT: $h does not resolve" >&2; exit 1; }
done

echo "==> certbot --nginx (no retry loop)"
certbot --nginx \
  -d pager.psyduck.in \
  -d ntfy.psyduck.in \
  --agree-tos --no-eff-email -m "$EMAIL" \
  --redirect --non-interactive

echo "==> nginx -t && reload"
nginx -t && systemctl reload nginx
echo "==> done"
