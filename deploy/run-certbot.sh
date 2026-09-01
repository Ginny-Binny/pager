#!/usr/bin/env bash
# Issues and installs certificates for the two pager subdomains.
# Run with sudo, AFTER deploy/install-nginx.sh has created the server blocks.
#
# Let's Encrypt rate-limits both failed challenges and duplicate issuance, so
# this never retries, and it never re-issues a certificate that already exists
# — in that case it only installs the one on disk.
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: sudo $0 <acme-email>" >&2
  exit 2
fi
EMAIL="$1"
CERT_NAME=pager.psyduck.in

echo "==> pre-flight: nginx must already have server blocks for both names"
# certbot --nginx matches on server_name. Without the vhosts it obtains the
# certificate and then fails to install it, which burns an issuance against
# the rate limit for nothing. Check before asking Let's Encrypt for anything.
missing=0
for h in pager.psyduck.in ntfy.psyduck.in; do
  if nginx -T 2>/dev/null | grep -qE "^\s*server_name\s+.*\b${h}\b"; then
    echo "    server_name $h                 found"
  else
    echo "    server_name $h                 MISSING"
    missing=1
  fi
done
if [ "$missing" -ne 0 ]; then
  cat >&2 <<'EOF'
ABORT: nginx has no server block for one or both names.

certbot --nginx finds the block to edit by matching server_name, so it would
obtain a certificate and then fail with "Could not automatically find a
matching server block". Run the installer first:

    sudo deploy/install-nginx.sh
EOF
  exit 1
fi

echo "==> pre-flight: both names must resolve to this host"
for h in pager.psyduck.in ntfy.psyduck.in; do
  ip=$(getent ahostsv4 "$h" | awk '{print $1}' | sort -u | tr '\n' ' ')
  echo "    $h -> ${ip:-NOT RESOLVING}"
  [ -n "$ip" ] || { echo "ABORT: $h does not resolve" >&2; exit 1; }
done

if certbot certificates --cert-name "$CERT_NAME" 2>/dev/null | grep -q 'Certificate Name:'; then
  # Already issued — a previous run may have obtained it but failed to install.
  # Installing is free; re-issuing counts against the duplicate-certificate
  # rate limit, so do not ask for a new one.
  echo "==> certificate $CERT_NAME already exists; installing it, not re-issuing"
  certbot install --nginx --cert-name "$CERT_NAME" --non-interactive
else
  echo "==> certbot --nginx (no retry loop)"
  certbot --nginx \
    -d pager.psyduck.in \
    -d ntfy.psyduck.in \
    --agree-tos --no-eff-email -m "$EMAIL" \
    --redirect --non-interactive
fi

echo "==> nginx -t && reload"
nginx -t && systemctl reload nginx
echo "==> done"
