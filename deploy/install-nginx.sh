#!/usr/bin/env bash
# Installs the pager + ntfy nginx vhosts on a host that already runs nginx.
# Run with sudo, from anywhere:  sudo deploy/install-nginx.sh
#
# Does NOT run certbot — do that separately with deploy/run-certbot.sh, once
# both A records are confirmed live.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# The /status hash is a credential and lives in .env, which is gitignored —
# it is deliberately not baked into this script. docker compose interprets a
# literal $ in .env as the start of an interpolation, so the hash is stored
# doubled ($$2a$$14$$...); undo that here to get the real bcrypt hash.
HASH="${STATUS_BASIC_AUTH:-}"
if [ -z "$HASH" ] && [ -f "$REPO/.env" ]; then
  HASH="$(sed -n 's/^STATUS_BASIC_AUTH=//p' "$REPO/.env" | head -1)"
fi
HASH="${HASH//\$\$/\$}"

case "$HASH" in
  '$2'*) : ;;
  '')    echo "ABORT: no STATUS_BASIC_AUTH in $REPO/.env or environment" >&2; exit 1 ;;
  *)     echo "ABORT: STATUS_BASIC_AUTH is not a bcrypt hash: ${HASH:0:12}..." >&2; exit 1 ;;
esac

echo "==> installing vhosts from $REPO/deploy/nginx"
install -m 644 "$REPO/deploy/nginx/pager.psyduck.in" /etc/nginx/sites-available/pager.psyduck.in
install -m 644 "$REPO/deploy/nginx/ntfy.psyduck.in"  /etc/nginx/sites-available/ntfy.psyduck.in
install -m 644 "$REPO/deploy/nginx/map-connection-upgrade.conf" /etc/nginx/conf.d/map-connection-upgrade.conf

ln -sfn /etc/nginx/sites-available/pager.psyduck.in /etc/nginx/sites-enabled/pager.psyduck.in
ln -sfn /etc/nginx/sites-available/ntfy.psyduck.in  /etc/nginx/sites-enabled/ntfy.psyduck.in

echo "==> writing /etc/nginx/.htpasswd-pager (user: admin)"
umask 027
printf 'admin:%s\n' "$HASH" > /etc/nginx/.htpasswd-pager
chown root:www-data /etc/nginx/.htpasswd-pager
chmod 640 /etc/nginx/.htpasswd-pager

echo "==> nginx -t"
nginx -t

echo "==> reloading nginx"
systemctl reload nginx
echo "==> done. Existing sites untouched."
