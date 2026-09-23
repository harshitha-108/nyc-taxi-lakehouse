#!/bin/sh
set -eu

superset db upgrade
users="$(superset fab list-users)"
if ! printf '%s\n' "$users" | grep -F "$SUPERSET_ADMIN_USER" >/dev/null; then
    superset fab create-admin \
        --username "$SUPERSET_ADMIN_USER" \
        --firstname Local \
        --lastname Admin \
        --email "$SUPERSET_ADMIN_EMAIL" \
        --password "$SUPERSET_ADMIN_PASSWORD"
fi
superset init
