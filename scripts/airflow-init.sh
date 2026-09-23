#!/bin/sh
set -eu

if [ "$(id -u)" -eq 0 ]; then
    # Phase 9's catalog may have been created by the root-run pipeline image.
    # Change ownership of only this ignored runtime catalog, then drop privileges.
    for path in /workspace/data/state/iceberg_catalog.db \
        /workspace/data/state/iceberg_catalog.db-shm \
        /workspace/data/state/iceberg_catalog.db-wal; do
        if [ -e "$path" ]; then
            chown airflow:airflow "$path"
        fi
    done
    exec runuser -u airflow -- /usr/local/bin/airflow-init.sh
fi

airflow db migrate
if ! airflow users list --output json | python -c 'import json,os,sys; users=json.load(sys.stdin); sys.exit(0 if any(user.get("username")==os.environ["AIRFLOW_ADMIN_USER"] for user in users) else 1)'; then
    airflow users create \
        --username "$AIRFLOW_ADMIN_USER" \
        --firstname Local \
        --lastname Admin \
        --role Admin \
        --email "$AIRFLOW_ADMIN_EMAIL" \
        --password "$AIRFLOW_ADMIN_PASSWORD"
fi
