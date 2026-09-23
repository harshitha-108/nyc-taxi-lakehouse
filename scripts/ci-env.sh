#!/usr/bin/env bash
set -euo pipefail

# Only a fresh checkout should create this ignored, ephemeral configuration.
if [[ -e .env ]]; then
    echo 'Refusing to replace an existing .env' >&2
    exit 1
fi

cp .env.example .env
chmod 600 .env
ci_password="$(openssl rand -hex 24)"
sed -i "s/local_development_only_change_me/${ci_password}/g" .env
echo 'Created ignored CI-only service configuration.'
