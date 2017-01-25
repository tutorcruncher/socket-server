#!/usr/bin/env bash
set -e
eval `cat ./env.sh`
if [ -z "$APP_MASTER_KEY" ]; then
    echo "environment variable APP_MASTER_KEY not set or blank"
    exit 2
fi
THIS_DIR=$(dirname "$0")
eval "${THIS_DIR}/compose.sh up -d --build $@"
