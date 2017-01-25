#!/usr/bin/env bash
set -e
eval `cat ./env.sh`
if [ -z "$APP_MASTER_KEY" ]; then
    echo "environment variable APP_MASTER_KEY not set or blank"
    exit 2
fi
export COMMIT=`git rev-parse HEAD`
export RELEASE_DATE=`date`
the_command="docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build $@"
echo "Running '${the_command}'..."
eval ${the_command}
