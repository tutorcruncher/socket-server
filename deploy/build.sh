#!/usr/bin/env bash
set -e
the_command="docker-compose -f docker-compose.yml -f docker-compose.prod.yml build $@"
echo "Running '${the_command}'..."
eval ${the_command}
