#!/usr/bin/env bash
. env/bin/activate
export COMMIT=`git rev-parse HEAD`
export RELEASE_DATE="<not set>"
export APP_MASTER_KEY="testing"
export CLIENT_SIGNING_KEY="testing"
export COMPOSE_PROJECT_NAME='socket'
export RAVEN_DSN="-"
