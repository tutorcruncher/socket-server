#!/usr/bin/env bash
. env/bin/activate
export COMMIT=`git rev-parse HEAD`
export RELEASE_DATE="<not set>"
export SERVER_NAME="localhost"
export APP_MASTER_KEY="testing"
export CLIENT_SIGNING_KEY="testing"
export RAVEN_DSN="-"
