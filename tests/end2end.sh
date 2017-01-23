#!/usr/bin/env bash
# should be run from the project root directory
set -e
set -x
# use dummy ssl certs to test
# this makes sure keys doesn't already exist
mkdir nginx/prod/keys
cp nginx/test-keys/* nginx/prod/keys/

# prevent the cloudflare ip check returning 403 below
echo "allow all;" > nginx/prod/allowed.nginx.conf

deploy/up.sh

sleep 10

docker ps
docker-compose ps

# the first curl prints the response
# the second curl fails if the response is not ok
curl -kv -H "Host: socket.tutorcruncher.com" https://localhost:443
docker-compose logs -t

curl -kfs -H "Host: socket.tutorcruncher.com" https://localhost:443 > /dev/null
