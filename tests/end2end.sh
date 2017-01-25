#!/usr/bin/env bash
# script to test end to end use of tutorcruncher-socket
# should be run from the project root directory
set -e
set -x
# use dummy ssl certs to test
# this makes sure keys doesn't already exist
key_dir="nginx/prod/keys"
if [[ ! -e ${key_dir} ]]; then
    echo "key dir ${key_dir} does not exist, creating it and using dummy keys"
    mkdir ${key_dir}
    cp nginx/test-keys/* ${key_dir}
elif [[ ! -d ${key_dir} ]]; then
    echo "key dir ${key_dir} already exists"
fi

# prevent the cloudflare ip check returning 403 below
echo "allow all;" > nginx/prod/allowed.nginx.conf
export LOGSPOUT_ENDPOINT="syslog://example.com"
export APP_MASTER_KEY="123"

deploy/up.sh

sleep 10

docker ps
docker-compose ps

# the first curl prints the response
# the second curl fails if the response is not ok
curl -kv -H "Host: socket.tutorcruncher.com" https://localhost:443
docker-compose logs -t

curl -kfs -H "Host: socket.tutorcruncher.com" https://localhost:443 > /dev/null

printf "\n\nend to end tests successful.\n"
