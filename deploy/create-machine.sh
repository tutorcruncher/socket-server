#!/usr/bin/env bash
eval `cat ./env.sh`
NAME='tc-socket'
docker-machine create -d scaleway --scaleway-commercial-type 'C2M' --scaleway-name ${NAME} ${NAME}
