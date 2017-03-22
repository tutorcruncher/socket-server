#!/usr/bin/env bash

gnome-terminal \
    --geometry=180x40 \
    --tab-with-profile=prod \
    -x bash -c "cd $(dirname "$0"); source activate.prod.sh; docker ps; exec bash"
