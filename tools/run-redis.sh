#!/bin/bash

set -ex

topdir=$(cd $(dirname $0)/.. && pwd)

podman run \
        --rm --user 0:0 \
        -v $topdir/work/redis-data:/data:z \
        docker.io/library/redis:5 \
        chown redis:redis /data

exec podman run \
        --name=flatpak-redis --rm \
        -p 6379:6379 \
        -v $topdir/work/redis-data:/data:z \
        docker.io/library/redis:5
