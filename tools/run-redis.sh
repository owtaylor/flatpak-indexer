#!/bin/bash

set -ex

topdir=$(cd $(dirname $0)/.. && pwd)

podman run \
        --rm --user 0:0 \
        -v $topdir/work/redis-data:/var/lib/redis/data:z \
        registry.redhat.io/rhel8/redis-5 \
        chown redis:redis /var/lib/redis/data

exec podman run \
        --name=flatpak-redis --rm \
        -p 6379:6379 \
        -v $topdir/work/redis-data:/var/lib/redis/data:z \
        registry.redhat.io/rhel8/redis-5
