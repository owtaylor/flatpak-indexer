#!/bin/bash

set -ex

topdir=$(cd $(dirname $0)/.. && pwd)

podman run \
        --rm --user 0:0 \
        -v $topdir/work/redis-data:/data:z \
        flatpak-indexer-redis \
        chown valkey:valkey /data

exec podman run \
        -e REDIS_PASSWORD=abc123 \
	--name=flatpak-redis --rm \
        -p 127.0.0.1:6379:6379 \
        -v $topdir/work/redis-data:/data:z \
        flatpak-indexer-redis
