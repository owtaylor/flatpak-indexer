#!/bin/bash

set -ex

topdir=$(cd $(dirname $0)/.. && pwd)
out_dir=$(cd $(dirname $0)/.. && pwd)/out

exec podman run \
       --name=flatpak-indexer --rm \
       -v $topdir/out:/var/www/flatpaks:z \
       -v $topdir/config-local.yaml:/etc/flatpak-indexer/config.yaml:z \
       flatpak-indexer
