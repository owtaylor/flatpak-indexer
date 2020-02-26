#!/bin/bash

set -ex

topdir=$(cd $(dirname $0)/.. && pwd)
out_dir=$(cd $(dirname $0)/.. && pwd)/out

if ! [ -e $topdir/flatpak_indexer/certs/RH-IT-Root-CA.crt -a \
       -e $HOME/.config/flatpak-indexer/client.crt ] ; then
    echo "Please follow setup instructions in README.md" 1>&2
    exit 1
fi

exec podman run \
       --name=flatpak-indexer --rm \
       -e PYXIS_CERT_DIR=/etc/pki/flatpak-indexer \
       -v $topdir/out:/var/www/flatpaks:z \
       -v $topdir/config-local.yaml:/etc/flatpak-indexer/config.yaml:z \
       -v $HOME/.config/flatpak-indexer:/etc/pki/flatpak-indexer:z \
       -v $topdir/flatpak_indexer/certs:/etc/pki/brew:z \
       -v $topdir/brew.conf:/etc/koji.conf:z \
       flatpak-indexer
