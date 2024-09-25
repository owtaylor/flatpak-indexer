#!/bin/bash

set -ex

topdir=$(cd $(dirname $0)/.. && pwd)

usage() {
    echo "Usage: run-indexer.sh [--indexer|--differ] [--fedora|--redhat]"
}

command=
datasource=

while [ "$#" '>' 0 ] ; do
    case "$1" in
        --indexer)
            command=daemon
            ;;
        --differ)
            command=differ
            ;;
        --fedora)
            datasource=fedora
            ;;
        --redhat)
            datasource=redhat
            ;;
        *)
            usage && exit 1
            ;;
    esac

    shift
done

[ -z "$datasource" -o -z "$command" ] && usage && exit 1

options="--name=flatpak-indexer-$command --rm
-e FLATPAK_INDEXER_COMMAND=$command \
-e REDIS_URL=redis://192.168.10.164:6379
-v $topdir/out:/var/www/flatpaks:z
-v $topdir/work:/var/lib/flatpak-indexer:z
-v $topdir/config-$datasource.yaml:/etc/flatpak-indexer/config.yaml:z"

if [ $datasource = redhat ] ; then
    if ! [ -e $topdir/flatpak_indexer/certs/RH-IT-Root-CA.crt -a \
              -e $HOME/.config/flatpak-indexer/client.crt ] ; then
        echo "Please follow setup instructions in README.md" 1>&2
        exit 1
    fi
    options="$options
        -e PYXIS_CERT_DIR=/etc/pki/flatpak-indexer
        -v $HOME/.config/flatpak-indexer:/etc/pki/flatpak-indexer:z \
        -v $topdir/flatpak_indexer/certs:/etc/pki/brew:z \
        -v $topdir/brew.conf:/etc/koji.conf:z"
else
    options="$options
        -v $topdir/koji.conf:/etc/koji.conf:z"
fi

exec podman run $options flatpak-indexer
