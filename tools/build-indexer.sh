#!/bin/bash

set -ex

origin=$(cd $(dirname $0)/.. && pwd)

tmp_container=
work=$(mktemp -d)
cleanup() {
    :
    #    rm -rf $work
    [ -n "$tmp_container" ] && podman rm $tmp_container
}
trap cleanup EXIT

set -x

podman build $origin/differ -t flatpak-indexer-tar-diff

s2i build --copy --as-dockerfile=$work/Dockerfile $origin registry.access.redhat.com/ubi8/python-38 flatpak-indexer

tmp_container=$(podman create flatpak-indexer-tar-diff)
mkdir -p -m 0755 $work/upload/src/bin
podman cp $tmp_container:/opt/app-root/tar-diff $work/upload/src/bin/tar-diff
podman cp $tmp_container:/usr/bin/time $work/upload/src/bin/time

tmp_tag="flatpak-indexer:$(date +%Y%m%d-%H%M%S)"
podman build $work -t $tmp_tag
if podman run --network=none --rm $tmp_tag tools/test.sh ; then
    podman tag $tmp_tag flatpak-indexer:latest
    podman rmi $tmp_tag
else
    podman rmi $tmp_tag
    exit 1
fi
