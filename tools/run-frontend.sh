#!/bin/bash

set -ex

tls_secrets_dir=$(cd $(dirname $0)/.. && pwd)/tls-secrets
out_dir=$(cd $(dirname $0)/.. && pwd)/out

# Dance here is to avoid SIGWINCH being through passed to apache,
# since that is used to make apache gracefully setdown.

setsid podman run \
       --name=flatpak-indexer-frontend --rm \
       -e SERVER_NAME=flatpaks.local.fishoup.net \
       -e TOPLEVEL_REDIRECT=https://catalog.redhat.com \
       -p 8443:8443 \
       -v $tls_secrets_dir:/etc/tls-secrets:ro,z \
       -v $out_dir:/var/www/flatpaks:ro,z \
       flatpak-indexer-frontend &
PODMAN_PID=$!

int_podman() {
    kill -INT $PODMAN_PID
}

trap int_podman SIGINT

wait $PODMAN_PID
