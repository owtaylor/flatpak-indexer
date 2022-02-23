#!/bin/sh

# These is set to build-time specific location when building the Red Hat-internal
# image; unset them to avoid breaking things at runtime.
unset REQUESTS_CA_BUNDLE
unset GIT_SSL_CAINFO

for d in deltas icons ; do
    [ -d $OUTPUT_DIR/$d ] || mkdir -p -m 0755 $OUTPUT_DIR/$d
done

command=${FLATPAK_INDEXER_COMMAND:-daemon}

exec flatpak-indexer -v -c /etc/flatpak-indexer/config.yaml $command
