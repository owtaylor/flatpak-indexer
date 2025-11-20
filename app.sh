#!/bin/sh

for d in deltas icons ; do
    [ -d $OUTPUT_DIR/$d ] || mkdir -p -m 0755 $OUTPUT_DIR/$d
done

command=${FLATPAK_INDEXER_COMMAND:-daemon}

exec flatpak-indexer -v -c /etc/flatpak-indexer/config.yaml $command
