#!/bin/sh

[ -d $OUTPUT_DIR/icons ] || mkdir -p -m 0755 $OUTPUT_DIR/icons

command=${FLATPAK_INDEXER_COMMAND:-daemon}

exec flatpak-indexer -v -c /etc/flatpak-indexer/config.yaml $command
