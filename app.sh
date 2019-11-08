#!/bin/sh

[ -d $OUTPUT_DIR/icons ] || mkdir -p 0755 $OUTPUT_DIR/icons
[ -d $OUTPUT_DIR/index ] || mkdir -p 0755 $OUTPUT_DIR/index

exec flatpak-indexer -v -c /etc/flatpak-indexer/config.yaml daemon
