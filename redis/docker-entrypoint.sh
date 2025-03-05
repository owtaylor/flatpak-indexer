#!/bin/sh

cat /etc/valkey/valkey.conf | \
    sed \
	-e 's@^dir .*@dir /data@' \
	-e 's@^logfile .*@logfile ""@' \
	-e 's@^bind @# bind @' \
        > /etc/valkey/valkey-docker.conf

if [ "${REDIS_PASSWORD+set}" = "set" ] ; then
    echo "requirepass ${REDIS_PASSWORD}" >> /etc/valkey/valkey-docker.conf
fi

if [ "$1" = "valkey-server" ] ; then
    shift
    set -- valkey-server /etc/valkey/valkey-docker.conf "$@"
fi

exec "$@"
