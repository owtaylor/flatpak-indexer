#!/bin/sh

cat /etc/redis/redis.conf | \
    sed \
	-e 's@^dir .*@dir /data@' \
	-e 's@^logfile .*@logfile ""@' \
	-e 's@^bind @# bind @' \
        > /etc/redis/redis-docker.conf

if [ "${REDIS_PASSWORD+set}" = "set" ] ; then
    echo "requirepass ${REDIS_PASSWORD}" >> /etc/redis/redis-docker.conf
fi

if [ "$1" = "redis-server" ] ; then
    shift
    set -- redis-server /etc/redis/redis-docker.conf "$@"
fi

exec "$@"
