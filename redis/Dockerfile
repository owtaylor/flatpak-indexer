FROM fedora:latest

RUN \
   groupadd -r -g 1001 redis && \
   useradd -r -g redis -u 1001 -s /sbin/nologin redis

RUN dnf -y update && dnf install -y redis && dnf clean all

RUN chmod 0770 /etc/redis

RUN mkdir -m 0770 /data && chown redis:0 /data
VOLUME /data
WORKDIR /data

COPY docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]

USER 1001
EXPOSE 6379
CMD ["redis-server"]
