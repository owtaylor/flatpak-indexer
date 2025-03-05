FROM fedora:latest

RUN \
   groupadd -r -g 1001 valkey && \
   useradd -r -g valkey -u 1001 -s /sbin/nologin valkey

RUN dnf -y update && dnf install -y valkey && dnf clean all

RUN chmod 0770 /etc/valkey

RUN mkdir -m 0770 /data && chown valkey:0 /data
VOLUME /data
WORKDIR /data

RUN mkdir /var/run/valkey && chown valkey:0 /var/run/valkey

COPY docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]

USER 1001
EXPOSE 6379
CMD ["valkey-server"]
