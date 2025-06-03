FROM fedora:latest

RUN dnf -y update && dnf install -y valkey && dnf clean all

RUN chmod 0770 /etc/valkey
RUN mkdir /run/valkey && chown valkey:valkey /run/valkey

RUN mkdir -m 0770 /data && chown valkey:0 /data
VOLUME /data
WORKDIR /data

COPY docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]

USER valkey
EXPOSE 6379
CMD ["valkey-server"]
