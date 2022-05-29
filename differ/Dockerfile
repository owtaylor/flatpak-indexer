FROM registry.access.redhat.com/ubi8/go-toolset

USER root
RUN yum -y install time && yum clean all
ADD tar-diff /tmp/src
RUN chown -R 1001:0 /tmp/src

USER 1001

RUN cd /tmp/src && go build -o /opt/app-root/tar-diff ./cmd/tar-diff
