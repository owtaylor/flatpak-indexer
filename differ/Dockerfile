FROM registry.access.redhat.com/ubi8/go-toolset

USER root
RUN yum -y install time && yum clean all
ADD tar-diff /tmp/src
RUN chown -R 1001:0 /tmp/src

USER 1001

# We need to pass -buildvcs=false because we only copy part of the git checkout
RUN cd /tmp/src && go build -buildvcs=false -o /opt/app-root/tar-diff ./cmd/tar-diff
