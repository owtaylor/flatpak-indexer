FROM registry.access.redhat.com/ubi9/go-toolset as tar-diff-builder

USER root
ADD tar-diff /tmp/src
RUN chown -R 1001:0 /tmp/src

USER 1001

# We need to pass -buildvcs=false because we only copy part of the git checkout
RUN cd /tmp/src && go build -buildvcs=false -o /opt/app-root/bin/tar-diff ./cmd/tar-diff

FROM registry.access.redhat.com/ubi9/python-312 as builder

ARG FLATPAK_INDEXER_UPDATE_TEST_DATA=false
ENV FLATPAK_INDEXER_UPDATE_TEST_DATA=${FLATPAK_INDEXER_UPDATE_TEST_DATA}

# Add application sources to a directory that the assemble script expects them
# and set permissions so that the container runs without root access
USER 0
ADD . /tmp/src
RUN /usr/bin/fix-permissions /tmp/src
USER 1001

# Install the application's dependencies from PyPI
RUN /bin/sh /tmp/src/.s2i/bin/assemble

FROM registry.access.redhat.com/ubi9/python-312-minimal

USER 0
RUN microdnf -y install time && microdnf clean all
USER 1001

# Copy the tar-diff binary from the tar-diff-builder image
COPY --from=tar-diff-builder /opt/app-root/bin/tar-diff /opt/app-root/bin/

# Copy app sources together with the whole virtual environment from the builder image
COPY --from=builder /opt/app-root /opt/app-root

# Run tests
RUN $APP_ROOT/src/tools/test.sh --pytest

# Set the default command for the resulting image
CMD /usr/libexec/s2i/run
