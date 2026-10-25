# The schedr server image (see PROJECT.md, Components and roles: the distributed chart): the
# binary only, for `schedr master` and `schedr webserver`, which never run user code: no
# Python. Workers use the base image (images/schedr/Dockerfile), which has Python and the
# libraries for tasks.
#
# The binary is the musllinux wheel's (statically linked: no second build). `make
# server-image` builds the wheel into dist/ and this image from it. From the repository root:
#   podman build -f images/schedr-server/Dockerfile -t schedr-server:0.1.0 .
FROM docker.io/library/alpine:3.24 AS wheel
COPY dist/schedr-*-musllinux_1_2_x86_64.whl /tmp/
RUN cd /tmp && unzip -q schedr-*.whl 'schedr-*.data/scripts/schedr' \
    && mv schedr-*.data/scripts/schedr /schedr

FROM docker.io/library/alpine:3.24
# tini as PID 1 forwards signals and reaps; CA certificates for Postgres over TLS and the like.
RUN apk add --no-cache tini ca-certificates
COPY --from=wheel /schedr /usr/local/bin/schedr
# OpenShift runs containers under an arbitrary UID in group 0: HOME writable by group 0.
RUN mkdir -p /var/lib/schedr \
    && chgrp -R 0 /var/lib/schedr \
    && chmod -R g=u /var/lib/schedr
ENV HOME=/var/lib/schedr
USER 1001
# The master's gRPC port, the webserver's API (no authentication of its own until the UI,
# milestone M3), and the ops port of both.
EXPOSE 8070 8080 8090
ENTRYPOINT ["/sbin/tini", "--", "schedr"]
CMD ["master"]
