# The schedr server image on glibc (decision 454; see archive/PROJECT.md, Components and roles:
# the distributed chart): the binary only, for `schedr master`, `schedr webserver` and `schedr
# ui`, which never run user code: no Python. Workers use the base image
# (images/schedr/Dockerfile), which has Python and the libraries for tasks. The same image on
# Alpine is Dockerfile.alpine, tagged `-alpine`.
#
# The binary is the manylinux wheel's. `make server-image` builds the wheel into dist/ and this
# image from it. From the repository root:
#   podman build -f images/schedr-server/Dockerfile -t schedr-server:1.0.0 .
# The wheel unpacked with Python's zipfile (Debian slim has no unzip): the base image's
# python:3.14.8-slim digest.
FROM docker.io/library/python:3.14.8-slim@sha256:fbecd461fc72d21b2143c6eaba25143738079154a6d0ea3da3aec724f50eed4a AS wheel
COPY dist/schedr-*-manylinux*_x86_64.whl /tmp/
RUN cd /tmp && python -m zipfile -e schedr-*.whl /tmp/wheel \
    && mv /tmp/wheel/schedr-*.data/scripts/schedr /schedr \
    && chmod 0755 /schedr \
    && mv /tmp/wheel/schedr-*.dist-info/licenses /licenses

# debian:trixie-20261027-slim, created 2026-10-28 (archive/PROJECT.md, CI supply-chain pins:
# the newest at least two weeks old).
FROM docker.io/library/debian:trixie-20261027-slim@sha256:f6b08b09a05725f9431502d3b5714f5c4576e82c1a84fa1debe0c24a7e0c3ec4
# tini as PID 1 (signals and orphans); CA certificates for TLS to the database.
RUN --mount=type=tmpfs,target=/var/cache/apt \
    apt-get update \
    && apt-get install -y --no-install-recommends tini ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=wheel /schedr /usr/local/bin/schedr
COPY --from=wheel /licenses /usr/share/licenses/schedr
# The documentation, served by `schedr ui` at /docs (scripts/docs-dist.sh made it).
COPY dist/docs /usr/share/schedr/docs
RUN mkdir -p /var/lib/schedr \
    && chgrp -R 0 /var/lib/schedr \
    && chmod -R g=u /var/lib/schedr
ENV HOME=/var/lib/schedr
USER 1001
EXPOSE 8070 8080 8090
ENTRYPOINT ["/usr/bin/tini", "--", "schedr"]
CMD ["master"]
