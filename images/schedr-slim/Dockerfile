# The slim schedr image on glibc (decisions 383, 454): the binary, Python and the `schedr`
# package, without the base image's task libraries and the command-line clients a command
# uses its connections with (psql, MariaDB's, Mongo's tools, ssh, curl). For DAGs whose tasks
# need none of them, or an image of your own built FROM this one with only what they use. The
# full image (images/schedr, decision 212) stays the default; the same image on Alpine is
# Dockerfile.alpine, tagged `-alpine`.
#
# `make image-slim` builds the manylinux wheel into dist/ and this image from it. From the
# repository root:
#   podman build -f images/schedr-slim/Dockerfile -t schedr-slim:1.0.0 .
# python:3.14.8-slim (Debian 13, trixie), created 2026-10-21: the base image's digest.
FROM docker.io/library/python:3.14.8-slim@sha256:fbecd461fc72d21b2143c6eaba25143738079154a6d0ea3da3aec724f50eed4a

# tini as PID 1 reaps orphaned processes (a task's child that outlives it), which schedr, not
# being an init, wouldn't.
RUN --mount=type=tmpfs,target=/var/cache/apt \
    apt-get update \
    && apt-get install -y --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/*

# The worker runs DAG tasks with this Python (found next to the schedr binary).
COPY dist/schedr-*-manylinux*_x86_64.whl /tmp/
RUN pip install --no-cache-dir --disable-pip-version-check --root-user-action=ignore /tmp/schedr-*.whl \
    && rm /tmp/schedr-*.whl

# The documentation, served by the web UI at /docs (scripts/docs-dist.sh made it).
COPY dist/docs /usr/share/schedr/docs

# OpenShift runs containers under an arbitrary UID in group 0: the data directory (the SQLite
# database, task logs, the journal, the standalone lock) must be writable by group 0.
RUN mkdir -p /var/lib/schedr \
    && chgrp -R 0 /var/lib/schedr \
    && chmod -R g=u /var/lib/schedr
ENV HOME=/var/lib/schedr \
    SCHEDR_CONFIG__WORKER__DATA_DIR=/var/lib/schedr
USER 1001

# The REST API and the ops port (probes, metrics).
EXPOSE 8080 8090
ENTRYPOINT ["/usr/bin/tini", "--", "schedr"]
# Not `--migrate`: the schema is migrated as a step of its own (`schedr db migrate`, the
# chart's init container); add it to try the image out on a fresh database.
CMD ["standalone", "--listen", "0.0.0.0"]
