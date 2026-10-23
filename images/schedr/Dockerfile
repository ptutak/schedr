# The schedr base image (see PROJECT.md, Components and roles: production path): the binary,
# Python and the `schedr` package, running `schedr standalone` by default. Your DAGs'
# libraries go in an image of your own built FROM this one (examples/kubernetes/Dockerfile).
#
# Alpine, for a small system to keep patched; it uses musl, so the musllinux wheel:
# `make image` builds it into dist/ and this image from it. From the repository root:
#   podman build -f images/schedr/Dockerfile -t schedr:0.1.0 .
# (.dockerignore keeps the build context to the wheels in dist/.)
FROM docker.io/library/python:3.14.8-alpine3.24

# tini as PID 1 reaps orphaned processes (a task's child that outlives it), which schedr, not
# being an init, wouldn't.
RUN apk add --no-cache tini

# The worker runs DAG tasks with this Python (found next to the schedr binary).
COPY dist/schedr-*-musllinux_1_2_x86_64.whl /tmp/
RUN pip install --no-cache-dir --disable-pip-version-check --root-user-action=ignore /tmp/schedr-*.whl \
    && rm /tmp/schedr-*.whl

# OpenShift runs containers under an arbitrary UID in group 0: the data directory (the SQLite
# database, task logs, the journal, the standalone lock) must be writable by group 0.
RUN mkdir -p /var/lib/schedr \
    && chgrp -R 0 /var/lib/schedr \
    && chmod -R g=u /var/lib/schedr
ENV HOME=/var/lib/schedr \
    SCHEDR_CONFIG__WORKER__DATA_DIR=/var/lib/schedr
USER 1001

# The REST API (no authentication of its own until the UI, milestone M3: keep the Service
# internal, or behind an authenticating proxy or mesh), and the ops port (probes, metrics).
EXPOSE 8080 8090
ENTRYPOINT ["/sbin/tini", "--", "schedr"]
# Not `--migrate`: the schema is migrated as a step of its own (`schedr db migrate`, the
# chart's init container); add it to try the image out on a fresh database.
CMD ["standalone", "--listen", "0.0.0.0"]
