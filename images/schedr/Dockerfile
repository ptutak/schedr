# The schedr base image (see PROJECT.md, Components and roles: production path): the binary,
# Python, the `schedr` package and a set of libraries for tasks (requirements.in: API calls,
# data, networking, MongoDB), running `schedr standalone` by default. More libraries go in an
# image of your own built FROM this one (examples/kubernetes/Dockerfile).
#
# Alpine, for a small system to keep patched; it uses musl, so the musllinux wheel:
# `make image` builds it into dist/ and this image from it. From the repository root:
#   podman build -f images/schedr/Dockerfile -t schedr:0.1.0 .
# (.dockerignore keeps the build context to the wheels in dist/ and the requirements here.)

# The builder: every library as a wheel in /wheels. Most come as musllinux wheels; pymongo has
# none, so it is compiled here, its C extensions required (PYMONGO_C_EXT_MUST_BUILD: without
# them it would fall back to pure Python, with only a warning), with the pinned build tools
# (no build isolation: nothing unpinned fetched).
FROM docker.io/library/python:3.14.8-alpine3.24 AS builder
RUN apk add --no-cache build-base
COPY images/schedr/requirements.txt images/schedr/build-requirements.txt /tmp/
RUN pip install --no-cache-dir --disable-pip-version-check --root-user-action=ignore \
        -r /tmp/build-requirements.txt \
    && PYMONGO_C_EXT_MUST_BUILD=1 pip wheel --no-cache-dir --disable-pip-version-check \
        --no-build-isolation --wheel-dir /wheels -r /tmp/requirements.txt

FROM docker.io/library/python:3.14.8-alpine3.24

# tini as PID 1 reaps orphaned processes (a task's child that outlives it), which schedr, not
# being an init, wouldn't.
RUN apk add --no-cache tini

# The libraries, only from the builder's wheels (no compiler in this image), mounted rather
# than copied: a copied layer would keep the wheels (80 MB) though deleted after.
RUN --mount=type=bind,from=builder,source=/wheels,target=/tmp/wheels \
    --mount=type=bind,source=images/schedr/requirements.txt,target=/tmp/requirements.txt \
    pip install --no-cache-dir --disable-pip-version-check --root-user-action=ignore --no-index \
        --find-links /tmp/wheels -r /tmp/requirements.txt

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
