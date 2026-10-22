# schedr standalone in one container, for trying schedr on a cluster before the full
# deployment (see PROJECT.md, "standalone in one pod"). Not for production: one process runs
# every role, on SQLite.
#
# Alpine uses musl, so it needs the musllinux wheel: `make smoke-musl` builds it into dist/
# (CI's `wheel` job builds it too). From the repository root:
#   podman build -f examples/kubernetes/Dockerfile -t schedr-standalone .
# (.dockerignore keeps the build context to the wheels in dist/.)
FROM docker.io/library/python:3.14.8-alpine3.24

# tini as PID 1 reaps orphaned processes (a task's child that outlives it), which schedr, not
# being an init, wouldn't.
RUN apk add --no-cache tini

# The worker runs DAG tasks with this Python (found next to the schedr binary): install your
# DAGs' libraries in this image as well.
COPY dist/schedr-*-musllinux_1_2_x86_64.whl /tmp/
RUN pip install --no-cache-dir --disable-pip-version-check --root-user-action=ignore /tmp/schedr-*.whl \
    && rm /tmp/schedr-*.whl

# OpenShift runs containers under an arbitrary UID in group 0: the data directory (the SQLite
# database, task logs, the standalone lock) must be writable by group 0.
RUN mkdir -p /var/lib/schedr \
    && chgrp -R 0 /var/lib/schedr \
    && chmod -R g=u /var/lib/schedr
ENV HOME=/var/lib/schedr \
    SCHEDR_CONFIG__WORKER__DATA_DIR=/var/lib/schedr
USER 1001

# The REST API (no authentication of its own until the UI, milestone M3: keep the Service
# internal, or behind an authenticating proxy or mesh).
EXPOSE 8080
ENTRYPOINT ["/sbin/tini", "--", "schedr"]
CMD ["standalone", "--listen", "0.0.0.0"]
