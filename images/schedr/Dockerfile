# The schedr base image on glibc (decision 454; see archive/PROJECT.md, Components and roles:
# production path): the binary, Python, the `schedr` package, a set of libraries for tasks
# (requirements.in: API calls, data, networking, every connection type's client) and the
# command-line clients a command uses its connections with, running `schedr standalone` by
# default. More libraries go in an image of your own built FROM this one
# (examples/kubernetes/Dockerfile). The same image on Alpine is Dockerfile.alpine, tagged
# `-alpine`.
#
# Debian, as the official Python images' `slim`; glibc, so the manylinux wheel: `make image`
# builds it into dist/ and this image from it. From the repository root:
#   podman build -f images/schedr/Dockerfile -t schedr:1.0.0 .
# (.dockerignore keeps the build context to the wheels in dist/ and the requirements here.)
# python:3.14.8-slim (Debian 13, trixie), created 2026-10-21 (archive/PROJECT.md, CI
# supply-chain pins: the newest at least two weeks old).
FROM docker.io/library/python:3.14.8-slim@sha256:fbecd461fc72d21b2143c6eaba25143738079154a6d0ea3da3aec724f50eed4a

# MongoDB's database tools aren't in Debian: MongoDB's own package, checked by its hash.
ARG MONGODB_TOOLS=https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian13-x86_64-100.20.1.deb
ARG MONGODB_TOOLS_SHA256=6c6d3905f98937168c33ea09bdab733156d653bfe196cad2479204f5337b216f
# PostgreSQL 18's client, as on Alpine: Debian 13 has 17, whose pg_dump refuses an 18 server.
# PostgreSQL's own packages (apt.postgresql.org), their signing key checked by its hash (its
# fingerprint B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8, checked when pinned, 2026-11-11).
ARG PGDG_KEY=https://www.postgresql.org/media/keys/ACCC4CF8.asc
ARG PGDG_KEY_SHA256=0144068502a1eddd2a0280ede10ef607d1ec592ce819940991203941564e8e76

# tini as PID 1 reaps orphaned processes (a task's child that outlives it), which schedr, not
# being an init, wouldn't. The clients commands use their connections with
# (docs/02-dags/14-connections.md): psql and pg_dump 18, MariaDB's (its connector has the
# caching_sha2_password plugin MySQL 8 logs in with), Mongo's database tools, ssh and scp,
# curl.
RUN --mount=type=tmpfs,target=/var/cache/apt \
    apt-get update \
    && apt-get install -y --no-install-recommends tini ca-certificates curl openssh-client \
        mariadb-client mariadb-client-compat libmariadb3 \
    && curl -fsSLo /usr/share/keyrings/postgresql.asc "$PGDG_KEY" \
    && echo "$PGDG_KEY_SHA256  /usr/share/keyrings/postgresql.asc" | sha256sum -c - \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql.asc] https://apt.postgresql.org/pub/repos/apt trixie-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client-18 \
    && curl -fsSLo /tmp/mongodb-tools.deb "$MONGODB_TOOLS" \
    && echo "$MONGODB_TOOLS_SHA256  /tmp/mongodb-tools.deb" | sha256sum -c - \
    && apt-get install -y --no-install-recommends /tmp/mongodb-tools.deb \
    && rm /tmp/mongodb-tools.deb \
    && rm -rf /var/lib/apt/lists/*

# The libraries, as manylinux wheels only (nothing compiled here: pymongo's wheel has its C
# extensions).
RUN --mount=type=bind,source=images/schedr/requirements.txt,target=/tmp/requirements.txt \
    pip install --no-cache-dir --disable-pip-version-check --root-user-action=ignore \
        --only-binary :all: -r /tmp/requirements.txt

# The worker runs DAG tasks with this Python (found next to the schedr binary).
COPY dist/schedr-*-manylinux*_x86_64.whl /tmp/
RUN pip install --no-cache-dir --disable-pip-version-check --root-user-action=ignore /tmp/schedr-*.whl \
    && rm /tmp/schedr-*.whl

# The documentation, served by the web UI at /docs (scripts/docs-dist.sh made it).
COPY dist/docs /usr/share/schedr/docs

# The fork server's preload list must leave a single thread: forking with threads can deadlock
# the child, and the fork server would be disabled at every start. Imported here, the build
# fails on a library that starts one (with the threads' names).
RUN --mount=type=bind,source=images/schedr/preload.txt,target=/tmp/preload.txt \
    python -c 'import importlib, sys, time; from schedr.runner.fork_server import thread_names; \
mods = [m for m in (l.strip() for l in open("/tmp/preload.txt")) if m and not m.startswith("#")]; \
[importlib.import_module(m) for m in mods]; time.sleep(1); names = thread_names(); \
sys.exit(f"the preload list starts threads: {names}" if names else 0)'

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
