# Your image: the schedr base image plus your DAGs' libraries (see archive/PROJECT.md, Components and
# roles: production path). Build the base first (`make image`), then from this directory:
#   podman build --build-arg SCHEDR=schedr:1.0.0 -t my-schedr:1 .
#
# The base is Debian (glibc, decision 454). pip takes `manylinux` wheels where a library
# publishes them (nearly all do); for the rest, the builder stage compiles wheels, and the final
# image installs only those wheels, with no compiler in it. On the Alpine base
# (`schedr:1.0.0-alpine`), the builder's packages come from apk instead
# (`apk add --no-cache build-base`, then the -dev packages), and pip looks for `musllinux` wheels.
ARG SCHEDR=schedr:1.0.0

# The builder: the same base, with compilers and headers. Add the -dev packages a library needs
# (e.g. libpq-dev for psycopg's C build, libffi-dev for cffi).
FROM ${SCHEDR} AS builder
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip wheel --no-cache-dir --disable-pip-version-check --wheel-dir /wheels -r /tmp/requirements.txt

# The image: the base, and the built wheels.
FROM ${SCHEDR}
USER root
# The runtime libraries the built wheels link against, if any (e.g. libpq5):
# RUN apt-get update && apt-get install -y --no-install-recommends libpq5 \
#     && rm -rf /var/lib/apt/lists/*
COPY --from=builder /wheels /tmp/wheels
RUN pip install --no-cache-dir --disable-pip-version-check --root-user-action=ignore --no-index \
        --find-links /tmp/wheels /tmp/wheels/*.whl \
    && rm -rf /tmp/wheels
USER 1001
