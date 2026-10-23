# Your image: the schedr base image plus your DAGs' libraries (see PROJECT.md, Components and
# roles: production path). Build the base first (`make image`), then from this directory:
#   podman build --build-arg SCHEDR=schedr:0.1.0 -t my-schedr:1 .
#
# The base is Alpine (musl). pip takes `musllinux` wheels where a library publishes them (numpy,
# pandas and most drivers do); for the rest, the builder stage compiles wheels, and the final
# image installs only those wheels, with no compiler in it. A library that can't build on musl
# shows here, when the image is built: pin a version that has a musl wheel, take it from apk, or
# choose another library.
ARG SCHEDR=schedr:0.1.0

# The builder: the same Alpine, with compilers and headers. Add the -dev packages a library
# needs (e.g. postgresql-dev for psycopg's C build, libffi-dev for cffi).
FROM ${SCHEDR} AS builder
USER root
RUN apk add --no-cache build-base
COPY requirements.txt /tmp/requirements.txt
RUN pip wheel --no-cache-dir --disable-pip-version-check --wheel-dir /wheels -r /tmp/requirements.txt

# The image: the base, and the built wheels.
FROM ${SCHEDR}
USER root
# The runtime libraries the built wheels link against, if any (e.g. libstdc++, libpq).
# RUN apk add --no-cache libstdc++
COPY --from=builder /wheels /tmp/wheels
RUN pip install --no-cache-dir --disable-pip-version-check --root-user-action=ignore --no-index \
        --find-links /tmp/wheels /tmp/wheels/*.whl \
    && rm -rf /tmp/wheels
USER 1001
