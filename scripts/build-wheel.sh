#!/usr/bin/env bash
# Build one release wheel into dist/. CI and `make smoke` / `make smoke-musl` use this.
#
#   scripts/build-wheel.sh manylinux-x86_64
#   scripts/build-wheel.sh manylinux-aarch64
#   scripts/build-wheel.sh musllinux-x86_64
#   scripts/build-wheel.sh macos-arm64
#
# Linux builds run in a digest-pinned image. The container tool is $CONTAINER, else podman
# when it is installed, else docker (podman locally, docker in CI). macOS builds on the
# host with the same rustup-init (aarch64-apple-darwin). See PROJECT.md, Packaging and releases.

set -euo pipefail

# Tag 2026.10.16-1, pushed 2026-10-16 (at least 7 days old on 2026-10-26). The pull is by digest.
MANYLINUX_X86_64_IMAGE=quay.io/pypa/manylinux2014_x86_64@sha256:2bd80fa8fca6b775a913b7e42fa410c5f2246fb6f7f8f3280b5d562e5ca5d63f
MANYLINUX_AARCH64_IMAGE=quay.io/pypa/manylinux2014_aarch64@sha256:01984696ca71627d9f61991da836e5d542aa21695e8337ab0dc1f27a4559502f
MUSLLINUX_X86_64_IMAGE=quay.io/pypa/musllinux_1_2_x86_64@sha256:915d73c75b4551f98fd7b12de655ef2e27419a6441ea07472b6e25e0d868d18d

# Published at static.rust-lang.org/rustup/archive/1.29.1/<triple>/rustup-init.sha256
# (archive last-modified 2026-09-01; the binary reports 2026-08-13 / 2026-08-18).
RUSTUP_VERSION=1.29.1

repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"

venv=
cargo_home=
rustup_home=

cleanup() {
    if [[ -n $venv && -d $venv ]]; then
        rm -rf "$venv"
    fi
    # Only the directories this script created. A caller's CARGO_HOME is not one of them.
    if [[ -n $cargo_home && -d $cargo_home ]]; then
        rm -rf "$cargo_home"
    fi
    if [[ -n $rustup_home && -d $rustup_home ]]; then
        rm -rf "$rustup_home"
    fi
}
trap cleanup EXIT

fail() {
    echo "build-wheel: $*" >&2
    exit 1
}

usage() {
    echo "usage: scripts/build-wheel.sh manylinux-x86_64|manylinux-aarch64|musllinux-x86_64|macos-arm64" >&2
    exit 2
}

# sha256 of rustup-init for one host triple, from the published .sha256 file.
rustup_sha256() {
    case $1 in
        x86_64-unknown-linux-gnu) echo dda7234360b7f578ca8b0ddcb80145646fa61a67c1720a5abc7051b35c9fcb71 ;;
        aarch64-unknown-linux-gnu) echo 15f6e4ce9f583b929c996c91562bad6d4454f3281de858b02cdfdef615fac433 ;;
        x86_64-unknown-linux-musl) echo 331228566cc931f32cd684f9bebc5956a5e5d7394c8d5de595f196a658ab98e6 ;;
        aarch64-apple-darwin) echo ec1b9233e7f72990ecd8e62063fa7f6c3dfc2bec8e97f88bff165f9100ac696a ;;
        *) fail "no published rustup-init sha256 for $1" ;;
    esac
}

# The channel in rust-toolchain.toml. Components in that file (rustfmt, clippy,
# llvm-tools) are for CI, not for a release wheel.
toolchain_channel() {
    local channel
    channel=$(sed -n 's/^channel = "\([^"]*\)"/\1/p' rust-toolchain.toml | head -1)
    [[ -n $channel ]] || fail "rust-toolchain.toml has no channel = \"...\""
    printf '%s\n' "$channel"
}

check_sha256() {
    local file=$1 expected=$2
    if command -v sha256sum >/dev/null 2>&1; then
        echo "${expected}  ${file}" | sha256sum -c -
    elif command -v shasum >/dev/null 2>&1; then
        echo "${expected}  ${file}" | shasum -a 256 -c -
    else
        fail "sha256sum or shasum is not available; rustup-init is checked with it"
    fi
}

# rustup-init installs nothing (`--default-toolchain none`): never `stable`.
# The compiler is the file's channel at profile minimal, and nothing else.
install_rustup() {
    local triple=$1
    local expected dest channel
    expected=$(rustup_sha256 "$triple")
    command -v curl >/dev/null 2>&1 || fail "curl is not available; rustup-init is downloaded with it"
    # The file must be named rustup-init. A mktemp name makes it look for a proxy called `tmp`.
    dest=$(mktemp -d)
    curl --proto '=https' --tlsv1.2 -fsSL \
        "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/${triple}/rustup-init" \
        -o "$dest/rustup-init"
    check_sha256 "$dest/rustup-init" "$expected"
    chmod +x "$dest/rustup-init"
    "$dest/rustup-init" -y --default-toolchain none --profile minimal --no-modify-path
    rm -rf "$dest"
    # shellcheck disable=SC1091
    source "${CARGO_HOME}/env"
    channel=$(toolchain_channel)
    # Install away from the repo. In the repo, rustup would also install the file's
    # rustfmt, clippy and llvm-tools. `--no-self-update` keeps this rustup-init:
    # without it the command downloads the current rustup (1.29.1 became 1.29.2).
    (
        cd /tmp
        rustup install --profile minimal --no-self-update "$channel"
    )
    # A later rustc must not sync those components just because the file is here.
    export RUSTUP_TOOLCHAIN="$channel"
    rustc --version | grep -q "$channel" || fail "rustc is not ${channel}: $(rustc --version)"
    if rustup component list --installed | grep -Eq 'rustfmt|clippy|llvm-tools'; then
        fail "a release build installed rustfmt, clippy or llvm-tools; only the minimal profile is needed"
    fi
}

require_patchelf() {
    if ! command -v patchelf >/dev/null 2>&1; then
        fail "patchelf is not in this image. maturin needs the image's patchelf; it is not installed from the network"
    fi
    echo "patchelf: $(command -v patchelf) ($(patchelf --version))"
}

# Interpreter that runs maturin. The manylinux image has no python3 on PATH; both images
# ship CPython 3.14 under /opt/python.
python_for_maturin() {
    local py=/opt/python/cp314-cp314/bin/python
    if [[ ! -x $py ]]; then
        fail "no $py in this image (found: $(ls /opt/python 2>/dev/null | tr '\n' ' ')). maturin is installed with that interpreter"
    fi
    echo "$py"
}

install_maturin() {
    local py=$1
    "$py" -m pip install --disable-pip-version-check --root-user-action=ignore \
        --require-hashes --only-binary=maturin \
        -r scripts/maturin-requirements.txt
}

build_in_container() {
    local platform=$1
    local rustup_triple target compatibility py
    case $platform in
        manylinux-x86_64)
            rustup_triple=x86_64-unknown-linux-gnu
            target=x86_64-unknown-linux-gnu
            compatibility=manylinux2014
            ;;
        manylinux-aarch64)
            rustup_triple=aarch64-unknown-linux-gnu
            target=aarch64-unknown-linux-gnu
            compatibility=manylinux2014
            ;;
        musllinux-x86_64)
            rustup_triple=x86_64-unknown-linux-musl
            target=x86_64-unknown-linux-musl
            compatibility=musllinux_1_2
            ;;
        *) fail "not a Linux platform: $platform" ;;
    esac
    export CARGO_HOME=/tmp/schedr-cargo
    export RUSTUP_HOME=/tmp/schedr-rustup
    require_patchelf
    install_rustup "$rustup_triple"
    py=$(python_for_maturin)
    install_maturin "$py"
    "$py" -m maturin build --release --locked --out dist --target "$target" --compatibility "$compatibility"
}

build_on_macos() {
    local py
    # Same installer as Linux, not the runner's rustup. The directories are this
    # job's; the runner's own rustup is left alone. cleanup removes these two.
    cargo_home=$(mktemp -d)
    rustup_home=$(mktemp -d)
    export CARGO_HOME=$cargo_home
    export RUSTUP_HOME=$rustup_home
    install_rustup aarch64-apple-darwin
    py=${PYTHON:-python3}
    command -v "$py" >/dev/null 2>&1 || fail "python interpreter '$py' not found (set PYTHON); maturin is installed with it"
    venv=$(mktemp -d)
    "$py" -m venv "$venv"
    install_maturin "$venv/bin/python"
    "$venv/bin/maturin" build --release --locked --out dist
}

# $CONTAINER if set, else podman when installed, else docker.
container_tool() {
    if [[ -n ${CONTAINER:-} ]]; then
        if ! command -v "$CONTAINER" >/dev/null 2>&1; then
            fail "CONTAINER=$CONTAINER is not on PATH"
        fi
        echo "$CONTAINER"
        return
    fi
    if command -v podman >/dev/null 2>&1; then
        echo podman
    elif command -v docker >/dev/null 2>&1; then
        echo docker
    else
        fail "no container tool: install podman (or docker), or set CONTAINER. Linux wheels are built in a pinned image"
    fi
}

host_arch() {
    case $1 in
        manylinux-x86_64 | musllinux-x86_64) echo x86_64 ;;
        manylinux-aarch64) echo aarch64 ;;
        macos-arm64) echo arm64 ;;
        *) fail "unknown platform: $1 (expected manylinux-x86_64, manylinux-aarch64, musllinux-x86_64 or macos-arm64)" ;;
    esac
}

run_container() {
    local platform=$1
    local image=$2
    local tool volume
    local need
    need=$(host_arch "$platform")
    if [[ $(uname -m) != "$need" ]]; then
        fail "$platform is built on $need (this machine is $(uname -m)). aarch64 and macOS wheels are built in CI"
    fi
    tool=$(container_tool)
    # podman on SELinux needs :Z. docker in CI does not.
    if [[ $tool == podman ]]; then
        volume="$repo:/io:Z"
    else
        volume="$repo:/io"
    fi
    mkdir -p "$repo/dist"
    # CARGO_TARGET_DIR stays inside the container so the host target/ is untouched.
    "$tool" run --rm \
        -v "$volume" \
        -w /io \
        -e CARGO_TARGET_DIR=/tmp/schedr-wheel-target \
        "$image" \
        bash scripts/build-wheel.sh --inside "$platform"
}

image_for_platform() {
    case $1 in
        manylinux-x86_64) echo "$MANYLINUX_X86_64_IMAGE" ;;
        manylinux-aarch64) echo "$MANYLINUX_AARCH64_IMAGE" ;;
        musllinux-x86_64) echo "$MUSLLINUX_X86_64_IMAGE" ;;
        *) fail "no image for $1" ;;
    esac
}

inside=0
if [[ ${1:-} == --inside ]]; then
    inside=1
    shift
fi

platform=${1:-}
[[ -n $platform ]] || usage
[[ $# -eq 1 ]] || usage

case $platform in
    manylinux-x86_64 | manylinux-aarch64 | musllinux-x86_64)
        if [[ $inside -eq 1 ]]; then
            build_in_container "$platform"
        else
            run_container "$platform" "$(image_for_platform "$platform")"
        fi
        ;;
    macos-arm64)
        [[ $inside -eq 0 ]] || fail "macos-arm64 is built on the host, not in a container"
        need=$(host_arch "$platform")
        [[ $(uname -m) == "$need" ]] || fail "$platform is built on $need (this machine is $(uname -m))"
        mkdir -p dist
        build_on_macos
        ;;
    *) usage ;;
esac
