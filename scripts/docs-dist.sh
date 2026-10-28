#!/usr/bin/env bash
# The documentation the images serve at /docs (PROJECT.md, Web UI: decision 28A): the built
# site (`make docs`, or the CI's docs artifact) without the Rust API reference (for schedr's
# developers) and Sphinx's `_sources` copies, into dist/docs for the images' build.
# Usage: scripts/docs-dist.sh [<site directory>]   (default: site)
set -euo pipefail
site=${1:-site}
[ -f "$site/index.html" ] || { echo "no built site in $site: run make docs" >&2; exit 1; }
rm -rf dist/docs
mkdir -p dist
cp -r "$site" dist/docs
rm -rf dist/docs/rust dist/docs/_sources dist/docs/.buildinfo
echo "dist/docs: $(du -sh dist/docs | cut -f1)"
