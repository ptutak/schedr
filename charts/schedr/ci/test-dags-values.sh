#!/usr/bin/env bash
# Tests charts/schedr/dags-values.sh with the chart (make chart-check): a flat folder's files go
# through the values and the ConfigMap back to the same bytes under the same names, and the pod
# spec doesn't list them; what is left out stays out; errors print nothing on stdout.
set -euo pipefail
CHART=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
fail() { echo "test-dags-values.sh: $*" >&2; exit 1; }

dags=$WORK/dags
mkdir -p "$dags/__pycache__" "$dags/.venv/lib" "$dags/.git" "$dags/data"
printf 'from schedr import Dag\n' > "$dags/first.py"
printf '    indented first line\n\tand a tab\n' > "$dags/spaces.py"
printf 'no final newline' > "$dags/last.py"
printf 'ŻÓŁW = "żółw: ünïcödé"\n' > "$dags/unicode.py"
: > "$dags/empty.py"
printf 'test_*.py\n' > "$dags/.schedrignore"
# A linked file counts as the file it links to.
printf 'linked = 1\n' > "$WORK/elsewhere.py"
ln -s "$WORK/elsewhere.py" "$dags/linked.py"
# Left out: a subfolder without Python files is no reason to refuse the folder.
for skipped in __pycache__/cached.py .venv/lib/site.py .git/hook.py .hidden.py notes.txt \
    data/rows.csv; do
    echo skipped > "$dags/$skipped"
done
kept=(.schedrignore empty.py first.py last.py linked.py spaces.py unicode.py)

"$CHART/dags-values.sh" "$dags" > "$WORK/values.yaml"
helm template t "$CHART" -f "$WORK/values.yaml" > "$WORK/rendered.yaml"

# name -> content (base64) from the ConfigMap.
sed -n '/^kind: ConfigMap/,/^---/s/^  "\([^"]*\)": "\(.*\)"$/\1 \2/p' "$WORK/rendered.yaml" \
    > "$WORK/contents"
[ "$(cut -d' ' -f1 "$WORK/contents" | tr '\n' ' ')" = "${kept[*]} " ] \
    || fail "files: $(cut -d' ' -f1 "$WORK/contents" | tr '\n' ' '), expected ${kept[*]}"
mkdir "$WORK/out"
while read -r name content; do
    printf '%s' "$content" | base64 -d > "$WORK/out/$name"
    cmp -s "$dags/$name" "$WORK/out/$name" || fail "$name: not the same bytes after the round trip"
done < "$WORK/contents"
# The pod's spec names none of them: adding or removing a file doesn't change it.
for name in "${kept[@]}"; do
    sed -n '/^kind: StatefulSet/,/^---/p' "$WORK/rendered.yaml" | grep -qF "$name" \
        && fail "the pod's spec names $name"
done
echo "ok: ${#kept[@]} files back under their names, byte for byte; none in the pod's spec"

# Errors: a message, a failure, and nothing on stdout.
expect_error() {
    local out
    if out=$("$CHART/dags-values.sh" "$@" 2>/dev/null); then fail "no error for $*"; fi
    [ -z "$out" ] || fail "output despite the error for $*"
}
mkdir -p "$WORK/none" "$WORK/bad name" "$WORK/big" "$WORK/nested/helpers" "$WORK/ignore-only" \
    "$WORK/linked-folder" "$WORK/package"
expect_error "$WORK/none"
expect_error "$WORK/missing"
printf 'x\n' > "$WORK/bad name/ok.py"; printf 'x\n' > "$WORK/bad name/a b.py"
expect_error "$WORK/bad name"
head -c $((600 * 1024)) /dev/zero > "$WORK/big/huge.py"
expect_error "$WORK/big"
printf 'x\n' > "$WORK/nested/top.py"; printf 'x\n' > "$WORK/nested/helpers/util.py"
expect_error "$WORK/nested"
printf 'x\n' > "$WORK/ignore-only/.schedrignore"
expect_error "$WORK/ignore-only"
printf 'x\n' > "$WORK/package/util.py"; printf 'x\n' > "$WORK/linked-folder/top.py"
ln -s "$WORK/package" "$WORK/linked-folder/helpers"
expect_error "$WORK/linked-folder"
expect_error
echo "ok: an empty folder, a missing one, a bad name, too much, a subfolder (also linked), only"
echo "    .schedrignore, no argument"
