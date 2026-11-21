#!/bin/sh
# An extension's plugin made of a shell script (M5.2i6, decision 519): `schedr extension shell`
# speaks the plugin protocol and runs it for each task of the `backup` extension.
#
# Configure it on a worker:
#
#   [extensions.backup]
#   command  = ["schedr", "extension", "shell", "--", "/opt/ops/backup.sh"]
#   versions = ["1.0"]
#   metrics  = [{name = "files_copied", kind = "counter", help = "Files copied", labels = [{name = "kind"}]}]
#   panels   = [{title = "Files copied", metric = "files_copied", chart = "bars", row = 1, cell = "full"}]
#
# and use it in a DAG:
#
#   ExtensionTask("nightly", name="backup", version="1.0", payload={"source": "/data"},
#                 secrets=["backup/target"], cleanup=True)
#
# The payload comes as JSON on stdin; the run as SCHEDR_* variables (SCHEDR_RUN_ID, ...);
# stdout and stderr are the attempt's log; exit 0 succeeds, with the output it set. A cleanup
# runs this script again with SCHEDR_CLEANUP=1. A real script would read the payload with `jq`;
# this one only looks for words in it.
set -eu

if [ "${SCHEDR_CLEANUP:-}" = 1 ]; then
    # The handles the attempt registered, whatever its end: remove what it left.
    for snapshot in $(schedr task-ctx cleanup list Snapshot --field id); do
        echo "removing snapshot $snapshot"
    done
    exit 0
fi

payload=$(cat)
echo "backing up $payload, run $SCHEDR_RUN_ID, try $SCHEDR_TRY_NUMBER"

# A stop (cancel, timeout, drain) reaches the script as SIGTERM; it may ask why.
trap 'echo "stopped: $(schedr task-ctx stop-reason --field reason)"; exit 143' TERM

schedr task-ctx status preparing --progress 0.1
token=$(schedr task-ctx secret get "backup/target#token")
echo "the target's token has ${#token} characters"

snapshot="snap-$SCHEDR_RUN_ID"
schedr task-ctx cleanup add Snapshot "id=$snapshot"
schedr task-ctx link Snapshot "https://backups.example/snapshots/$snapshot"

schedr task-ctx status copying --message "3 files" --progress 0.5
schedr task-ctx metric files_copied 3 --label kind=full
schedr task-ctx log "files copied" --field count=3

case "$payload" in
*'"slow"'*)
    # Long enough to be stopped; `sleep` in the background so the trap runs at once.
    sleep 300 &
    wait $!
    ;;
*'"fail"'*)
    echo "the target refused the copy" >&2
    exit 3
    ;;
esac

schedr task-ctx status done --progress 1
schedr task-ctx extension output set "{\"snapshot\": \"$snapshot\", \"files\": 3}"
