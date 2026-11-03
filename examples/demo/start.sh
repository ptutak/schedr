#!/bin/sh
# A demo deployment's start (DEMO_ENV: dev or prod): its database migrated, its secrets set
# from what `make demo` generated, then schedr standalone without a UI.
set -eu
env=$DEMO_ENV
# Its own volume: none of the other deployment's files, none of the servers'.
own=/demo/own
SCHEDR_SECRET_KEY=$(cat "$own/secret_key")
export SCHEDR_SECRET_KEY
# The standalone's own SQLite file, named for `db migrate` and `secrets set` too.
data=${XDG_DATA_HOME:-$HOME/.local/share}/schedr
mkdir -p "$data"
export SCHEDR_CONFIG__DATABASE__URL="sqlite://$data/schedr.db"
schedr db migrate
for server in postgres mysql mariadb mongo; do
    schedr secrets set "demo/$server" user="${env}_app" password=@"$own/password" \
        database="$env" >/dev/null
done
schedr secrets set demo/api token=@"$own/token" >/dev/null
schedr secrets set demo/ssh key=@"$own/ssh_key" known_hosts=@"$own/known_hosts" >/dev/null
schedr secrets set demo/deployment name="$env" >/dev/null
exec schedr standalone --no-ui --listen 0.0.0.0
