#!/bin/sh
# `make demo`'s first step, once: each deployment's password (for every database), API token,
# secret key and SSH key, the SSH server's host key and the known_hosts line trusting it.
# They go into the demo's volumes (the container engine's storage, kept until `make
# demo-clean`), one per reader, so each container sees only what it needs:
#
#   /demo/dev, /demo/prod  a deployment's own: its password, token, secret key, SSH key and
#                          known_hosts (user 1001 only)
#   /demo/servers          the databases' first start: both deployments' passwords and the
#                          servers' administrator password
#   /demo/ssh-users        the SSH server: the deployments' public keys
#   /demo/ssh-host         the SSH server: its host key
#
# Run as root (a new volume is root's). An earlier run that stopped half-way is started over.
set -eu
[ -e /demo/servers/ready ] && exit 0
for volume in /demo/dev /demo/prod /demo/servers /demo/ssh-users /demo/ssh-host; do
    find "$volume" -mindepth 1 -delete
done
umask 077
random() { head -c "$1" /dev/urandom | base64 | tr '+/' '-_' | tr -d '\n'; }
ssh-keygen -q -t ed25519 -N '' -C demo-ssh -f /demo/ssh-host/ssh_host_ed25519_key
known_hosts=$(printf '[ssh]:2222 %s\n' "$(cut -d ' ' -f 1,2 /demo/ssh-host/ssh_host_ed25519_key.pub)")
mkdir /demo/servers/dev /demo/servers/prod
for env in dev prod; do
    own=/demo/$env
    random 18 > "$own/password"
    random 24 > "$own/token"
    schedr secrets new-key > "$own/secret_key"
    ssh-keygen -q -t ed25519 -N '' -C "demo-$env" -f "$own/ssh_key"
    printf '%s\n' "$known_hosts" > "$own/known_hosts"
    cp "$own/password" "/demo/servers/$env/password"
    mv "$own/ssh_key.pub" "/demo/ssh-users/$env.pub"
    chown -R 1001:0 "$own"
    chmod 700 "$own"
done
# The servers' own administrators (Postgres, MySQL, MariaDB, Mongo), never the demo's.
random 18 > /demo/servers/admin_password
# The servers read them as users of their own (Postgres's, MySQL's, ...): readable, in
# volumes no deployment mounts.
chmod 755 /demo/servers /demo/servers/dev /demo/servers/prod /demo/ssh-users
chmod 644 /demo/servers/admin_password /demo/servers/*/password /demo/ssh-users/*.pub
touch /demo/servers/ready
