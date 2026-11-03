#!/bin/sh
# MySQL's or MariaDB's first start (CLIENT: mysql or mariadb): a database and a user for each
# deployment, its greeting in it. The passwords are URL-safe base64: nothing to quote. The
# administrator's goes in an option file for the client (`printf` is the shell's own), never
# on its command line; the deployments' in its input.
set -eu
client=${CLIENT:-mysql}
umask 077
login=$(mktemp)
trap 'rm -f "$login"' EXIT
printf '[client]\nuser=root\npassword=%s\n' \
    "${MYSQL_ROOT_PASSWORD:-${MARIADB_ROOT_PASSWORD:-}}" > "$login"
for env in dev prod; do
    password=$(cat "/demo/servers/$env/password")
    "$client" --defaults-extra-file="$login" <<SQL
CREATE DATABASE $env;
CREATE USER '${env}_app'@'%' IDENTIFIED BY '$password';
GRANT ALL ON $env.* TO '${env}_app'@'%';
CREATE TABLE $env.greetings (greeting TEXT);
INSERT INTO $env.greetings VALUES ('hello from $env''s $client');
SQL
done
