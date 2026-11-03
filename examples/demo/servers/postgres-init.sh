#!/bin/sh
# Postgres's first start: a database and a user for each deployment, its greeting in it. psql
# reads the password itself (`\set pw `cat ...``), so it is on no command line.
set -eu
for env in dev prod; do
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -v env="$env" -v user="${env}_app" <<'SQL'
\set pw `cat /demo/servers/:env/password`
CREATE USER :"user" PASSWORD :'pw';
CREATE DATABASE :"env" OWNER :"user";
SQL
    psql -v ON_ERROR_STOP=1 -U "${env}_app" -d "$env" -v env="$env" <<'SQL'
CREATE TABLE greetings (greeting text);
INSERT INTO greetings VALUES ('hello from ' || :'env' || '''s Postgres');
SQL
done
