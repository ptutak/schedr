-- What the stress suite's databases ran (`make stress STRESS_STATEMENTS=1`, M5.1d): per
-- database the transactions and the statements in them, then the statements run most often
-- and those that took longest. Postgres loads `pg_stat_statements` for it.
\pset pager off
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

\echo 'Per database: transactions, statements (BEGIN/COMMIT and the like left out), per transaction'
SELECT d.datname AS database,
       s.xact_commit + s.xact_rollback AS transactions,
       st.statements,
       round(st.statements::numeric / nullif(s.xact_commit + s.xact_rollback, 0), 2) AS per_transaction,
       round(st.ms::numeric, 0) AS statement_ms
FROM pg_database d
JOIN pg_stat_database s ON s.datid = d.oid
JOIN LATERAL (
    SELECT sum(p.calls) AS statements, sum(p.total_exec_time) AS ms
    FROM pg_stat_statements p
    WHERE p.dbid = d.oid
      AND p.query !~* '^\s*(begin|commit|rollback|savepoint|release|set|show|listen|unlisten)\M'
) st ON true
WHERE d.datname NOT IN ('postgres', 'template0', 'template1')
ORDER BY 1;

\echo 'The statements run most often'
SELECT d.datname AS database, p.calls, round(p.total_exec_time::numeric, 0) AS total_ms,
       round(p.mean_exec_time::numeric, 2) AS mean_ms, p.rows,
       left(regexp_replace(p.query, '\s+', ' ', 'g'), 200) AS query
FROM pg_stat_statements p
JOIN pg_database d ON d.oid = p.dbid
WHERE d.datname NOT IN ('postgres', 'template0', 'template1')
ORDER BY p.calls DESC
LIMIT 60;

\echo 'The statements that took longest in all'
SELECT d.datname AS database, p.calls, round(p.total_exec_time::numeric, 0) AS total_ms,
       round(p.mean_exec_time::numeric, 2) AS mean_ms, p.rows,
       left(regexp_replace(p.query, '\s+', ' ', 'g'), 200) AS query
FROM pg_stat_statements p
JOIN pg_database d ON d.oid = p.dbid
WHERE d.datname NOT IN ('postgres', 'template0', 'template1')
ORDER BY p.total_exec_time DESC
LIMIT 30;
