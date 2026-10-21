-- schedr schema v1 (SQLite). Keep in step with migrations/postgres/0001_initial.sql.
--
-- Conventions (see PROJECT.md, State storage):
--   * times: TEXT in ISO 8601 / RFC 3339, always in UTC with `Z` (never naive);
--   * IDs: UUIDv7 as TEXT;
--   * JSON: TEXT holding JSON;
--   * states: TEXT checked against the model's names (schedr-model, states).

CREATE TABLE workers (
    worker_id         TEXT PRIMARY KEY,
    incarnation_id    TEXT NOT NULL,
    state             TEXT NOT NULL CHECK (state IN ('active', 'draining', 'disconnected', 'offline', 'lost')),
    protocol_version  TEXT NOT NULL,
    schedr_version    TEXT NOT NULL,
    queues            TEXT NOT NULL,
    slots             INTEGER NOT NULL CHECK (slots >= 0),
    log_endpoint      TEXT NOT NULL,
    registered_at     TEXT NOT NULL,
    last_heartbeat_at TEXT NOT NULL,
    offline_at        TEXT
);

CREATE TABLE parse_results (
    id             TEXT PRIMARY KEY,
    root           TEXT NOT NULL,
    content_hash   TEXT NOT NULL,
    schedr_version TEXT NOT NULL,
    worker_id      TEXT NOT NULL,
    parsed_at      TEXT NOT NULL,
    UNIQUE (root, content_hash, schedr_version)
);

-- Each file of a parse as the worker reported it (after the master's validation): its DAGs
-- (a JSON array of DAG JSON) or its import error (exception, message, traceback). The DAG list
-- is worked out from the current parse of each root (see PROJECT.md, DAG code delivery).
CREATE TABLE parse_files (
    parse_result_id TEXT NOT NULL REFERENCES parse_results (id) ON DELETE CASCADE,
    path            TEXT NOT NULL,
    dags            TEXT NOT NULL,
    error           TEXT,
    PRIMARY KEY (parse_result_id, path)
);

CREATE TABLE dags (
    dag_id             TEXT PRIMARY KEY,
    paused             INTEGER NOT NULL DEFAULT 1 CHECK (paused IN (0, 1)),
    current_version_id TEXT,
    first_seen_at      TEXT NOT NULL,
    removed_at         TEXT,
    -- Set while the DAG ID is defined in more than one place: a JSON array of {root, path}.
    conflict           TEXT
);

CREATE TABLE dag_versions (
    id              TEXT PRIMARY KEY,
    dag_id          TEXT NOT NULL REFERENCES dags (dag_id),
    parse_result_id TEXT NOT NULL REFERENCES parse_results (id),
    dag_json        TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    UNIQUE (dag_id, parse_result_id)
);

CREATE TABLE runs (
    id             TEXT PRIMARY KEY,
    dag_id         TEXT NOT NULL REFERENCES dags (dag_id),
    dag_version_id TEXT NOT NULL REFERENCES dag_versions (id),
    kind           TEXT NOT NULL CHECK (kind IN ('scheduled', 'manual', 'backfill', 'chained', 'event')),
    state          TEXT NOT NULL CHECK (state IN ('queued', 'running', 'success', 'failed', 'cancelled')),
    planned_for    TEXT NOT NULL,
    since          TEXT NOT NULL,
    triggered_at   TEXT NOT NULL,
    triggered_by   TEXT NOT NULL,
    parent_run_id  TEXT REFERENCES runs (id),
    backfill_id    TEXT,
    params         TEXT NOT NULL DEFAULT '{}',
    started_at     TEXT,
    ended_at       TEXT,
    result_reason  TEXT,
    -- Set when a user asked to cancel the run: nothing new starts, failures aren't retried.
    cancel_requested_at TEXT
);

-- A master that crashes after creating a scheduled run can't create it twice.
CREATE UNIQUE INDEX runs_scheduled_once ON runs (dag_id, planned_for) WHERE kind = 'scheduled';
CREATE INDEX runs_by_dag ON runs (dag_id, planned_for);
CREATE INDEX runs_unfinished ON runs (state) WHERE state IN ('queued', 'running');

CREATE TABLE task_instances (
    id            TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    task_id       TEXT NOT NULL,
    -- -1 for a task that isn't mapped (NULL would defeat the uniqueness below).
    map_index     INTEGER NOT NULL DEFAULT -1 CHECK (map_index >= -1),
    state         TEXT NOT NULL CHECK (state IN (
        'scheduled', 'queued', 'unschedulable', 'running', 'cleaning_up', 'up_for_retry',
        'success', 'failed', 'cancelled', 'skipped', 'upstream_failed')),
    outcome       TEXT,
    try_number    INTEGER NOT NULL DEFAULT 0 CHECK (try_number >= 0),
    -- Attempts that used a retry (try_number also counts those that didn't).
    retries_used  INTEGER NOT NULL DEFAULT 0 CHECK (retries_used >= 0),
    queued_reason TEXT,
    ready_at      TEXT,
    updated_at    TEXT NOT NULL,
    UNIQUE (run_id, task_id, map_index)
);

CREATE INDEX task_instances_ready ON task_instances (ready_at) WHERE state = 'queued';

CREATE TABLE attempts (
    id                TEXT PRIMARY KEY,
    task_instance_id  TEXT NOT NULL REFERENCES task_instances (id) ON DELETE CASCADE,
    try_number        INTEGER NOT NULL CHECK (try_number >= 1),
    worker_id         TEXT,
    -- NULL while the attempt runs.
    status            TEXT CHECK (status IN (
        'success', 'failed', 'timed_out', 'oom_killed', 'cancelled', 'interrupted', 'lost',
        'assignment_expired')),
    start_method      TEXT CHECK (start_method IN ('spawn', 'fork')),
    code_hash         TEXT,
    exit_code         INTEGER,
    signal            INTEGER,
    peak_memory_bytes INTEGER,
    error_kind        TEXT,
    error_summary     TEXT,
    assigned_at       TEXT NOT NULL,
    started_at        TEXT,
    ended_at          TEXT,
    UNIQUE (task_instance_id, try_number)
);

CREATE TABLE outputs (
    task_instance_id TEXT PRIMARY KEY REFERENCES task_instances (id) ON DELETE CASCADE,
    outcome          TEXT NOT NULL,
    value            TEXT NOT NULL,
    size_bytes       INTEGER NOT NULL CHECK (size_bytes >= 0)
);

CREATE TABLE commands (
    id           TEXT PRIMARY KEY,
    kind         TEXT NOT NULL,
    payload      TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'applied', 'rejected')),
    reason       TEXT,
    -- What an applied command produced, e.g. the run a trigger created.
    result       TEXT,
    decided_at   TEXT
);

CREATE INDEX commands_pending ON commands (requested_at) WHERE status = 'pending';
