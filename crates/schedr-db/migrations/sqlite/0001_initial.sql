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
    offline_at        TEXT,
    -- Drained for maintenance (`schedr workers drain`): no new tasks until resumed, across
    -- restarts of the worker and the master.
    drained           INTEGER NOT NULL DEFAULT 0 CHECK (drained IN (0, 1)),
    -- What is wrong with it now ([{kind, interpreter, message}]), replaced by each report.
    warnings          TEXT NOT NULL DEFAULT '[]'
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

-- When workers last switched to each code version of a root (a git-sync, a revert). The
-- newest code is the one switched to most recently (see PROJECT.md, Code versions and change
-- detection); kept so a restarted master decides the same.
CREATE TABLE root_hashes (
    root          TEXT NOT NULL,
    content_hash  TEXT NOT NULL,
    switched_at   TEXT NOT NULL,
    PRIMARY KEY (root, content_hash)
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
-- The runs list over every DAG, newest triggered first (the UI's runs page).
CREATE INDEX runs_by_trigger ON runs (triggered_at, id);

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
    -- When a user asked to cancel this task (`schedr tasks cancel`) while it ran.
    cancel_requested_at TEXT,
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
    -- What the process used (ProcessUsage as JSON); NULL if not read.
    usage             TEXT,
    core_dumped       INTEGER NOT NULL DEFAULT 0 CHECK (core_dumped IN (0, 1)),
    duration_ms       INTEGER,
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
    decided_at   TEXT,
    -- Set before the command's first write: one still pending with it was cut off partway
    -- (its round failed), so its next application may find its own work done (B7).
    applying_at  TEXT
);

CREATE INDEX commands_pending ON commands (requested_at) WHERE status = 'pending';
-- A run's commands: those naming it, the trigger that made it, the pauses that cancelled it.
CREATE INDEX commands_by_run ON commands ((payload ->> 'run_id'));
CREATE INDEX commands_by_result_run ON commands ((result ->> 'run_id'));
CREATE INDEX commands_pauses ON commands (requested_at) WHERE kind = 'pause_dag';

-- The master's lease: one master per database (see PROJECT.md, Components and roles:
-- production path). Also created before the migrations (`Database::create_lease_table`), so
-- a master holds it while it migrates.
CREATE TABLE IF NOT EXISTS master_lease (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    holder     TEXT NOT NULL,
    epoch      INTEGER NOT NULL,
    expires_at TEXT NOT NULL
);
