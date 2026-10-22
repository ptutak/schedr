-- schedr schema v1 (Postgres). Keep in step with migrations/sqlite/0001_initial.sql.
--
-- Conventions (see PROJECT.md, State storage):
--   * times: TIMESTAMPTZ, always UTC (never naive TIMESTAMP);
--   * IDs: UUIDv7 as UUID;
--   * JSON: JSONB;
--   * states: TEXT checked against the model's names (schedr-model, states).

CREATE TABLE workers (
    worker_id         TEXT PRIMARY KEY,
    incarnation_id    UUID NOT NULL,
    state             TEXT NOT NULL CHECK (state IN ('active', 'draining', 'disconnected', 'offline', 'lost')),
    protocol_version  TEXT NOT NULL,
    schedr_version    TEXT NOT NULL,
    queues            JSONB NOT NULL,
    slots             INTEGER NOT NULL CHECK (slots >= 0),
    log_endpoint      TEXT NOT NULL,
    registered_at     TIMESTAMPTZ NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    offline_at        TIMESTAMPTZ
);

CREATE TABLE parse_results (
    id             UUID PRIMARY KEY,
    root           TEXT NOT NULL,
    content_hash   TEXT NOT NULL,
    schedr_version TEXT NOT NULL,
    worker_id      TEXT NOT NULL,
    parsed_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (root, content_hash, schedr_version)
);

-- Each file of a parse as the worker reported it (after the master's validation): its DAGs
-- (a JSON array of DAG JSON) or its import error (exception, message, traceback). The DAG list
-- is worked out from the current parse of each root (see PROJECT.md, DAG code delivery).
CREATE TABLE parse_files (
    parse_result_id UUID NOT NULL REFERENCES parse_results (id) ON DELETE CASCADE,
    path            TEXT NOT NULL,
    dags            JSONB NOT NULL,
    error           JSONB,
    PRIMARY KEY (parse_result_id, path)
);

CREATE TABLE dags (
    dag_id             TEXT PRIMARY KEY,
    paused             BOOLEAN NOT NULL DEFAULT TRUE,
    current_version_id UUID,
    first_seen_at      TIMESTAMPTZ NOT NULL,
    removed_at         TIMESTAMPTZ,
    -- Set while the DAG ID is defined in more than one place: a JSON array of {root, path}.
    conflict           JSONB
);

CREATE TABLE dag_versions (
    id              UUID PRIMARY KEY,
    dag_id          TEXT NOT NULL REFERENCES dags (dag_id),
    parse_result_id UUID NOT NULL REFERENCES parse_results (id),
    dag_json        JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (dag_id, parse_result_id)
);

CREATE TABLE runs (
    id             UUID PRIMARY KEY,
    dag_id         TEXT NOT NULL REFERENCES dags (dag_id),
    dag_version_id UUID NOT NULL REFERENCES dag_versions (id),
    kind           TEXT NOT NULL CHECK (kind IN ('scheduled', 'manual', 'backfill', 'chained', 'event')),
    state          TEXT NOT NULL CHECK (state IN ('queued', 'running', 'success', 'failed', 'cancelled')),
    planned_for    TIMESTAMPTZ NOT NULL,
    since          TIMESTAMPTZ NOT NULL,
    triggered_at   TIMESTAMPTZ NOT NULL,
    triggered_by   TEXT NOT NULL,
    parent_run_id  UUID REFERENCES runs (id),
    backfill_id    UUID,
    params         JSONB NOT NULL DEFAULT '{}',
    started_at     TIMESTAMPTZ,
    ended_at       TIMESTAMPTZ,
    result_reason  TEXT,
    -- Set when a user asked to cancel the run: nothing new starts, failures aren't retried.
    cancel_requested_at TIMESTAMPTZ
);

-- A master that crashes after creating a scheduled run can't create it twice.
CREATE UNIQUE INDEX runs_scheduled_once ON runs (dag_id, planned_for) WHERE kind = 'scheduled';
CREATE INDEX runs_by_dag ON runs (dag_id, planned_for);
CREATE INDEX runs_unfinished ON runs (state) WHERE state IN ('queued', 'running');

CREATE TABLE task_instances (
    id            UUID PRIMARY KEY,
    run_id        UUID NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
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
    ready_at      TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL,
    UNIQUE (run_id, task_id, map_index)
);

CREATE INDEX task_instances_ready ON task_instances (ready_at) WHERE state = 'queued';

CREATE TABLE attempts (
    id                UUID PRIMARY KEY,
    task_instance_id  UUID NOT NULL REFERENCES task_instances (id) ON DELETE CASCADE,
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
    peak_memory_bytes BIGINT,
    duration_ms       BIGINT,
    error_kind        TEXT,
    error_summary     JSONB,
    assigned_at       TIMESTAMPTZ NOT NULL,
    started_at        TIMESTAMPTZ,
    ended_at          TIMESTAMPTZ,
    UNIQUE (task_instance_id, try_number)
);

CREATE TABLE outputs (
    task_instance_id UUID PRIMARY KEY REFERENCES task_instances (id) ON DELETE CASCADE,
    outcome          TEXT NOT NULL,
    value            JSONB NOT NULL,
    size_bytes       BIGINT NOT NULL CHECK (size_bytes >= 0)
);

CREATE TABLE commands (
    id           UUID PRIMARY KEY,
    kind         TEXT NOT NULL,
    payload      JSONB NOT NULL,
    requested_by TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'applied', 'rejected')),
    reason       TEXT,
    -- What an applied command produced, e.g. the run a trigger created.
    result       JSONB,
    decided_at   TIMESTAMPTZ
);

CREATE INDEX commands_pending ON commands (requested_at) WHERE status = 'pending';
