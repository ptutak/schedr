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
    offline_at        TIMESTAMPTZ,
    -- Drained for maintenance (`schedr workers drain`): no new tasks until resumed, across
    -- restarts of the worker and the master.
    drained           BOOLEAN NOT NULL DEFAULT FALSE,
    -- What is wrong with it now ([{kind, interpreter, message}]), replaced by each report.
    warnings          JSONB NOT NULL DEFAULT '[]'
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

-- When workers last switched to each code version of a root (a git-sync, a revert). The
-- newest code is the one switched to most recently (see PROJECT.md, Code versions and change
-- detection); kept so a restarted master decides the same.
CREATE TABLE root_hashes (
    root          TEXT NOT NULL,
    content_hash  TEXT NOT NULL,
    switched_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (root, content_hash)
);

-- Each file of a parse as the worker reported it (after the master's validation): its DAGs
-- (a JSON array of DAG JSON) or its import error (exception, message, traceback). The DAG list
-- is worked out from the current parse of each root (see PROJECT.md, DAG code delivery).
CREATE TABLE parse_files (
    parse_result_id UUID NOT NULL REFERENCES parse_results (id) ON DELETE CASCADE,
    path            TEXT NOT NULL,
    dags            JSONB NOT NULL,
    error           JSONB,
    -- Who parsed this file and when: the parse's worker, or the one that parsed it again
    -- after it failed transiently (decisions 72.2, 93).
    worker_id       TEXT NOT NULL,
    parsed_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (parse_result_id, path)
);

-- Each root's current parse, as the master decided it (from what alive workers report; see
-- PROJECT.md, Code versions and change detection): written so the API can show its import
-- errors (decision 66) without deciding again.
CREATE TABLE current_parses (
    root            TEXT PRIMARY KEY,
    parse_result_id UUID NOT NULL REFERENCES parse_results (id) ON DELETE CASCADE,
    decided_at      TIMESTAMPTZ NOT NULL
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
-- The runs list over every DAG, newest triggered first (the UI's runs page).
CREATE INDEX runs_by_trigger ON runs (triggered_at, id);

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
    -- When a user asked to cancel this task (`schedr tasks cancel`) while it ran.
    cancel_requested_at TIMESTAMPTZ,
    -- Why it ended without an attempt (a mapped task: too many instances, a mapped list that
    -- isn't a list); see PROJECT.md, decision 78.
    reason        TEXT,
    -- A mapped task as a whole (map_index -1) once expanded: the size of each dimension of
    -- its instances (a JSON array), for an instance's place in each.
    map_sizes     JSONB,
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
    -- What the process used (ProcessUsage as JSON); NULL if not read.
    usage             JSONB,
    core_dumped       BOOLEAN NOT NULL DEFAULT FALSE,
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
    decided_at   TIMESTAMPTZ,
    -- Set before the command's first write: one still pending with it was cut off partway
    -- (its round failed), so its next application may find its own work done (B7).
    applying_at  TIMESTAMPTZ
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
    expires_at TIMESTAMPTZ NOT NULL
);

-- What changed, for the UI's live pages (see PROJECT.md, Web UI: 34A): one notification per
-- changed run, DAG or the workers, sent when the writing transaction commits (Postgres drops
-- a transaction's duplicates). `run <run id> <dag id>`, `dag <dag id>`, `workers`; DAG IDs
-- have no spaces. Statement triggers: a round updating many rows notifies each run once.
CREATE FUNCTION schedr_runs_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('schedr_changes', 'run ' || c.id::text || ' ' || c.dag_id)
    FROM (SELECT DISTINCT id, dag_id FROM changed) c;
    RETURN NULL;
END $$;
CREATE TRIGGER runs_inserted AFTER INSERT ON runs REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION schedr_runs_changed();
CREATE TRIGGER runs_updated AFTER UPDATE ON runs REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION schedr_runs_changed();

CREATE FUNCTION schedr_task_instances_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('schedr_changes', 'run ' || r.id::text || ' ' || r.dag_id)
    FROM (SELECT DISTINCT run_id FROM changed) c JOIN runs r ON r.id = c.run_id;
    RETURN NULL;
END $$;
CREATE TRIGGER task_instances_inserted AFTER INSERT ON task_instances
    REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION schedr_task_instances_changed();
CREATE TRIGGER task_instances_updated AFTER UPDATE ON task_instances
    REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION schedr_task_instances_changed();

CREATE FUNCTION schedr_attempts_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('schedr_changes', 'run ' || r.id::text || ' ' || r.dag_id)
    FROM (SELECT DISTINCT task_instance_id FROM changed) c
    JOIN task_instances t ON t.id = c.task_instance_id JOIN runs r ON r.id = t.run_id;
    RETURN NULL;
END $$;
CREATE TRIGGER attempts_inserted AFTER INSERT ON attempts REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION schedr_attempts_changed();
CREATE TRIGGER attempts_updated AFTER UPDATE ON attempts REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION schedr_attempts_changed();

CREATE FUNCTION schedr_dags_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('schedr_changes', 'dag ' || c.dag_id)
    FROM (SELECT DISTINCT dag_id FROM changed) c;
    RETURN NULL;
END $$;
CREATE TRIGGER dags_inserted AFTER INSERT ON dags REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION schedr_dags_changed();
CREATE TRIGGER dags_updated AFTER UPDATE ON dags REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION schedr_dags_changed();

-- Only when a row changed: a statement trigger also fires for an UPDATE matching nothing (a
-- heartbeat of a worker not registered).
CREATE FUNCTION schedr_workers_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM changed) THEN
        PERFORM pg_notify('schedr_changes', 'workers');
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER workers_inserted AFTER INSERT ON workers REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION schedr_workers_changed();
CREATE TRIGGER workers_updated AFTER UPDATE ON workers REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION schedr_workers_changed();
CREATE TRIGGER workers_deleted AFTER DELETE ON workers REFERENCING OLD TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION schedr_workers_changed();
