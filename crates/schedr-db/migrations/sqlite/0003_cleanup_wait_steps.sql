-- Whether a task instance is a resource's cleanup (decision 225) or a wait step (a sensor, which
-- takes a watcher slot) (the user, 2026-11-19): kept on the row, each with an index of the queued
-- ones, for reads that want them apart from the other tasks. Nothing reads them yet.
ALTER TABLE task_instances ADD COLUMN cleanup INTEGER NOT NULL DEFAULT 0 CHECK (cleanup IN (0, 1));
ALTER TABLE task_instances ADD COLUMN wait_step INTEGER NOT NULL DEFAULT 0 CHECK (wait_step IN (0, 1));
CREATE INDEX task_instances_queued_cleanups ON task_instances (ready_at) WHERE cleanup = 1 AND state = 'queued';
CREATE INDEX task_instances_queued_wait_steps ON task_instances (ready_at) WHERE wait_step = 1 AND state = 'queued';
-- Instances stored before this migration, of runs not finished only (a history-wide statement
-- holds SQLite's write lock long on a large database): marked from their run's DAG version.
UPDATE task_instances SET cleanup = 1
WHERE run_id IN (SELECT id FROM runs WHERE state IN ('queued', 'running'))
    AND EXISTS (
        SELECT 1 FROM runs r JOIN dag_versions v ON v.id = r.dag_version_id,
            json_each(v.dag_json, '$.resources') res
        WHERE r.id = task_instances.run_id
            AND json_extract(res.value, '$.cleanup') = task_instances.task_id
    );
UPDATE task_instances SET wait_step = 1
WHERE run_id IN (SELECT id FROM runs WHERE state IN ('queued', 'running'))
    AND EXISTS (
        SELECT 1 FROM runs r JOIN dag_versions v ON v.id = r.dag_version_id,
            json_each(v.dag_json, '$.tasks') task
        WHERE r.id = task_instances.run_id
            AND json_extract(task.value, '$.id') = task_instances.task_id
            AND json_extract(task.value, '$.kind') = 'sensor'
    );
-- What a running attempt reported (decision 513): its statuses, each with its time, in order
-- (at most 100: past it the oldest after the first goes), and its links by label (at most 10).
CREATE TABLE attempt_statuses (
    attempt_id  TEXT NOT NULL REFERENCES attempts (id) ON DELETE CASCADE,
    seq         INTEGER NOT NULL CHECK (seq >= 0),
    at          TEXT NOT NULL,
    phase       TEXT NOT NULL,
    message     TEXT,
    progress    REAL CHECK (progress >= 0 AND progress <= 1),
    -- The extension whose plugin reported it (DN19): its page lists its attempts' phases.
    extension   TEXT NOT NULL DEFAULT '',
    -- Its version (DN40): a version's page lists its own attempts (B292).
    extension_version TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (attempt_id, seq)
);
-- By version too (B292): a version's page reads its own attempts' rows only.
CREATE INDEX attempt_statuses_extension ON attempt_statuses (extension, extension_version, at);
CREATE TABLE attempt_links (
    attempt_id  TEXT NOT NULL REFERENCES attempts (id) ON DELETE CASCADE,
    label       TEXT NOT NULL,
    url         TEXT NOT NULL,
    position    INTEGER NOT NULL,
    PRIMARY KEY (attempt_id, label)
);
-- The extensions workers ran (decision 513): each version's declarations (metrics, page) and
-- series cap as a worker last sent them, and when it was last heard of; and their metrics'
-- samples, one row a minute per worker and series (step 60: a counter's growth, a gauge's
-- value, a histogram's new observations), rolled up per 15 minutes across workers (step 900,
-- worker_id ''), kept as the processes' samples are.
CREATE TABLE extension_versions (
    name          TEXT NOT NULL,
    version       TEXT NOT NULL,
    worker_id     TEXT NOT NULL,
    declarations  TEXT NOT NULL,
    max_series    INTEGER NOT NULL CHECK (max_series >= 1),
    seen_at       TEXT NOT NULL,
    PRIMARY KEY (name, version, worker_id)
);
CREATE TABLE extension_samples (
    name       TEXT NOT NULL,
    version    TEXT NOT NULL,
    worker_id  TEXT NOT NULL,
    metric     TEXT NOT NULL,
    labels     TEXT NOT NULL,
    step_s     INTEGER NOT NULL,
    at         TEXT NOT NULL,
    data       TEXT NOT NULL,
    PRIMARY KEY (name, step_s, at, version, worker_id, metric, labels)
);
-- What happened to each extension on its workers (DN19): discovered, skipped, refused, dropped;
-- the newest 200 per extension kept.
CREATE TABLE extension_events (
    id         INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    worker_id  TEXT NOT NULL,
    version    TEXT NOT NULL,
    kind       TEXT NOT NULL CHECK (kind IN ('discovered', 'skipped', 'refused', 'dropped', 'notified', 'service_started', 'service_ready', 'service_crashed', 'service_memory_killed', 'service_turned_off', 'service_status', 'service_stopped')),
    at         TEXT NOT NULL,
    message    TEXT NOT NULL
);
CREATE INDEX extension_events_name ON extension_events (name, at);
-- Whether a run's RUN_STARTED was queued (M5.2i8, DN18): the round announces each run running
-- and not yet announced. Runs stored before this migration count as announced: an upgrade
-- tells of no run already going.
ALTER TABLE runs ADD COLUMN announced INTEGER NOT NULL DEFAULT 1 CHECK (announced IN (0, 1));
CREATE INDEX runs_unannounced ON runs (started_at, id) WHERE announced = 0 AND started_at IS NOT NULL;
-- Whether a posted event's EVENT_POSTED was queued (M5.2i8, DN18): the round announces each to
-- the DAGs listening for its name. Events posted before this migration count as announced.
ALTER TABLE named_events ADD COLUMN announced INTEGER NOT NULL DEFAULT 1 CHECK (announced IN (0, 1));
CREATE INDEX named_events_unannounced ON named_events (posted_at, id) WHERE announced = 0;

-- A worker's extensions' services' memory, a part of its own (B281): on average, as the other
-- parts are.
ALTER TABLE samples ADD COLUMN services_avg BIGINT;

-- Each run of a DAG's trigger on a worker (B272): where its log is (the worker, the instance its
-- log file is named by), when it started and ended, and why it failed. The newest ones of a DAG
-- are kept (`MASTER__TRIGGER_RUNS_KEPT`), the older pruned as one ends.
CREATE TABLE trigger_runs (
    instance_id  TEXT PRIMARY KEY,
    dag_id       TEXT NOT NULL,
    worker_id    TEXT NOT NULL,
    started_at   TEXT NOT NULL,
    ended_at     TEXT,
    error        TEXT
);
CREATE INDEX trigger_runs_newest ON trigger_runs (dag_id, started_at DESC, instance_id DESC);
