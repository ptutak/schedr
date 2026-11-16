-- Decision 485: a scheduled DAG's next fire time, stored so that the master's probe finds it:
-- set by the scheduler after each visit, cleared (NULL: not known, visit it) by whatever
-- changes the schedule (a new version, a pause or unpause). 9999-01-01T00:00:00Z
-- (`schedr_db::dags::NO_FIRE`) is "no fire time ahead".
ALTER TABLE dag_triggers ADD COLUMN next_fire_at TEXT;
CREATE INDEX dag_triggers_next_fire ON dag_triggers (next_fire_at);

-- Decision 486: the name of the event an `EventNamed` trigger waits for (NULL for any other
-- kind), written with the trigger's row, so that posting an event clears the stored next look
-- (`next_fire_at`) of only the DAGs that wait for that name, and the master's probe finds them.
ALTER TABLE dag_triggers ADD COLUMN event_name TEXT;
CREATE INDEX dag_triggers_event_name ON dag_triggers (event_name);
-- Rows stored before this migration: filled from each DAG's current definition (Grok's review
-- of M5.1f2: an upgraded database's posts found no listener until the DAG was parsed again).
UPDATE dag_triggers SET event_name = (
    SELECT json_extract(v.dag_json, '$.trigger.event.name')
    FROM dags d JOIN dag_versions v ON v.id = d.current_version_id
    WHERE d.dag_id = dag_triggers.dag_id
        AND json_extract(v.dag_json, '$.trigger.kind') = 'event_named')
WHERE kind = 'event';

-- Decision 487: a `wait_for` step the master holds, while it waits, stores when the round looks
-- at it next (`look_at`, its `max_wait` deadline; NULL: at once) and what it waits for: another
-- DAG's run or task ending (`waits_dag`), or a named event posted (`waits_event`). The ready
-- check skips it until then; that DAG's run or task ending, or that event posted, clears
-- `look_at` (the triggers below; the post itself), and so does the step's own state changing.
ALTER TABLE task_instances ADD COLUMN look_at TEXT;
ALTER TABLE task_instances ADD COLUMN waits_dag TEXT;
ALTER TABLE task_instances ADD COLUMN waits_event TEXT;
CREATE INDEX task_instances_waits_dag ON task_instances (waits_dag) WHERE look_at IS NOT NULL;
CREATE INDEX task_instances_waits_event ON task_instances (waits_event) WHERE look_at IS NOT NULL;

CREATE TRIGGER task_instances_own_look AFTER UPDATE OF state ON task_instances
    WHEN NEW.state <> OLD.state AND NEW.look_at IS NOT NULL
BEGIN
    UPDATE task_instances SET look_at = NULL WHERE id = NEW.id;
END;
CREATE TRIGGER task_instances_ended_waits AFTER UPDATE OF state ON task_instances
    WHEN NEW.state <> OLD.state
        AND NEW.state IN ('success', 'failed', 'cancelled', 'skipped', 'upstream_failed')
BEGIN
    UPDATE task_instances SET look_at = NULL
    WHERE look_at IS NOT NULL AND waits_dag = (SELECT dag_id FROM runs WHERE id = NEW.run_id);
END;
CREATE TRIGGER runs_ended_waits AFTER UPDATE OF state ON runs
    WHEN NEW.state <> OLD.state AND NEW.state IN ('success', 'failed', 'cancelled')
BEGIN
    UPDATE task_instances SET look_at = NULL WHERE look_at IS NOT NULL AND waits_dag = NEW.dag_id;
END;
-- Stored already ended (a test's run, a task skipped as it is created): the same.
CREATE TRIGGER task_instances_inserted_ended_waits AFTER INSERT ON task_instances
    WHEN NEW.state IN ('success', 'failed', 'cancelled', 'skipped', 'upstream_failed')
BEGIN
    UPDATE task_instances SET look_at = NULL
    WHERE look_at IS NOT NULL AND waits_dag = (SELECT dag_id FROM runs WHERE id = NEW.run_id);
END;
CREATE TRIGGER runs_inserted_ended_waits AFTER INSERT ON runs
    WHEN NEW.state IN ('success', 'failed', 'cancelled')
BEGIN
    UPDATE task_instances SET look_at = NULL WHERE look_at IS NOT NULL AND waits_dag = NEW.dag_id;
END;

-- Decision 463: what each master and worker uses (memory, CPU, the container's memory and
-- limit; a worker's by part), as it last said: a master with its row, a worker's at most once
-- a minute from its heartbeats. NULL: not known (an older worker, no /proc).
ALTER TABLE masters ADD COLUMN usage TEXT;
ALTER TABLE workers ADD COLUMN usage TEXT;
-- When the master stored it: a sample's CPU counts over the time between two of them.
ALTER TABLE workers ADD COLUMN usage_at TEXT;

-- Decision 464: a sample of each schedr process a minute (step 60), written by the leading
-- master (its masters and workers, one row per process however many roles it runs) and by each
-- webserver in a process of its own; and 15-minute averages and peaks of them (step 900).
-- Minute rows are kept 24 h, rollups 30 days (hidden timings).
CREATE TABLE samples (
    process_id      TEXT NOT NULL,
    step_s          INTEGER NOT NULL,
    at              TEXT NOT NULL,
    -- What the process runs, as JSON text: each role ("master", "worker", "webserver") with its
    -- node's ID (text on both databases).
    roles           TEXT NOT NULL,
    resident_avg    BIGINT NOT NULL,
    resident_peak   BIGINT NOT NULL,
    -- Its whole memory: itself and, for a worker, its fork servers, wait step hosts and running
    -- tasks (separate processes), so a worker's line shows its tasks (the user, 2026-11-14).
    memory_avg      BIGINT NOT NULL,
    memory_peak     BIGINT NOT NULL,
    -- The minutes it covers: 1, or a rollup's minute samples; averages over several rows are
    -- weighted by it. `cpu_minutes` and `container_minutes`: of those, the ones with a CPU or a
    -- container figure, their averages' weights. `covered`: the minutes of its window in which
    -- any process was sampled (1 for a minute), so that a deployment's total in a bucket is its
    -- processes' weighted memory over the minutes sampled (Codex's reviews of M5.1j: a process
    -- replaced within a window counted twice).
    minutes           INTEGER NOT NULL,
    cpu_minutes       INTEGER NOT NULL,
    container_minutes INTEGER NOT NULL,
    covered           INTEGER NOT NULL,
    -- CPU time used since the process started, in seconds (minute rows only).
    cpu_seconds     REAL,
    -- Cores used since the sample before (none for a process's first).
    cpu_avg         REAL,
    cpu_peak        REAL,
    container_avg   BIGINT,
    container_peak  BIGINT,
    container_limit BIGINT,
    -- A worker's memory by part and its largest tasks (a rollup: the parts' averages only).
    parts           TEXT,
    -- A worker's parts on average (NULL: not running a worker), for the averages in SQL.
    forks_avg       BIGINT,
    hosts_avg       BIGINT,
    tasks_avg       BIGINT,
    PRIMARY KEY (process_id, step_s, at)
);
CREATE INDEX samples_by_time ON samples (step_s, at);

-- The Metrics page's throughput and start latency (decision 494): attempts by when they
-- ended and when they started.
CREATE INDEX attempts_ended ON attempts (ended_at) WHERE ended_at IS NOT NULL;
CREATE INDEX attempts_started ON attempts (started_at) WHERE started_at IS NOT NULL;

-- Decision 500: the runs of some DAGs, newest triggered first (the Runs page's DAG filter: one
-- DAG's ID or a part of it). Without it Postgres walks every run by `runs_by_trigger` and keeps
-- the DAG's: 54 ms over 250,000 runs for a DAG of five (Codex's review of D500).
CREATE INDEX runs_by_dag_trigger ON runs (dag_id, triggered_at, id);
