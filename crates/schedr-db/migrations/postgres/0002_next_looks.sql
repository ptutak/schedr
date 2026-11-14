-- Decision 485: a scheduled DAG's next fire time, stored so that the master's probe finds it:
-- set by the scheduler after each visit, cleared (NULL: not known, visit it) by whatever
-- changes the schedule (a new version, a pause or unpause). 9999-01-01T00:00:00Z
-- (`schedr_db::dags::NO_FIRE`) is "no fire time ahead".
ALTER TABLE dag_triggers ADD COLUMN next_fire_at TIMESTAMPTZ;
CREATE INDEX dag_triggers_next_fire ON dag_triggers (next_fire_at);

-- Decision 486: the name of the event an `EventNamed` trigger waits for (NULL for any other
-- kind), written with the trigger's row, so that posting an event clears the stored next look
-- (`next_fire_at`) of only the DAGs that wait for that name, and the master's probe finds them.
ALTER TABLE dag_triggers ADD COLUMN event_name TEXT;
CREATE INDEX dag_triggers_event_name ON dag_triggers (event_name);
-- Rows stored before this migration: filled from each DAG's current definition (Grok's review
-- of M5.1f2: an upgraded database's posts found no listener until the DAG was parsed again).
UPDATE dag_triggers t SET event_name = v.dag_json -> 'trigger' -> 'event' ->> 'name'
FROM dags d JOIN dag_versions v ON v.id = d.current_version_id
WHERE d.dag_id = t.dag_id AND t.kind = 'event'
    AND v.dag_json -> 'trigger' ->> 'kind' = 'event_named';

-- Decision 487: a `wait_for` step the master holds, while it waits, stores when the round looks
-- at it next (`look_at`, its `max_wait` deadline; NULL: at once) and what it waits for: another
-- DAG's run or task ending (`waits_dag`), or a named event posted (`waits_event`). The ready
-- check skips it until then; that DAG's run or task ending, or that event posted, clears
-- `look_at` (the triggers below; the post itself), and so does the step's own state changing.
ALTER TABLE task_instances ADD COLUMN look_at TIMESTAMPTZ;
ALTER TABLE task_instances ADD COLUMN waits_dag TEXT;
ALTER TABLE task_instances ADD COLUMN waits_event TEXT;
CREATE INDEX task_instances_waits_dag ON task_instances (waits_dag) WHERE look_at IS NOT NULL;
CREATE INDEX task_instances_waits_event ON task_instances (waits_event) WHERE look_at IS NOT NULL;

-- Row triggers, run only when a row's state really changes (or it is stored ended): their own
-- update sets only `look_at`, so it runs none of them again.
CREATE FUNCTION schedr_own_look() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE task_instances SET look_at = NULL WHERE id = NEW.id;
    RETURN NULL;
END $$;
CREATE TRIGGER task_instances_own_look AFTER UPDATE OF state ON task_instances
    FOR EACH ROW WHEN (OLD.state IS DISTINCT FROM NEW.state AND NEW.look_at IS NOT NULL)
    EXECUTE FUNCTION schedr_own_look();

CREATE FUNCTION schedr_task_ended_waits() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE task_instances SET look_at = NULL
    WHERE look_at IS NOT NULL AND waits_dag = (SELECT dag_id FROM runs WHERE id = NEW.run_id);
    RETURN NULL;
END $$;
CREATE TRIGGER task_instances_ended_waits AFTER UPDATE OF state ON task_instances
    FOR EACH ROW WHEN (OLD.state IS DISTINCT FROM NEW.state
        AND NEW.state IN ('success', 'failed', 'cancelled', 'skipped', 'upstream_failed'))
    EXECUTE FUNCTION schedr_task_ended_waits();
-- Stored already ended (a test's run, a task skipped as it is created): the same.
CREATE TRIGGER task_instances_inserted_ended_waits AFTER INSERT ON task_instances
    FOR EACH ROW WHEN (NEW.state IN ('success', 'failed', 'cancelled', 'skipped', 'upstream_failed'))
    EXECUTE FUNCTION schedr_task_ended_waits();

CREATE FUNCTION schedr_run_ended_waits() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE task_instances SET look_at = NULL WHERE look_at IS NOT NULL AND waits_dag = NEW.dag_id;
    RETURN NULL;
END $$;
CREATE TRIGGER runs_ended_waits AFTER UPDATE OF state ON runs
    FOR EACH ROW WHEN (OLD.state IS DISTINCT FROM NEW.state
        AND NEW.state IN ('success', 'failed', 'cancelled'))
    EXECUTE FUNCTION schedr_run_ended_waits();
CREATE TRIGGER runs_inserted_ended_waits AFTER INSERT ON runs
    FOR EACH ROW WHEN (NEW.state IN ('success', 'failed', 'cancelled'))
    EXECUTE FUNCTION schedr_run_ended_waits();

-- Decision 463: what each master and worker uses (memory, CPU, the container's memory and
-- limit; a worker's by part), as it last said: a master with its row, a worker's at most once
-- a minute from its heartbeats. NULL: not known (an older worker, no /proc).
ALTER TABLE masters ADD COLUMN usage JSONB;
ALTER TABLE workers ADD COLUMN usage JSONB;
