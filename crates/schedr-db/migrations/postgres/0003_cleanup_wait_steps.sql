-- Whether a task instance is a resource's cleanup (decision 225) or a wait step (a sensor, which
-- takes a watcher slot) (the user, 2026-11-19): kept on the row, each with an index of the queued
-- ones, for reads that want them apart from the other tasks. Nothing reads them yet.
ALTER TABLE task_instances ADD COLUMN cleanup BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE task_instances ADD COLUMN wait_step BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX task_instances_queued_cleanups ON task_instances (ready_at) WHERE cleanup AND state = 'queued';
CREATE INDEX task_instances_queued_wait_steps ON task_instances (ready_at) WHERE wait_step AND state = 'queued';
-- Instances stored before this migration, of runs not finished only (a history-wide statement
-- would hold the database long on a large one): marked from their run's DAG version.
UPDATE task_instances t SET cleanup = true
FROM runs r JOIN dag_versions v ON v.id = r.dag_version_id
WHERE r.id = t.run_id AND r.state IN ('queued', 'running')
    AND EXISTS (
        SELECT 1 FROM jsonb_array_elements(COALESCE(v.dag_json -> 'resources', '[]'::jsonb)) res
        WHERE res ->> 'cleanup' = t.task_id
    );
UPDATE task_instances t SET wait_step = true
FROM runs r JOIN dag_versions v ON v.id = r.dag_version_id
WHERE r.id = t.run_id AND r.state IN ('queued', 'running')
    AND EXISTS (
        SELECT 1 FROM jsonb_array_elements(COALESCE(v.dag_json -> 'tasks', '[]'::jsonb)) task
        WHERE task ->> 'id' = t.task_id AND task ->> 'kind' = 'sensor'
    );
