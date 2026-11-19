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
