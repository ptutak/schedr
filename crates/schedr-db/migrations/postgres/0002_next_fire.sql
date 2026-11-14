-- Decision 485: a scheduled DAG's next fire time, stored so that the master's probe finds it:
-- set by the scheduler after each visit, cleared (NULL: not known, visit it) by whatever
-- changes the schedule (a new version, a pause or unpause). 9999-01-01T00:00:00Z
-- (`schedr_db::dags::NO_FIRE`) is "no fire time ahead".
ALTER TABLE dag_triggers ADD COLUMN next_fire_at TIMESTAMPTZ;
CREATE INDEX dag_triggers_next_fire ON dag_triggers (next_fire_at);
