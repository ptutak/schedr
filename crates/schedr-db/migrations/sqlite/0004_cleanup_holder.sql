-- The current hold of an attempt's cleanup (decision 535, B304): 0 for the task's own run, one
-- more for each worker a moved cleanup is given to, never lowered. That worker holds it under its
-- own ID, `<attempt>_<n>`; only the current hold's reports count.
ALTER TABLE attempts ADD COLUMN cleanup_holder INTEGER NOT NULL DEFAULT 0;
