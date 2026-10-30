//! The fault-injection harness (decision 98): a master operation run once per write it makes,
//! that write failing as a database error would, each time from a fresh state, then checked
//! for anything left half-done, in the database and against the master's memory. A path that
//! can leave half a state fails its test the day it is written.

use std::collections::BTreeSet;

use schedr_db::Database;
use schedr_db::attempts::StoredAttempt;
use schedr_db::instances::RunTasks;
use schedr_model::{TaskId, TaskInstanceState};

use crate::dispatch::Dispatcher;

/// One master operation to put through [`fail_each_write`]: how to set its state up, how to
/// run it (as the master does: inside a transaction where the master opens one), and where
/// to look afterwards.
pub(crate) trait Scenario: Sized {
    /// A fresh state for the operation.
    async fn setup() -> Self;
    /// The operation; its error is ignored (a failed write is expected to make it fail).
    async fn run(&mut self);
    /// Its database.
    fn db(&self) -> &Database;
    /// The run it works on.
    fn run_id(&self) -> &str;
    /// The master's memory to compare with the database.
    fn dispatcher(&self) -> &Dispatcher;
}

/// Runs the scenario once to count its writes (it must leave nothing half-done), then once per
/// write with that write failing. Returns the writes counted and every half-done state found,
/// each with the write that failed.
///
/// # Panics
///
/// If the scenario makes no write (the test would prove nothing), or leaves a half-done state
/// without any failure.
pub(crate) async fn fail_each_write<S: Scenario>() -> (u64, Vec<String>) {
    let mut clean = S::setup().await;
    clean.db().count_writes();
    clean.run().await;
    let writes = clean.db().writes();
    assert!(writes > 0, "the scenario makes no write: it proves nothing");
    let baseline = half_done(clean.db(), clean.run_id(), clean.dispatcher()).await;
    assert!(
        baseline.is_empty(),
        "half-done without any failure: {baseline:?}"
    );
    let mut found = Vec::new();
    for nth in 1..=writes {
        let mut scenario = S::setup().await;
        scenario.db().fail_write(nth);
        scenario.run().await;
        scenario.db().count_writes();
        for problem in half_done(scenario.db(), scenario.run_id(), scenario.dispatcher()).await {
            found.push(format!("write {nth} of {writes} failing: {problem}"));
        }
    }
    (writes, found)
}

/// What is half-done in `run_id` and its workers: a task running (or cleaning up) without
/// exactly one open attempt, or not running with one; a finished run with work left; an open
/// attempt the master's memory doesn't hold a slot for, or a slot held for an ended attempt;
/// a cancel queued with no reason stored for it.
pub(crate) async fn half_done(db: &Database, run_id: &str, dispatcher: &Dispatcher) -> Vec<String> {
    use TaskInstanceState as S;
    let mut found = Vec::new();
    let Some(run) = db.run_tasks(run_id).await.expect("the run reads") else {
        return vec![format!("run `{run_id}` is gone")];
    };
    let open: Vec<_> = db
        .open_attempts(None)
        .await
        .expect("the attempts read")
        .into_iter()
        .filter(|a| a.run_id == run_id)
        .collect();
    let mapped: BTreeSet<&TaskId> = run
        .dag
        .tasks
        .iter()
        .filter(|t| t.expand.is_some())
        .map(|t| &t.id)
        .collect();
    for instance in &run.instances {
        // A mapped task's own row stands for its instances: it runs with no attempt.
        if instance.map_index == -1 && mapped.contains(&instance.task_id) {
            continue;
        }
        let attempts = open.iter().filter(|a| a.instance_id == instance.id).count();
        let active = matches!(instance.state, S::Running | S::CleaningUp);
        let name = if instance.map_index >= 0 {
            format!("`{}[{}]`", instance.task_id, instance.map_index)
        } else {
            format!("`{}`", instance.task_id)
        };
        if active && attempts != 1 {
            found.push(format!(
                "{name} is {:?} with {attempts} open attempts",
                instance.state
            ));
        }
        if !active && attempts > 0 {
            found.push(format!(
                "{name} is {:?} with an open attempt",
                instance.state
            ));
        }
    }
    if run.state.is_final() {
        let left: Vec<String> = run
            .instances
            .iter()
            .filter(|i| !i.state.is_final())
            .map(|i| format!("`{}`", i.task_id))
            .collect();
        if !left.is_empty() {
            found.push(format!(
                "the run ended {:?} with {} unfinished",
                run.state,
                left.join(", ")
            ));
        }
    }
    cancel_problems(&run, &open, dispatcher, &mut found);
    slot_problems(db, run_id, &open, dispatcher, &mut found).await;
    found
}

/// A cancel queued for the next step must have its reason stored: the task or the run being
/// cancelled, or its mapped step failed (B51: a rolled-back change queued one anyway).
fn cancel_problems(
    run: &RunTasks,
    open: &[StoredAttempt],
    dispatcher: &Dispatcher,
    found: &mut Vec<String>,
) {
    use TaskInstanceState as S;
    for (worker, attempt_id) in dispatcher.queued_cancels() {
        let Some(attempt) = open.iter().find(|a| a.id == attempt_id) else {
            continue;
        };
        let instance = run.instances.iter().find(|i| i.id == attempt.instance_id);
        let step_failed = run.instances.iter().any(|i| {
            i.task_id == attempt.task_id
                && i.map_index == -1
                && attempt.map_index >= 0
                && matches!(i.state, S::Failed | S::UpstreamFailed)
        });
        let justified =
            run.cancel_requested || instance.is_some_and(|i| i.cancel_requested) || step_failed;
        if !justified {
            found.push(format!(
                "a cancel of `{}` queued for `{worker}`, with no cancel or failure stored",
                attempt.task_id
            ));
        }
    }
}

/// The master's memory holds a slot for every open attempt of the run, and none for one that
/// ended.
async fn slot_problems(
    db: &Database,
    run_id: &str,
    open: &[StoredAttempt],
    dispatcher: &Dispatcher,
    found: &mut Vec<String>,
) {
    for attempt in open {
        let held = dispatcher
            .workers()
            .get(&attempt.worker_id)
            .is_some_and(|w| w.attempts.contains(&attempt.id));
        if !held {
            found.push(format!(
                "attempt of `{}` open in the database, no slot held on `{}` in memory",
                attempt.task_id, attempt.worker_id
            ));
        }
    }
    let open_ids: BTreeSet<&str> = open.iter().map(|a| a.id.as_str()).collect();
    for worker in db.workers().await.expect("the workers read") {
        let Some(memory) = dispatcher.workers().get(&worker.id) else {
            continue;
        };
        for attempt in &memory.attempts {
            let of_run = db
                .attempt(attempt)
                .await
                .expect("the attempt reads")
                .is_some_and(|a| a.run_id == run_id);
            if of_run && !open_ids.contains(attempt.as_str()) {
                found.push(format!(
                    "a slot held on `{}` in memory for an attempt that ended",
                    worker.id
                ));
            }
        }
    }
}
