//! The `TriggerDagOperator` node (decisions 357-362, 378-381): the master starts its attempt,
//! creates the run, and ends the attempt with it.

use pretty_assertions::assert_eq;

use super::*;
use crate::dispatch::chained as kinds;
use crate::faults::{Scenario, fail_each_write};

/// DAG `sync`: `start`, a `TriggerDagOperator` of `orders` with `extra` over its fields
/// (`wait`, a minute's `max_wait`), then `after` on its value and, if `routed`, `notify` on
/// `Failed`.
fn chaining_dag(extra: &Json, routed: bool, retries: u32) -> Dag {
    let mut start = task("start", &json!({}), retries);
    let fields = start.as_object_mut().unwrap();
    fields.remove("arguments");
    fields.insert("kind".to_owned(), json!("trigger_dag"));
    fields.insert("dag".to_owned(), json!("orders"));
    fields.insert("max_wait_ms".to_owned(), json!(60_000));
    for (key, value) in extra.as_object().unwrap() {
        if value.is_null() {
            fields.remove(key);
        } else {
            fields.insert(key.clone(), value.clone());
        }
    }
    let mut tasks = vec![start, task("after", &json!({}), 0)];
    let mut edges =
        vec![json!({"from": "start", "to": "after", "kind": "order", "on": ["SUCCESS"]})];
    if routed {
        tasks.push(task("notify", &json!({}), 0));
        edges.push(json!({"from": "start", "to": "notify", "kind": "order", "on": ["Failed"]}));
    }
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "team/sync.py"},
        "trigger": {"kind": "none"}, "initial_since": null,
        "max_active_runs": 1, "tasks": tasks, "edges": edges
    }))
    .unwrap()
}

/// Loads DAG `orders` (root `shop`) with `trigger`, with no run.
async fn define_orders(db: &Database, trigger: &Json) {
    let parse_id = db
        .insert_parse(&NewParse {
            root: "shop",
            content_hash: "h2",
            schedr_version: "0.1.0",
            worker_id: "w1",
            parsed_at: ts(NOW),
            files: &[],
        })
        .await
        .unwrap();
    let orders: Dag = serde_json::from_value(json!({
        "produced_by": "0.1", "id": "orders", "location": {"root": "shop", "path": "orders.py"},
        "trigger": trigger, "initial_since": null, "max_active_runs": 5,
        "tasks": [task("load", &json!({}), 0)], "edges": []
    }))
    .unwrap();
    let change = DagChange::Define {
        dag: Box::new(orders),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts(NOW)).await.unwrap();
    // A new DAG is stored paused; a paused target refuses operators (decision 398).
    let orders = DagId::new("orders").unwrap();
    db.set_paused(&orders, false).await.unwrap();
}

/// A dispatcher with a run of `dag` and DAG `orders` loaded, `trigger=Chained()`.
async fn chaining(dag: Dag) -> (Dispatcher, Database, String) {
    let (dispatcher, db, run_id) = with_run(dag).await;
    define_orders(&db, &json!({"kind": "chained"})).await;
    (dispatcher, db, run_id)
}

async fn step(dispatcher: &mut Dispatcher, at: Timestamp) -> Vec<Outgoing> {
    let mut out = Vec::new();
    dispatcher.step_into(at, &mut out).await.unwrap();
    out
}

/// The runs `start` started, oldest first.
async fn children(db: &Database, run_id: &str) -> Vec<schedr_db::reads::RunRecord> {
    let orders = DagId::new("orders").unwrap();
    let mut runs: Vec<_> = db
        .dag_runs(&orders, None, 100)
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.parent_run_id.as_deref() == Some(run_id))
        .collect();
    runs.sort_by(|a, b| a.id.cmp(&b.id));
    runs
}

async fn start_attempts(db: &Database, run_id: &str) -> Vec<schedr_db::reads::AttemptRecord> {
    db.task_attempts(run_id, &id("start")).await.unwrap()
}

async fn outcome_of(db: &Database, run_id: &str, task: &str) -> Option<String> {
    let run = db.run_tasks(run_id).await.unwrap().unwrap();
    run.instances
        .iter()
        .find(|i| i.task_id == id(task))
        .and_then(|i| i.outcome.clone())
}

#[tokio::test]
async fn a_trigger_dag_starts_a_chained_run_and_ends_with_its_id() {
    let (mut dispatcher, db, run_id) = chaining(chaining_dag(&json!({}), false, 0)).await;
    assert_eq!(
        step(&mut dispatcher, ts(NOW)).await,
        [],
        "no worker is told"
    );
    assert_eq!(
        state_of(&db, &run_id, "start").await.0,
        TaskInstanceState::Running
    );
    let child = &children(&db, &run_id).await[0];
    assert_eq!(
        (
            child.kind,
            child.state,
            child.planned_for,
            child.triggered_by.as_str()
        ),
        (
            RunKind::Chained,
            RunState::Running,
            ts(NOW),
            run_id.as_str()
        )
    );
    let attempts = start_attempts(&db, &run_id).await;
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].chained_run_id.as_deref(),
        Some(child.id.as_str())
    );
    assert_eq!(attempts[0].worker_id, None);
    // Not counted towards the limits (decision 380), nor taken back as unstarted.
    assert_eq!(db.active_attempts().await.unwrap(), []);
    step(&mut dispatcher, later(START_TIMEOUT.as_secs() + 1)).await;
    assert_eq!(
        children(&db, &run_id).await.len(),
        1,
        "one run, whatever the rounds"
    );
    assert_eq!(
        state_of(&db, &run_id, "start").await.0,
        TaskInstanceState::Running
    );
    db.finish_run(&child.id, RunState::Success, later(40))
        .await
        .unwrap();
    step(&mut dispatcher, later(41)).await;
    assert_eq!(
        state_of(&db, &run_id, "start").await.0,
        TaskInstanceState::Success
    );
    assert_eq!(output_of(&db, &run_id, "start").await, json!(child.id));
    assert_eq!(
        start_attempts(&db, &run_id).await[0].status,
        Some(AttemptStatus::Success)
    );
}

#[tokio::test]
async fn a_failed_chained_run_fails_the_task_unless_the_dag_takes_it() {
    for routed in [false, true] {
        let (mut dispatcher, db, run_id) = chaining(chaining_dag(&json!({}), routed, 0)).await;
        step(&mut dispatcher, ts(NOW)).await;
        let child = children(&db, &run_id).await.remove(0);
        db.finish_run(&child.id, RunState::Cancelled, later(1))
            .await
            .unwrap();
        step(&mut dispatcher, later(2)).await;
        let (state, _) = state_of(&db, &run_id, "start").await;
        if routed {
            assert_eq!(state, TaskInstanceState::Success);
            assert_eq!(
                outcome_of(&db, &run_id, "start").await.as_deref(),
                Some("Failed")
            );
            assert_eq!(
                output_of(&db, &run_id, "start").await,
                json!({"state": "cancelled", "run_id": child.id})
            );
            assert_eq!(
                state_of(&db, &run_id, "notify").await.0,
                TaskInstanceState::Queued
            );
        } else {
            assert_eq!(state, TaskInstanceState::Failed);
            let attempt = start_attempts(&db, &run_id).await.remove(0);
            assert_eq!(attempt.error_kind.as_deref(), Some(kinds::RUN_FAILED));
        }
    }
}

#[tokio::test]
async fn a_retry_starts_a_new_run_only_after_a_failure() {
    let (mut dispatcher, db, run_id) = chaining(chaining_dag(&json!({}), false, 1)).await;
    step(&mut dispatcher, ts(NOW)).await;
    let first = children(&db, &run_id).await.remove(0);
    db.finish_run(&first.id, RunState::Failed, later(1))
        .await
        .unwrap();
    step(&mut dispatcher, later(2)).await;
    // `retry_delay` 0: queued again, and started again by the next round.
    step(&mut dispatcher, later(3)).await;
    let runs = children(&db, &run_id).await;
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].id, first.id);
    assert_eq!(runs[1].state, RunState::Running);
    assert_eq!(
        state_of(&db, &run_id, "start").await,
        (TaskInstanceState::Running, 2)
    );
}

#[tokio::test]
async fn without_waiting_it_ends_once_the_run_is_created() {
    let extra = json!({"wait": false, "max_wait_ms": null});
    let (mut dispatcher, db, run_id) = chaining(chaining_dag(&extra, false, 0)).await;
    step(&mut dispatcher, ts(NOW)).await;
    let child = children(&db, &run_id).await.remove(0);
    assert_eq!(child.state, RunState::Running);
    assert_eq!(
        state_of(&db, &run_id, "start").await.0,
        TaskInstanceState::Success
    );
    assert_eq!(output_of(&db, &run_id, "start").await, json!(child.id));
}

#[tokio::test]
async fn past_its_max_wait_it_times_out_and_leaves_the_run_going() {
    let (mut dispatcher, db, run_id) = chaining(chaining_dag(&json!({}), false, 0)).await;
    step(&mut dispatcher, ts(NOW)).await;
    step(&mut dispatcher, later(59)).await;
    assert_eq!(
        state_of(&db, &run_id, "start").await.0,
        TaskInstanceState::Running
    );
    step(&mut dispatcher, later(60)).await;
    assert_eq!(
        state_of(&db, &run_id, "start").await.0,
        TaskInstanceState::Success
    );
    assert_eq!(
        outcome_of(&db, &run_id, "start").await.as_deref(),
        Some("TimedOut")
    );
    assert_eq!(children(&db, &run_id).await[0].state, RunState::Running);
}

#[tokio::test]
async fn a_run_that_ended_past_max_wait_seen_a_round_late_is_timed_out() {
    // Codex's review of M4.12a: as a `RunOf` wait, its end time decides, not when it is seen.
    let (mut dispatcher, db, run_id) = chaining(chaining_dag(&json!({}), false, 0)).await;
    step(&mut dispatcher, ts(NOW)).await;
    let child = children(&db, &run_id).await.remove(0);
    db.finish_run(&child.id, RunState::Success, later(61))
        .await
        .unwrap();
    step(&mut dispatcher, later(62)).await;
    assert_eq!(
        outcome_of(&db, &run_id, "start").await.as_deref(),
        Some("TimedOut")
    );
}

#[tokio::test]
async fn cancelling_its_run_cancels_the_run_it_waits_for() {
    for wait in [true, false] {
        let extra = if wait {
            json!({})
        } else {
            json!({"wait": false, "max_wait_ms": null})
        };
        let (mut dispatcher, db, run_id) = chaining(chaining_dag(&extra, false, 0)).await;
        step(&mut dispatcher, ts(NOW)).await;
        dispatcher.cancel_run(&run_id, later(1)).await.unwrap();
        step(&mut dispatcher, later(2)).await;
        let run = db.run_record(&run_id).await.unwrap().unwrap();
        assert_eq!(run.state, RunState::Cancelled, "wait={wait}");
        let child = children(&db, &run_id).await.remove(0);
        let want = if wait {
            RunState::Cancelled
        } else {
            RunState::Running
        };
        assert_eq!(child.state, want, "wait={wait}");
        if wait {
            assert_eq!(
                start_attempts(&db, &run_id).await[0].status,
                Some(AttemptStatus::Cancelled)
            );
        }
    }
}

#[tokio::test]
async fn a_master_stopped_before_creating_the_run_creates_it_by_its_recorded_id() {
    let (mut dispatcher, db, run_id) = chaining(chaining_dag(&json!({}), false, 0)).await;
    // The round that started the attempt ended there.
    dispatcher.execution.prepare_runs(ts(NOW)).await.unwrap();
    let run = db.run_tasks(&run_id).await.unwrap().unwrap();
    let instance = run
        .instances
        .iter()
        .find(|i| i.task_id == id("start"))
        .unwrap();
    assert_eq!(instance.state, TaskInstanceState::Queued);
    let chosen = uuid::Uuid::now_v7().to_string();
    db.start_chained_attempt(&instance.id, &chosen, ts(NOW))
        .await
        .unwrap()
        .unwrap();
    step(&mut dispatcher, later(1)).await;
    let runs = children(&db, &run_id).await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].id, chosen);
}

#[rstest::rstest]
#[case::the_target_missing(None, 1, TaskInstanceState::UpForRetry, kinds::TARGET_MISSING)]
#[case::params_it_doesnt_take(
    Some(json!({"params": {"day": {"literal": 1}}})),
    2,
    TaskInstanceState::Failed,
    kinds::PARAMS_REFUSED
)]
#[case::planned_for_later(
    Some(json!({"planned_for": {"literal": {"$datetime": "2027-01-01T00:00:00Z"}}})),
    2,
    TaskInstanceState::Failed,
    kinds::PARAMS_REFUSED
)]
#[case::an_upstream_value_not_a_time(
    Some(json!({"planned_for": {"ref": {"kind": "dag_ctx", "field": "dag_id"}}})),
    2,
    TaskInstanceState::Failed,
    kinds::PARAMS_REFUSED
)]
#[tokio::test]
async fn a_run_that_cant_be_created_fails_the_task_retried_only_when_it_may_work_later(
    #[case] extra: Option<Json>,
    #[case] retries: u32,
    #[case] state: TaskInstanceState,
    #[case] kind: &str,
) {
    let dag = chaining_dag(extra.as_ref().unwrap_or(&json!({})), false, retries);
    let (mut dispatcher, db, run_id) = if extra.is_some() {
        chaining(dag).await
    } else {
        with_run(dag).await
    };
    step(&mut dispatcher, ts(NOW)).await;
    assert_eq!(state_of(&db, &run_id, "start").await.0, state);
    let attempt = start_attempts(&db, &run_id).await.remove(0);
    assert_eq!(attempt.error_kind.as_deref(), Some(kind));
    assert_eq!(children(&db, &run_id).await, Vec::new());
}

#[rstest::rstest]
#[case::no_trigger(json!({"kind": "none"}), false, TaskInstanceState::Failed, kinds::TARGET_REFUSED)]
#[case::a_schedule(
    json!({"kind": "once", "at": "2026-11-01T00:00:00Z"}),
    false,
    TaskInstanceState::Failed,
    kinds::TARGET_REFUSED
)]
#[case::not_named(
    json!({"kind": "chained", "by": ["other"]}),
    false,
    TaskInstanceState::Failed,
    kinds::TARGET_REFUSED
)]
#[case::paused(json!({"kind": "chained"}), true, TaskInstanceState::UpForRetry, kinds::TARGET_HELD)]
#[tokio::test]
async fn only_a_chained_target_letting_the_dag_in_and_not_paused_is_started(
    #[case] trigger: Json,
    #[case] paused: bool,
    #[case] state: TaskInstanceState,
    #[case] kind: &str,
) {
    let (mut dispatcher, db, run_id) = with_run(chaining_dag(&json!({}), false, 1)).await;
    define_orders(&db, &trigger).await;
    if paused {
        let orders = DagId::new("orders").unwrap();
        db.set_paused(&orders, true).await.unwrap();
    }
    step(&mut dispatcher, ts(NOW)).await;
    assert_eq!(state_of(&db, &run_id, "start").await.0, state);
    let attempt = start_attempts(&db, &run_id).await.remove(0);
    assert_eq!(attempt.error_kind.as_deref(), Some(kind));
    assert_eq!(children(&db, &run_id).await, Vec::new());
}

#[tokio::test]
async fn a_chained_target_naming_the_dag_is_started() {
    let (mut dispatcher, db, run_id) = with_run(chaining_dag(&json!({}), false, 0)).await;
    define_orders(&db, &json!({"kind": "chained", "by": ["other", "sync"]})).await;
    step(&mut dispatcher, ts(NOW)).await;
    assert_eq!(children(&db, &run_id).await.len(), 1);
}

#[tokio::test]
async fn a_run_planned_for_a_reference_is_planned_for_its_value() {
    let extra = json!({"planned_for": {"ref": {"kind": "dag_ctx", "field": "since"}}});
    let (mut dispatcher, db, run_id) = chaining(chaining_dag(&extra, false, 0)).await;
    step(&mut dispatcher, ts(NOW)).await;
    let since = db.run_record(&run_id).await.unwrap().unwrap().since;
    assert_eq!(children(&db, &run_id).await[0].planned_for, since);
}

#[tokio::test]
async fn dags_may_start_each_other_in_a_cycle() {
    let (mut dispatcher, db, first) = chaining(chaining_dag(&json!({}), false, 2)).await;
    // A run of `sync` started by a run of `orders` starts `orders` again (decision 399).
    let orders = DagId::new("orders").unwrap();
    let orders_version = db
        .stored_dag(&orders)
        .await
        .unwrap()
        .unwrap()
        .current
        .unwrap()
        .version_id;
    let parent = db
        .insert_run(&NewRun {
            params: &schedr_db::runs::NO_PARAMS,
            event: None,
            dag_id: &orders,
            dag_version_id: &orders_version,
            kind: RunKind::Manual,
            state: RunState::Running,
            planned_for: ts(NOW),
            since: ts(NOW),
            triggered_at: ts(NOW),
            triggered_by: "anna",
            parent_run_id: None,
        })
        .await
        .unwrap()
        .unwrap();
    let version = db.run_record(&first).await.unwrap().unwrap();
    let sync = DagId::new("sync").unwrap();
    let version_id = db
        .stored_dag(&sync)
        .await
        .unwrap()
        .unwrap()
        .current
        .unwrap()
        .version_id;
    assert_eq!(version.dag_id, sync);
    let run_id = db
        .insert_run(&NewRun {
            params: &schedr_db::runs::NO_PARAMS,
            event: None,
            dag_id: &sync,
            dag_version_id: &version_id,
            kind: RunKind::Chained,
            state: RunState::Running,
            planned_for: later(5),
            since: ts(NOW),
            triggered_at: ts(NOW),
            triggered_by: &parent,
            parent_run_id: Some(&parent),
        })
        .await
        .unwrap()
        .unwrap();
    step(&mut dispatcher, later(6)).await;
    assert_eq!(children(&db, &run_id).await.len(), 1);
    assert_eq!(
        start_attempts(&db, &run_id).await[0].error_kind,
        None,
        "no error"
    );
}

/// The round starting `start` and creating its run (decision 98).
struct Chaining {
    dispatcher: Dispatcher,
    db: Database,
    run_id: String,
}

impl Scenario for Chaining {
    async fn setup() -> Self {
        let (dispatcher, db, run_id) = chaining(chaining_dag(&json!({}), false, 0)).await;
        Self {
            dispatcher,
            db,
            run_id,
        }
    }
    async fn run(&mut self) {
        let _ = self.dispatcher.step_into(ts(NOW), &mut Vec::new()).await;
    }
    fn db(&self) -> &Database {
        &self.db
    }
    fn run_id(&self) -> &str {
        &self.run_id
    }
    fn dispatcher(&self) -> &Dispatcher {
        &self.dispatcher
    }
    async fn recover(&mut self) {
        let _ = self.dispatcher.step_into(later(1), &mut Vec::new()).await;
    }
    async fn extra(&self) -> Vec<String> {
        let mut found = Vec::new();
        let attempts = start_attempts(&self.db, &self.run_id).await;
        let runs = children(&self.db, &self.run_id).await;
        if runs.len() > 1 {
            found.push(format!("{} runs started", runs.len()));
        }
        for attempt in attempts {
            let Some(child) = attempt.chained_run_id else {
                found.push("an attempt of `start` with no run recorded".to_owned());
                continue;
            };
            if !runs.iter().any(|r| r.id == child) {
                found.push(format!("run `{child}` recorded, not created a round later"));
            }
        }
        found
    }
}

/// The round ending `start` with its run ended, a value stored and `after` moved on.
struct ChainEnding(Chaining);

impl Scenario for ChainEnding {
    async fn setup() -> Self {
        let mut chaining = Chaining::setup().await;
        step(&mut chaining.dispatcher, ts(NOW)).await;
        let child = children(&chaining.db, &chaining.run_id).await.remove(0);
        chaining
            .db
            .finish_run(&child.id, RunState::Success, later(1))
            .await
            .unwrap();
        Self(chaining)
    }
    async fn run(&mut self) {
        let _ = self.0.dispatcher.step_into(later(2), &mut Vec::new()).await;
    }
    fn db(&self) -> &Database {
        &self.0.db
    }
    fn run_id(&self) -> &str {
        &self.0.run_id
    }
    fn dispatcher(&self) -> &Dispatcher {
        &self.0.dispatcher
    }
    async fn recover(&mut self) {
        let _ = self.0.dispatcher.step_into(later(3), &mut Vec::new()).await;
    }
    async fn extra(&self) -> Vec<String> {
        let (state, _) = state_of(&self.0.db, &self.0.run_id, "start").await;
        let output = output_of(&self.0.db, &self.0.run_id, "start").await;
        if state != TaskInstanceState::Success || output.is_null() {
            return vec![format!(
                "`start` {state:?} with output {output} a round later"
            )];
        }
        Vec::new()
    }
}

#[tokio::test]
async fn starting_a_chained_run_leaves_nothing_half_done_whichever_write_fails() {
    let (writes, found) = Box::pin(fail_each_write::<Chaining>()).await;
    assert!(found.is_empty(), "{writes} writes:\n{}", found.join("\n"));
}

#[tokio::test]
async fn ending_with_a_chained_run_leaves_nothing_half_done_whichever_write_fails() {
    let (writes, found) = Box::pin(fail_each_write::<ChainEnding>()).await;
    assert!(found.is_empty(), "{writes} writes:\n{}", found.join("\n"));
}
