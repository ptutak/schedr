use pretty_assertions::assert_eq;
use schedr_db::dags::CurrentVersion;
use schedr_model::commands::CommandStatus;
use schedr_model::{AttemptStatus, DagId, RunKind, RunState, TaskId, TaskInstanceState};
use serde_json::json;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

#[test]
fn a_dag_shows_its_current_version_or_its_conflict() {
    let dag: schedr_model::Dag = serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "description": "Daily sync", "tags": ["team-a"],
        "trigger": {"kind": "every", "interval_ms": 60_000, "catchup": false}, "initial_since": null, "max_active_runs": 1, "tasks": [], "edges": []
    }))
    .unwrap();
    let stored = StoredDag {
        dag_id: DagId::new("sync").unwrap(),
        paused: true,
        removed: false,
        conflict: None,
        current: Some(CurrentVersion {
            version_id: "v".to_owned(),
            parse_id: "p".to_owned(),
            dag,
        }),
    };
    assert_eq!(
        serde_json::to_value(DagView::of(&stored, &HashMap::new())).unwrap(),
        json!({
            "dag_id": "sync", "paused": true,
            "location": {"root": "etl", "path": "a.py"}, "conflict": null,
            "trigger": {
                "kind": "every", "interval_ms": 60_000, "catchup": false,
                "start": null, "end": null
            },
            "description": "Daily sync", "tags": ["team-a"], "held": null
        })
    );
    // Its file fails to import in the current code: held, with the error (decision 72).
    let held = Held {
        path: "a.py".to_owned(),
        exception: "ModuleNotFoundError".to_owned(),
        message: "No module named 'pandas'".to_owned(),
    };
    let by_file = HashMap::from([(("etl".to_owned(), "a.py".to_owned()), held)]);
    assert_eq!(
        serde_json::to_value(DagView::of(&stored, &by_file).held).unwrap(),
        json!({
            "path": "a.py", "exception": "ModuleNotFoundError",
            "message": "No module named 'pandas'",
            "reason": "DAG `sync` is held, no runs are created and it can't be triggered: its \
                file `a.py` fails to import (ModuleNotFoundError: No module named 'pandas'), so \
                its tasks couldn't run. Every DAG of the file is held until it imports"
        })
    );
    let conflict = StoredDag {
        current: None,
        conflict: Some(vec![DagLocation {
            root: "etl".to_owned(),
            path: "b.py".to_owned(),
        }]),
        ..stored
    };
    let view = DagView::of(&conflict, &by_file);
    assert_eq!(
        (view.location, view.trigger, view.tags),
        (None, None, vec![])
    );
    assert_eq!(view.conflict.unwrap()[0].path, "b.py");
}

#[test]
fn runs_tasks_attempts_and_commands_use_model_names() {
    let run = RunRecord {
        id: "r".to_owned(),
        dag_id: DagId::new("sync").unwrap(),
        kind: RunKind::Event,
        state: RunState::Queued,
        planned_for: ts("2026-10-21T08:00:00Z"),
        since: ts("2026-10-20T08:00:00Z"),
        triggered_at: ts("2026-10-21T08:00:01Z"),
        triggered_by: "trigger".to_owned(),
        started_at: None,
        ended_at: None,
    };
    let view = RunView::from(&run);
    assert_eq!(
        (view.kind.as_str(), view.state.as_str()),
        ("event", "queued")
    );
    assert_eq!(
        serde_json::to_value(&view).unwrap()["planned_for"],
        json!("2026-10-21T08:00:00Z")
    );

    let task = TaskView::from(&InstanceRecord {
        task_id: TaskId::new("extract").unwrap(),
        state: TaskInstanceState::UpstreamFailed,
        outcome: None,
        try_number: 0,
        queued_reason: Some("waiting".to_owned()),
        updated_at: ts("2026-10-21T08:00:00Z"),
    });
    assert_eq!(
        (task.state.as_str(), task.reason.as_deref()),
        ("upstream_failed", Some("waiting"))
    );

    let attempt = AttemptView::from(&AttemptRecord {
        id: "a".to_owned(),
        try_number: 2,
        worker_id: Some("w1".to_owned()),
        status: Some(AttemptStatus::TimedOut),
        exit_code: None,
        error_kind: None,
        error_summary: Some(r#"{"exception": "Timeout"}"#.to_owned()),
        assigned_at: ts("2026-10-21T08:00:00Z"),
        started_at: None,
        ended_at: None,
        duration_ms: Some(1_500),
        peak_memory_bytes: Some(64 << 20),
        start_method: Some("fork".to_owned()),
        usage: Some(schedr_model::runner::ProcessUsage {
            user_cpu_micros: 1,
            system_cpu_micros: 2,
            peak_rss_bytes: Some(3),
            major_faults: 4,
            voluntary_context_switches: Some(5),
            involuntary_context_switches: Some(6),
        }),
        core_dumped: true,
    });
    assert_eq!(
        serde_json::to_value(attempt.usage).unwrap(),
        json!({
            "user_cpu_micros": 1, "system_cpu_micros": 2, "peak_rss_bytes": 3,
            "major_faults": 4, "voluntary_context_switches": 5,
            "involuntary_context_switches": 6
        })
    );
    assert!(attempt.core_dumped);
    assert_eq!(attempt.status.as_deref(), Some("timed_out"));
    assert_eq!(attempt.duration_ms, Some(1_500));
    assert_eq!(attempt.peak_memory_bytes, Some(64 << 20));
    assert_eq!(attempt.start_method.as_deref(), Some("fork"));
    assert_eq!(attempt.error_summary, Some(json!({"exception": "Timeout"})));

    let command = CommandView::from(&CommandRecord {
        id: "c".to_owned(),
        kind: "pause_dag".to_owned(),
        status: CommandStatus::Rejected,
        reason: Some("no DAG `x`".to_owned()),
        result: None,
        requested_by: "anna".to_owned(),
        requested_at: ts("2026-10-21T08:00:00Z"),
        decided_at: None,
    });
    assert_eq!(command.status, "rejected");
}

#[test]
fn requests_default_and_refuse_unknown_fields() {
    assert_eq!(
        serde_json::from_value::<TriggerRequest>(json!({})).unwrap(),
        TriggerRequest::default()
    );
    assert!(
        !serde_json::from_value::<PauseRequest>(json!({}))
            .unwrap()
            .cancel_runs
    );
    assert!(serde_json::from_value::<PauseRequest>(json!({"cancel": true})).is_err());
}
