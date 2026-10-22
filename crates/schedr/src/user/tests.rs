use std::time::Duration;

use axum::body::Body;
use http_body_util::BodyExt;
use pretty_assertions::assert_eq;
use schedr_api::{ApiState, router};
use schedr_db::Database;
use schedr_db::dags::{DagChange, NewParse};
use schedr_master::commands::Commander;
use schedr_master::dispatch::{DEFAULT_CANCEL_GRACE, Dispatcher};
use schedr_master::scheduler::Scheduler;
use serde_json::json;
use tokio::sync::Mutex;
use tower::ServiceExt;

use super::*;
use crate::client::{ApiRequest, ApiResponse, ClientError, Method};

const NOW: &str = "2026-10-21T08:00:00Z";

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn fixed_now() -> Timestamp {
    ts(NOW)
}

/// The API in process; reading a command back first lets the master apply pending commands
/// (unless `master_down`), so actions go the whole way: CLI → API → command row → master.
struct InProcess {
    app: axum::Router,
    db: Database,
    master: Mutex<(Commander, Scheduler, Dispatcher)>,
    master_down: bool,
}

impl Transport for InProcess {
    async fn send(&self, request: ApiRequest) -> Result<ApiResponse, ClientError> {
        if request.path.starts_with("/api/v1/commands/") && !self.master_down {
            let mut master = self.master.lock().await;
            let (commander, scheduler, dispatcher) = &mut *master;
            commander
                .apply_pending(scheduler, dispatcher, ts(NOW))
                .await
                .unwrap();
        }
        let method = match request.method {
            Method::Get => "GET",
            Method::Post => "POST",
        };
        let body = request
            .body
            .map_or_else(Body::empty, |b| Body::from(b.to_string()));
        let http = axum::http::Request::builder()
            .method(method)
            .uri(&request.path)
            .header("content-type", "application/json")
            .body(body)
            .unwrap();
        let response = self.app.clone().oneshot(http).await.unwrap();
        let status = response.status().as_u16();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        Ok(ApiResponse {
            status,
            body: serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
        })
    }
}

/// DAG `sync` (paused, daily at 03:00 UTC) behind the API, and a worker for its tasks.
async fn world(master_down: bool) -> Client<InProcess> {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h1",
            schedr_version: "0.1.0",
            worker_id: "w1",
            parsed_at: ts(NOW),
            files: &[],
        })
        .await
        .unwrap();
    let dag: schedr_model::Dag = serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "team/sync.py"},
        "trigger": {"kind": "cron", "expression": "0 3 * * *", "timezone": "Europe/Warsaw", "catchup": false},
        "initial_since": null,
        "max_active_runs": 1,
        "tasks": [{
            "id": "extract", "kind": "python",
            "settings": {
                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
                "queue": "default", "max_output_size": 1_048_576
            },
            "output_type": {"type": "none"}
        }],
        "edges": []
    }))
    .unwrap();
    let change = DagChange::Define {
        dag: Box::new(dag),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts(NOW)).await.unwrap();
    let mut state = ApiState::new(db.clone(), None);
    state.clock = fixed_now;
    let mut dispatcher = Dispatcher::new(db.clone(), DEFAULT_CANCEL_GRACE);
    dispatcher
        .register("w1", "i1", &[], 2, ts(NOW))
        .await
        .unwrap();
    let transport = InProcess {
        app: router(state),
        master: Mutex::new((
            Commander::new(db.clone()),
            Scheduler::new(db.clone()),
            dispatcher,
        )),
        db,
        master_down,
    };
    Client::new(
        transport,
        Duration::from_millis(1),
        Duration::from_millis(20),
    )
}

fn text() -> Output {
    Output {
        json: false,
        local: TimeZone::get("Europe/Warsaw").unwrap(),
    }
}

fn json_output() -> Output {
    Output {
        json: true,
        ..text()
    }
}

async fn dags(client: &Client<InProcess>, command: DagsCommand, output: &Output) -> String {
    let mut out = Vec::new();
    run_dags(&command, client, output, &mut out).await.unwrap();
    String::from_utf8(out).unwrap()
}

async fn runs(
    client: &Client<InProcess>,
    command: RunsCommand,
    output: &Output,
) -> (String, String) {
    let (mut out, mut err) = (Vec::new(), Vec::new());
    run_runs(&command, client, output, &mut out, &mut err)
        .await
        .unwrap();
    (
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

fn trigger(planned_for: Option<&str>) -> RunsCommand {
    RunsCommand::Trigger {
        dag: "sync".to_owned(),
        planned_for: planned_for.map(str::to_owned),
        params: vec![],
        params_json: None,
    }
}

#[tokio::test]
async fn dags_are_listed_as_a_table() {
    let client = world(false).await;
    assert_eq!(
        dags(&client, DagsCommand::List, &text()).await,
        "DAG   PAUSED  TRIGGER                    DEFINED IN\n\
         sync  yes     0 3 * * * (Europe/Warsaw)  etl:team/sync.py\n"
    );
    let listed: serde_json::Value =
        serde_json::from_str(&dags(&client, DagsCommand::List, &json_output()).await).unwrap();
    assert_eq!(listed["dags"][0]["dag_id"], "sync");
}

#[tokio::test]
async fn pausing_and_unpausing_wait_for_the_master() {
    let client = world(false).await;
    let unpause = DagsCommand::Unpause {
        dag: "sync".to_owned(),
    };
    assert_eq!(dags(&client, unpause, &text()).await, "unpaused sync\n");
    assert!(!client.transport().db.stored_dags().await.unwrap()[0].paused);
    let pause = DagsCommand::Pause {
        dag: "sync".to_owned(),
    };
    let printed: serde_json::Value =
        serde_json::from_str(&dags(&client, pause, &json_output()).await).unwrap();
    assert_eq!(printed["status"], "applied");
    assert!(client.transport().db.stored_dags().await.unwrap()[0].paused);
}

#[tokio::test]
async fn a_rejected_command_is_an_error_with_the_masters_reason() {
    let client = world(false).await;
    let pause = DagsCommand::Pause {
        dag: "ghost".to_owned(),
    };
    let error = run_dags(&pause, &client, &text(), &mut Vec::new())
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "rejected: no DAG `ghost`");
}

#[tokio::test]
async fn without_the_master_a_command_times_out_pending() {
    let client = world(true).await;
    let pause = DagsCommand::Pause {
        dag: "sync".to_owned(),
    };
    let error = run_dags(&pause, &client, &text(), &mut Vec::new())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("the master hasn't applied the command yet"),
        "{error}"
    );
}

#[tokio::test]
async fn a_trigger_reports_the_new_run_and_warns_about_utc() {
    let client = world(false).await;
    let (out, err) = runs(&client, trigger(Some("2026-10-15T01:00")), &text()).await;
    assert_eq!(
        err,
        "warning: no offset given: interpreted as 2026-10-15T01:00Z (UTC)\n"
    );
    assert!(out.starts_with("triggered run "), "{out}");
    assert!(
        out.ends_with(" of sync for 2026-10-15 01:00 UTC · 03:00 CEST (running)\n"),
        "{out}"
    );
    let (json, err) = runs(&client, trigger(None), &json_output()).await;
    assert_eq!(err, "");
    let printed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(printed["kind"], "trigger_run");
    assert!(printed["result"]["run_id"].is_string());
}

#[tokio::test]
async fn runs_are_listed_and_cancelled() {
    let client = world(false).await;
    runs(&client, trigger(Some("2026-10-15T01:00:00Z")), &text()).await;
    let list = RunsCommand::List {
        dag: "sync".to_owned(),
        planned_for: None,
    };
    let (out, _) = runs(&client, list, &text()).await;
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].starts_with("RUN "), "{out}");
    assert!(
        lines[0].ends_with("KIND    STATE    FOR                                BY"),
        "{out}"
    );
    assert!(
        lines[1].ends_with("manual  running  2026-10-15 01:00 UTC · 03:00 CEST  anonymous"),
        "{out}"
    );
    let run_id = lines[1].split_whitespace().next().unwrap().to_owned();

    let on_day = RunsCommand::List {
        dag: "sync".to_owned(),
        planned_for: Some("2026-10-16T01:00:00Z".to_owned()),
    };
    let (json, _) = runs(&client, on_day, &json_output()).await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&json).unwrap(),
        json!({"runs": []})
    );

    let cancel = RunsCommand::Cancel {
        run_id: Some(run_id.clone()),
        backfill: None,
    };
    let (out, _) = runs(&client, cancel, &text()).await;
    assert_eq!(
        out,
        format!("cancelling run {run_id} (0 running task(s) being stopped)\n")
    );
}

#[tokio::test]
async fn later_features_and_bad_arguments_are_errors() {
    let client = world(false).await;
    let cases = [
        (
            RunsCommand::Trigger {
                dag: "sync".to_owned(),
                planned_for: None,
                params: vec!["a=1".to_owned()],
                params_json: None,
            },
            "`schedr runs trigger -p / --params` isn't implemented yet (milestone M4)",
        ),
        (
            RunsCommand::Cancel {
                run_id: None,
                backfill: Some("b1".to_owned()),
            },
            "`schedr runs cancel --backfill` isn't implemented yet (milestone M4)",
        ),
        (
            RunsCommand::Backfill {
                dag: "sync".to_owned(),
                from: "2026-10-01".to_owned(),
                to: None,
                all: false,
                params: vec![],
            },
            "`schedr runs backfill` isn't implemented yet (milestone M4)",
        ),
        (
            RunsCommand::List {
                dag: "sync".to_owned(),
                planned_for: Some("soon".to_owned()),
            },
            "`--for soon` isn't a date or a date-time \
             (e.g. 2026-10-15T03:00, 2026-10-15T03:00+02:00, 2026-10-15)",
        ),
    ];
    for (command, message) in cases {
        let error = run_runs(&command, &client, &text(), &mut Vec::new(), &mut Vec::new())
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), message);
    }
    let check = DagsCommand::Check {
        path: "dags".into(),
        against: None,
    };
    let error = run_dags(&check, &client, &text(), &mut Vec::new())
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "`dags check` and `dags test` run locally, not against a server"
    );
}

#[test]
fn triggers_are_written_as_people_write_them() {
    let utc = TimeZone::UTC;
    let cron = json!({"kind": "cron", "expression": "@daily", "timezone": "UTC", "catchup": false});
    assert_eq!(trigger_text(&cron, &utc), "@daily");
    assert_eq!(
        trigger_text(
            &json!({"kind": "every", "interval_ms": 5_400_000, "catchup": false}),
            &utc
        ),
        "every 1h 30m"
    );
    assert_eq!(
        trigger_text(
            &json!({"kind": "every", "interval_ms": 3_600_000, "catchup": true}),
            &utc
        ),
        "every 1h, catchup"
    );
    assert_eq!(
        trigger_text(&json!({"kind": "once", "at": "2026-12-24T22:00:00Z"}), &utc),
        "once at 2026-12-24 22:00Z"
    );
    assert_eq!(trigger_text(&json!({"kind": "none"}), &utc), "-");
}

#[test]
fn a_triggers_window_is_shown_in_utc_with_the_local_offset() {
    let warsaw = TimeZone::get("Europe/Warsaw").unwrap();
    let window = json!({
        "kind": "cron", "expression": "0 3 * * *", "timezone": "Europe/Warsaw",
        "catchup": true, "start": "2025-12-31T23:00:00Z", "end": "2026-06-30T22:00:00Z"
    });
    assert_eq!(
        trigger_text(&window, &warsaw),
        "0 3 * * * (Europe/Warsaw), catchup, \
         2025-12-31 23:00Z (local:+1h)..2026-06-30 22:00Z (local:+2h)"
    );
    let until = json!({"kind": "every", "interval_ms": 60_000, "catchup": false, "start": null,
                       "end": "2026-12-31T00:00:00Z"});
    assert_eq!(
        trigger_text(&until, &TimeZone::UTC),
        "every 1m, ..2026-12-31 00:00Z"
    );
    let from = json!({"kind": "cron", "expression": "@daily", "timezone": "UTC",
                      "catchup": false, "start": "2026-01-01T00:00:00Z", "end": null});
    assert_eq!(
        trigger_text(&from, &TimeZone::UTC),
        "@daily, 2026-01-01 00:00Z.."
    );
}

#[test]
fn a_dag_in_conflict_says_so() {
    let dag = DagView {
        dag_id: "dup".to_owned(),
        paused: false,
        location: None,
        conflict: Some(vec![
            schedr_api::views::LocationView {
                root: "etl".to_owned(),
                path: "a.py".to_owned(),
            },
            schedr_api::views::LocationView {
                root: "etl".to_owned(),
                path: "b.py".to_owned(),
            },
        ]),
        trigger: None,
        description: None,
        tags: vec![],
    };
    assert_eq!(
        dag_rows(&[dag], &TimeZone::UTC),
        [vec![
            "dup".to_owned(),
            "no".to_owned(),
            String::new(),
            "CONFLICT: 2 places".to_owned()
        ]]
    );
}

async fn workers(client: &Client<InProcess>, command: WorkersCommand, output: &Output) -> String {
    let mut out = Vec::new();
    run_workers(&command, client, output, &mut out)
        .await
        .unwrap();
    String::from_utf8(out).unwrap()
}

#[tokio::test]
async fn workers_are_listed_as_a_table() {
    let client = world(false).await;
    let queues = vec!["default".to_owned(), "large".to_owned()];
    client
        .transport()
        .db
        .register_worker(
            &schedr_db::workers::WorkerRegistration {
                id: "w1",
                incarnation: "i1",
                protocol_version: "1.0",
                schedr_version: "0.1.0",
                queues: &queues,
                slots: 4,
                log_endpoint: "",
            },
            ts(NOW),
        )
        .await
        .unwrap();
    assert_eq!(
        workers(&client, WorkersCommand::List, &text()).await,
        "WORKER  STATE   RUNNING  QUEUES         LAST SEEN\n\
         w1      active  0/4      default,large  2026-10-21 08:00 UTC · 10:00 CEST\n"
    );
    let listed: serde_json::Value =
        serde_json::from_str(&workers(&client, WorkersCommand::List, &json_output()).await)
            .unwrap();
    assert_eq!(listed["workers"][0]["state"], "active");
}

#[tokio::test]
async fn draining_comes_later() {
    let client = world(false).await;
    let error = run_workers(
        &WorkersCommand::Drain {
            worker: "w1".to_owned(),
        },
        &client,
        &text(),
        &mut Vec::new(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("milestone M2"), "{error}");
}
