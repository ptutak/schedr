use std::time::Duration;

use axum::body::Body;
use http_body_util::BodyExt;
use jiff::Timestamp;
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
    world_with_logs(master_down, None).await
}

/// As [`world`], the API reading task logs from `logs`.
async fn world_with_logs(
    master_down: bool,
    logs: Option<std::sync::Arc<dyn schedr_api::logs::LogSource>>,
) -> Client<InProcess> {
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
    if let Some(logs) = logs {
        state = state.with_logs(logs);
    }
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
        "DAG   PAUSED  HELD  TRIGGER                    DEFINED IN\n\
         sync  yes     no    0 3 * * * (Europe/Warsaw)  etl:team/sync.py\n"
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
        out.ends_with(" of sync for 2026-10-15 01:00 UTC (03:00 CEST) (running)\n"),
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
        lines[1].ends_with("manual  running  2026-10-15 01:00 UTC (03:00 CEST)  anonymous"),
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
        held: None,
    };
    let held = DagView {
        dag_id: "sync".to_owned(),
        conflict: None,
        location: Some(schedr_api::views::LocationView {
            root: "etl".to_owned(),
            path: "sync.py".to_owned(),
        }),
        held: Some(schedr_api::views::HeldView {
            reason: String::new(),
            path: "sync.py".to_owned(),
            exception: "ModuleNotFoundError".to_owned(),
            message: "No module named 'pandas'".to_owned(),
        }),
        ..dag.clone()
    };
    assert_eq!(
        dag_rows(&[dag, held], &TimeZone::UTC),
        [
            vec![
                "dup".to_owned(),
                "no".to_owned(),
                "no".to_owned(),
                String::new(),
                "CONFLICT: 2 places".to_owned()
            ],
            vec![
                "sync".to_owned(),
                "no".to_owned(),
                "yes: ModuleNotFoundError".to_owned(),
                String::new(),
                "etl:sync.py".to_owned()
            ]
        ]
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
        "WORKER  STATE   RUNNING  QUEUES         LAST SEEN                          WARNINGS\n\
         w1      active  0/4      default,large  2026-10-21 08:00 UTC (10:00 CEST)  0\n"
    );
    let disabled = schedr_db::workers::WorkerWarning {
        kind: "fork_server_disabled".to_owned(),
        interpreter: "python".to_owned(),
        message: "the preloaded fork server is disabled: preloading `polars` failed".to_owned(),
    };
    client
        .transport()
        .db
        .set_worker_warnings("w1", &[disabled])
        .await
        .unwrap();
    assert_eq!(
        workers(&client, WorkersCommand::List, &text()).await,
        "WORKER  STATE   RUNNING  QUEUES         LAST SEEN                          WARNINGS\n\
         w1      active  0/4      default,large  2026-10-21 08:00 UTC (10:00 CEST)  1\n\
         w1: the preloaded fork server is disabled: preloading `polars` failed\n"
    );
    let listed: serde_json::Value =
        serde_json::from_str(&workers(&client, WorkersCommand::List, &json_output()).await)
            .unwrap();
    assert_eq!(listed["workers"][0]["state"], "active");
    assert_eq!(
        listed["workers"][0]["warnings"][0]["kind"],
        "fork_server_disabled"
    );
}

#[tokio::test]
async fn a_worker_is_drained_and_resumed() {
    let client = world(false).await;
    let drain = WorkersCommand::Drain {
        worker: "w1".to_owned(),
    };
    let error = run_workers(&drain, &client, &text(), &mut Vec::new())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no worker `w1`"), "{error}");
    client
        .transport()
        .db
        .register_worker(
            &schedr_db::workers::WorkerRegistration {
                id: "w1",
                incarnation: "i1",
                protocol_version: "1.0",
                schedr_version: "0.1.0",
                queues: &[],
                slots: 2,
                log_endpoint: "",
            },
            ts(NOW),
        )
        .await
        .unwrap();
    assert_eq!(
        workers(&client, drain, &text()).await,
        "draining w1: no new tasks; 0 running task(s) finishing \
         (`schedr workers resume w1` to undo)\n"
    );
    let listed = workers(&client, WorkersCommand::List, &text()).await;
    assert!(listed.contains("w1      active (drained)"), "{listed}");
    let resume = WorkersCommand::Resume {
        worker: "w1".to_owned(),
    };
    assert_eq!(workers(&client, resume, &text()).await, "resumed w1\n");
}

async fn tasks(client: &Client<InProcess>, command: TasksCommand) -> Result<String, AppError> {
    let mut out = Vec::new();
    run_tasks(&command, client, &text(), &mut out).await?;
    Ok(String::from_utf8(out).unwrap())
}

fn target(run_id: &str, map_index: Option<u32>) -> crate::cli::TaskTarget {
    crate::cli::TaskTarget {
        run_id: run_id.to_owned(),
        task_id: "extract".to_owned(),
        map_index,
    }
}

#[tokio::test]
async fn a_task_is_cancelled_and_only_an_unschedulable_one_retried() {
    let client = world(false).await;
    runs(&client, trigger(Some("2026-10-15T01:00:00Z")), &text()).await;
    let run_id = client
        .transport()
        .db
        .dag_runs(&schedr_model::DagId::new("sync").unwrap(), None, 1)
        .await
        .unwrap()[0]
        .id
        .clone();
    let error = tasks(&client, TasksCommand::Cancel(target(&run_id, None)))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("hasn't started its tasks yet"), "{error}");

    let mut master = client.transport().master.lock().await;
    master.2.step(ts(NOW)).await.unwrap();
    drop(master);
    let error = tasks(&client, TasksCommand::Retry(target(&run_id, None)))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("is running, not unschedulable"), "{error}");
    assert_eq!(
        tasks(&client, TasksCommand::Cancel(target(&run_id, None)))
            .await
            .unwrap(),
        format!("cancelling task extract of run {run_id} (being stopped)\n")
    );
    let error = tasks(&client, TasksCommand::Retry(target(&run_id, Some(2))))
        .await
        .unwrap_err()
        .to_string();
    // An unmapped task has no instances (decision 82): the API says so.
    assert_eq!(error, "task `extract` isn't mapped: it has no instance 2");
}

/// A worker whose log grows: each request gets the next answer.
#[derive(Debug, Default)]
struct GrowingLog {
    answers: std::sync::Mutex<std::collections::VecDeque<schedr_api::logs::WorkerLog>>,
    asked_from: std::sync::Mutex<Vec<u64>>,
}

impl schedr_api::logs::LogSource for GrowingLog {
    fn fetch<'a>(
        &'a self,
        _endpoint: &'a str,
        _attempt_id: &'a str,
        from: u64,
        _limit: u64,
    ) -> schedr_api::logs::LogFuture<'a> {
        self.asked_from.lock().unwrap().push(from);
        let answer = self.answers.lock().unwrap().pop_front().expect("an answer");
        Box::pin(async move { Ok(answer) })
    }
}

fn part(size: u64, running: bool, text: &str) -> schedr_api::logs::WorkerLog {
    schedr_api::logs::WorkerLog {
        worker_id: Some("w1".to_owned()),
        size,
        running,
        bytes: text.as_bytes().to_vec(),
    }
}

/// A run of `sync` whose `extract` started on `w1`, with the log `answers`: the client, the
/// run and the fake worker.
async fn running_task(
    answers: Vec<schedr_api::logs::WorkerLog>,
) -> (Client<InProcess>, String, std::sync::Arc<GrowingLog>) {
    let (client, run_id, logs, attempt_id) = assigned_task(answers).await;
    client
        .transport()
        .db
        .mark_started(&attempt_id, "spawn", ts(NOW))
        .await
        .unwrap();
    (client, run_id, logs)
}

/// As [`running_task`], `extract` assigned to `w1` but not started: also its attempt's ID.
async fn assigned_task(
    answers: Vec<schedr_api::logs::WorkerLog>,
) -> (
    Client<InProcess>,
    String,
    std::sync::Arc<GrowingLog>,
    String,
) {
    let logs = std::sync::Arc::new(GrowingLog {
        answers: std::sync::Mutex::new(answers.into()),
        ..GrowingLog::default()
    });
    let client = world_with_logs(false, Some(logs.clone())).await;
    runs(&client, trigger(Some("2026-10-15T01:00:00Z")), &text()).await;
    let db = &client.transport().db;
    let run_id = db
        .dag_runs(&schedr_model::DagId::new("sync").unwrap(), None, 1)
        .await
        .unwrap()[0]
        .id
        .clone();
    let error = tasks(&client, logs_of(&run_id, None, false))
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(
        error,
        format!("task `extract` of run `{run_id}` hasn't been tried yet: no log so far")
    );
    client
        .transport()
        .master
        .lock()
        .await
        .2
        .step(ts(NOW))
        .await
        .unwrap();
    let queues = vec![];
    db.register_worker(
        &schedr_db::workers::WorkerRegistration {
            id: "w1",
            incarnation: "i1",
            protocol_version: "0.1",
            schedr_version: "0.1.0",
            queues: &queues,
            slots: 2,
            log_endpoint: "http://w1:8081",
        },
        ts(NOW),
    )
    .await
    .unwrap();
    let attempt = db
        .task_attempts(&run_id, &schedr_model::TaskId::new("extract").unwrap())
        .await
        .unwrap()
        .remove(0);
    (client, run_id, logs, attempt.id)
}

#[tokio::test]
async fn following_waits_for_the_attempt_to_start() {
    let (client, run_id, logs, attempt_id) = assigned_task(vec![part(3, false, "ok\n")]).await;
    let db = client.transport().db.clone();
    let start = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        db.mark_started(&attempt_id, "spawn", ts(NOW))
            .await
            .unwrap();
    };
    let (out, ()) = tokio::join!(tasks(&client, logs_of(&run_id, None, true)), start);
    assert_eq!(out.unwrap(), "ok\n");
    assert_eq!(logs.asked_from.lock().unwrap().clone(), [0]);
}

fn logs_of(run_id: &str, try_number: Option<u32>, follow: bool) -> TasksCommand {
    TasksCommand::Logs(crate::cli::LogsArgs {
        target: target(run_id, None),
        try_number,
        follow,
    })
}

#[tokio::test]
async fn a_log_is_printed_as_it_is_and_followed_until_the_task_ends() {
    let (client, run_id, logs) = running_task(vec![
        part(6, true, "part1\n"),
        part(12, true, "part2\n"),
        part(12, true, ""),
        part(18, false, "part3\n"),
    ])
    .await;
    assert_eq!(
        tasks(&client, logs_of(&run_id, None, true)).await.unwrap(),
        "part1\npart2\npart3\n"
    );
    assert_eq!(logs.asked_from.lock().unwrap().clone(), [0, 6, 12, 12]);
}

#[tokio::test]
async fn without_follow_a_running_tasks_log_is_printed_so_far() {
    let (client, run_id, logs) =
        running_task(vec![part(12, true, "part1\n"), part(12, true, "part2\n")]).await;
    // The first part is all there is in one request: it stops there.
    logs.answers.lock().unwrap()[0] = part(6, true, "part1\n");
    assert_eq!(
        tasks(&client, logs_of(&run_id, Some(1), false))
            .await
            .unwrap(),
        "part1\n"
    );
}

#[tokio::test]
async fn a_log_in_json_is_one_part_per_line() {
    let (client, run_id, _logs) = running_task(vec![part(3, false, "ok\n")]).await;
    let mut out = Vec::new();
    run_tasks(
        &logs_of(&run_id, None, false),
        &client,
        &json_output(),
        &mut out,
    )
    .await
    .unwrap();
    let line: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(
        (&line["content"], &line["next"], &line["worker_id"]),
        (&json!("ok\n"), &json!(3), &json!("w1"))
    );
    let error = tasks(
        &client,
        TasksCommand::Logs(crate::cli::LogsArgs {
            target: target(&run_id, Some(1)),
            try_number: None,
            follow: false,
        }),
    )
    .await
    .unwrap_err()
    .to_string();
    assert_eq!(error, "task `extract` isn't mapped: it has no instance 1");
    let error = tasks(&client, logs_of(&run_id, Some(7), false))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("has no try 7"), "{error}");
}

#[tokio::test]
async fn a_run_its_attempts_and_a_missing_output_are_shown() {
    let (client, run_id, _) = running_task(vec![]).await;
    let show = RunsCommand::Show {
        run_id: run_id.clone(),
    };
    let (out, _) = runs(&client, show.clone(), &text()).await;
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(
        lines[0],
        format!("run {run_id} of sync (manual) for 2026-10-15 01:00 UTC (03:00 CEST): running"),
        "{out}"
    );
    assert_eq!(
        lines[1..],
        [
            "TASK     STATE    OUTCOME  TRIES  INSTANCES  REASON",
            "extract  running           1",
        ],
        "{out}"
    );
    let (json, _) = runs(&client, show, &json_output()).await;
    let detail: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(detail["run"]["run_id"], run_id.as_str());
    assert_eq!(detail["tasks"][0]["state"], "running");

    let attempts = tasks(&client, TasksCommand::Attempts(target(&run_id, None)))
        .await
        .unwrap();
    let lines: Vec<&str> = attempts.lines().collect();
    assert_eq!(
        lines[0],
        "TRY  STATUS   WORKER  STARTED                        ENDED  DURATION  EXIT  ERROR",
        "{attempts}"
    );
    assert_eq!(
        lines[1], "1    running  w1      2026-10-21 08:00Z (local:+2h)",
        "{attempts}"
    );

    // No output before the task succeeds: the API's 404 is the error.
    let error = tasks(&client, TasksCommand::Output(target(&run_id, None)))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("no output"), "{error}");
}

#[tokio::test]
async fn a_failed_attempt_shows_its_exception_and_a_rounded_duration() {
    let (client, run_id, _, attempt_id) = assigned_task(vec![]).await;
    let db = &client.transport().db;
    db.mark_started(&attempt_id, "spawn", ts(NOW))
        .await
        .unwrap();
    // The task's own exception: no error kind, only the summary.
    let summary =
        json!({"exception": "ValueError", "message": "no rows\nin table x", "traceback": ""});
    let end = schedr_db::attempts::AttemptEnd {
        exit_code: Some(1),
        error_summary: Some(summary.to_string()),
        duration_ms: Some(99),
        ..schedr_db::attempts::AttemptEnd::by_master(schedr_model::AttemptStatus::Failed)
    };
    db.end_attempt(&attempt_id, &end, ts(NOW)).await.unwrap();
    let out = tasks(&client, TasksCommand::Attempts(target(&run_id, None)))
        .await
        .unwrap();
    let row: Vec<&str> = out
        .lines()
        .nth(1)
        .unwrap()
        .split("  ")
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .collect();
    assert_eq!(
        row[row.len() - 3..],
        ["0.1s", "1", "ValueError: no rows"],
        "{out}"
    );
}

#[test]
fn durations_are_seconds_rounded_half_up_to_a_tenth() {
    let shown: Vec<String> = [0, 49, 50, 99, 150, 250, 950, 1_499, 61_234]
        .into_iter()
        .map(super::seconds)
        .collect();
    assert_eq!(
        shown,
        [
            "0.0s", "0.0s", "0.1s", "0.1s", "0.2s", "0.3s", "1.0s", "1.5s", "61.2s"
        ]
    );
}
