use pretty_assertions::assert_eq;
use rstest::rstest;
use tempfile::TempDir;

use super::*;
use crate::cli::ClientArgs;
use crate::client::ClientError;

fn client_args(json: bool) -> ClientArgs {
    ClientArgs {
        server: "http://127.0.0.1:9".to_owned(),
        token: None,
        user: None,
        json,
    }
}

fn test_command(changes: impl FnOnce(&mut DagsCommand)) -> DagsCommand {
    let mut command = DagsCommand::Test {
        dag: "sync".to_owned(),
        planned_for: None,
        params: vec![],
        secrets: vec![],
        task: None,
        path: None,
    };
    changes(&mut command);
    command
}

#[test]
fn a_directory_is_a_root_of_its_python_files() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("etl");
    std::fs::create_dir_all(root.join("team")).unwrap();
    std::fs::write(root.join("team/sync.py"), "").unwrap();
    std::fs::write(root.join("notes.md"), "").unwrap();
    std::fs::write(root.join("old.py"), "").unwrap();
    std::fs::write(root.join(".schedrignore"), "old.py\n").unwrap();
    let found = LocalRoot::at(&root).unwrap();
    assert_eq!(found.name, "etl");
    assert_eq!(found.dir, root.canonicalize().unwrap());
    assert_eq!(found.files, ["team/sync.py"]);
}

#[test]
fn a_file_is_a_root_of_one_file_in_its_directory() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("sync.py"), "").unwrap();
    let found = LocalRoot::at(&dir.path().join("sync.py")).unwrap();
    assert_eq!(found.dir, dir.path().canonicalize().unwrap());
    assert_eq!(found.files, ["sync.py"]);
}

#[test]
fn a_missing_path_is_a_bad_argument() {
    let dir = TempDir::new().unwrap();
    let error = LocalRoot::at(&dir.path().join("nope")).unwrap_err();
    assert!(error.to_string().contains("nope"), "{error}");
}

#[rstest]
#[case(test_command(|c| if let DagsCommand::Test { params, .. } = c { params.push("a=1".to_owned()); }), "dags test -p", "M4")]
#[case(test_command(|c| if let DagsCommand::Test { secrets, .. } = c { secrets.push("p#k=v".to_owned()); }), "dags test --secret", "M4")]
#[case(test_command(|c| if let DagsCommand::Test { task, .. } = c { *task = Some("load".to_owned()); }), "dags test --task", "M4")]
#[tokio::test]
async fn later_features_say_when_they_come(
    #[case] command: DagsCommand,
    #[case] what: &str,
    #[case] milestone: &str,
) {
    let error = run_local(
        &command,
        &Config::default(),
        &BTreeMap::new(),
        &client_args(false),
        &mut Vec::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        format!("`schedr {what}` isn't implemented yet (milestone {milestone})")
    );
}

/// B20: `--against` used to return before doing anything, with "isn't implemented yet".
#[tokio::test]
async fn dags_check_against_no_longer_says_not_implemented() {
    let command = DagsCommand::Check {
        path: ".".into(),
        against: Some("http://127.0.0.1:1".to_owned()),
    };
    let error = run_local(
        &command,
        &Config::default(),
        &BTreeMap::new(),
        &client_args(false),
        &mut Vec::new(),
    )
    .await
    .unwrap_err();
    let message = error.to_string();
    assert!(!message.contains("isn't implemented yet"), "{message}");
    assert!(
        message.contains("the DAG interpreter `python` isn't defined"),
        "{message}"
    );
}

#[tokio::test]
async fn a_bad_for_and_an_undefined_interpreter_are_bad_arguments() {
    let bad_for = test_command(|c| {
        if let DagsCommand::Test { planned_for, .. } = c {
            *planned_for = Some("someday".to_owned());
        }
    });
    let error = run_local(
        &bad_for,
        &Config::default(),
        &BTreeMap::new(),
        &client_args(false),
        &mut Vec::new(),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("isn't a date or a date-time"),
        "{error}"
    );

    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("sync.py"), "").unwrap();
    let check = DagsCommand::Check {
        path: dir.path().to_path_buf(),
        against: None,
    };
    let error = run_local(
        &check,
        &Config::default(),
        &BTreeMap::new(),
        &client_args(false),
        &mut Vec::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "the DAG interpreter `python` isn't defined in [interpreters]"
    );
}

#[test]
fn durations_are_the_last_attempts_as_the_worker_measured_it() {
    let task = json!({"attempts": [
        {"duration_ms": 5_000},
        {"duration_ms": 1_500}
    ]});
    assert_eq!(duration(&task), "1.5s");
    // The timestamps are the master's wall clock: one that stepped back doesn't matter.
    let stepped_back = json!({"attempts": [{
        "started_at": "2026-10-21T08:00:01Z", "ended_at": "2026-10-21T08:00:00.2Z",
        "duration_ms": 300
    }]});
    assert_eq!(duration(&stepped_back), "0.3s");
    assert_eq!(duration(&json!({"attempts": []})), "-");
    assert_eq!(duration(&json!({"attempts": [{"duration_ms": null}]})), "-");
}

fn worker(id: &str, state: &str, version: &str, drained: bool) -> schedr_api::views::WorkerView {
    serde_json::from_value(json!({
        "worker_id": id, "state": state,
        "incarnation_id": "0190a0a0-0000-7000-8000-000000000001",
        "queues": ["default"], "slots": 1, "running": 0,
        "schedr_version": version, "protocol_version": "0.1", "log_endpoint": "",
        "registered_at": "2026-10-22T08:00:00Z",
        "last_heartbeat_at": "2026-10-22T08:00:00Z",
        "offline_at": null, "drained": drained
    }))
    .unwrap()
}

fn parsed(path: &str, error: bool) -> ParsedFile {
    ParsedFile {
        path: path.to_owned(),
        dags: vec![],
        error: error.then(|| schedr_model::runner::ErrorSummary {
            exception: "RuntimeError".to_owned(),
            message: "boom".to_owned(),
            traceback: "sync.py:1\n".to_owned(),
        }),
    }
}

fn local_version() -> SchedrVersion {
    SchedrVersion::new(0, 3)
}

#[test]
fn an_older_active_worker_is_named_and_a_drained_one_counts() {
    let workers = vec![
        worker("w-new", "active", "0.3.1", false),
        worker("w2", "active", "0.2.9", true),
        worker("w-lost", "lost", "0.1.0", false),
        worker("w-gone", "disconnected", "0.0.1", false),
        worker("w-leaving", "draining", "0.1.0", false),
    ];
    let check = compare_active_workers(local_version(), "http://sched.example", &workers);
    assert!(!check.ok);
    assert_eq!(
        check.message.unwrap(),
        "this schedr is 0.3; older active workers at http://sched.example: 0.2: w2"
    );
    assert_eq!(
        check.workers,
        [
            ComparedWorker {
                worker_id: "w-new".to_owned(),
                schedr_version: "0.3.1".to_owned()
            },
            ComparedWorker {
                worker_id: "w2".to_owned(),
                schedr_version: "0.2.9".to_owned()
            },
        ]
    );
}

#[test]
fn older_workers_are_grouped_by_minor_version_oldest_first() {
    let workers = vec![
        worker("w2", "active", "0.2.0", false),
        worker("w1", "active", "0.1.4", false),
        worker("w4", "active", "0.2.8", false),
        worker("w-same", "active", "0.3.9", false),
        worker("w-newer", "active", "1.0.0", false),
        worker("w9", "active", "bogus", false),
    ];
    let check = compare_active_workers(local_version(), "http://sched.example", &workers);
    assert!(!check.ok);
    assert!(check.workers.iter().any(|worker| worker.worker_id == "w9"));
    assert_eq!(
        check.message.unwrap(),
        "this schedr is 0.3; older active workers at http://sched.example: 0.1: w1; 0.2: w2, w4"
    );
}

#[test]
fn an_unreadable_version_stays_eligible_like_the_master() {
    let workers = vec![
        worker("w1", "active", "0.3.0", false),
        worker("w9", "active", "bogus", false),
    ];
    let check = compare_active_workers(local_version(), "http://sched.example", &workers);
    assert!(check.ok);
    assert_eq!(check.message, None);
    assert_eq!(
        check.line(),
        "ok: this schedr is 0.3; 2 active workers at http://sched.example run 0.3 (w1), \
         unknown version `bogus` (w9)"
    );

    let only = vec![worker("w9", "active", "bogus", false)];
    let check = compare_active_workers(local_version(), "http://sched.example", &only);
    assert!(check.ok);
    assert_eq!(
        check.line(),
        "ok: this schedr is 0.3; 1 active worker at http://sched.example runs \
         unknown version `bogus` (w9)"
    );
}

#[test]
fn the_same_or_a_newer_minor_passes_and_no_active_worker_fails() {
    let current = vec![
        worker("w1", "active", "0.3.0", false),
        worker("w2", "active", "0.4.0", false),
    ];
    let check = compare_active_workers(local_version(), "http://sched.example", &current);
    assert!(check.ok);
    assert_eq!(check.message, None);
    assert_eq!(
        check.line(),
        "ok: this schedr is 0.3; 2 active workers at http://sched.example run 0.3 (w1), 0.4 (w2)"
    );
    let mut out = Vec::new();
    finish_check(
        &[parsed("sync.py", false)],
        Some(Ok(check)),
        false,
        &mut out,
    )
    .unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "ok     sync.py: no DAGs\n\
         1 file(s), 0 DAG(s), 0 error(s)\n\
         ok: this schedr is 0.3; 2 active workers at http://sched.example run 0.3 (w1), 0.4 (w2)\n"
    );

    let gone = vec![worker("w1", "offline", "0.1.0", false)];
    let check = compare_active_workers(local_version(), "http://sched.example", &gone);
    assert_eq!(
        check.message.unwrap(),
        "no active worker at http://sched.example to compare with this schedr (0.3): \
         workers are recorded disconnected right after a master restart until they \
         register again, so retry in a minute"
    );
    assert_eq!(check.workers, []);

    let check = compare_active_workers(local_version(), "http://sched.example", &[]);
    assert!(!check.ok);
    assert_eq!(check.workers, []);
}

#[test]
fn the_report_keeps_file_errors_and_adds_the_version_check() {
    let files = vec![parsed("sync.py", false)];
    let version = compare_active_workers(
        local_version(),
        "http://sched.example",
        &[worker("w2", "active", "0.2.0", false)],
    );
    let mut out = Vec::new();
    let error = finish_check(&files, Some(Ok(version)), false, &mut out).unwrap_err();
    assert_eq!(
        error.to_string(),
        "this schedr is 0.3; older active workers at http://sched.example: 0.2: w2"
    );
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "ok     sync.py: no DAGs\n1 file(s), 0 DAG(s), 0 error(s)\n"
    );

    let mut out = Vec::new();
    let error = finish_check(
        &[parsed("broken.py", true)],
        Some(Ok(compare_active_workers(
            local_version(),
            "http://sched.example",
            &[worker("w2", "active", "0.2.0", false)],
        ))),
        true,
        &mut out,
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "1 file(s) with errors\n\
         error: this schedr is 0.3; older active workers at http://sched.example: 0.2: w2"
    );
    let report: Value = serde_json::from_str(&String::from_utf8(out).unwrap()).unwrap();
    assert_eq!(report["version_check"]["local"], "0.3");
    assert_eq!(report["version_check"]["ok"], false);
    assert_eq!(report["version_check"]["workers"][0]["worker_id"], "w2");
    assert_eq!(
        report["version_check"]["workers"][0]["schedr_version"],
        "0.2.0"
    );
    assert_eq!(
        report["version_check"]["message"],
        "error: this schedr is 0.3; older active workers at http://sched.example: 0.2: w2"
    );
    assert_eq!(report["files"][0]["error"]["exception"], "RuntimeError");
}

#[test]
fn without_against_the_json_stays_the_files_and_a_dead_server_is_its_own_error() {
    let mut out = Vec::new();
    finish_check(&[parsed("sync.py", false)], None, true, &mut out).unwrap();
    let report: Value = serde_json::from_str(&String::from_utf8(out).unwrap()).unwrap();
    assert!(report.get("version_check").is_none(), "{report}");

    let mut out = Vec::new();
    let error = finish_check(
        &[parsed("sync.py", false), parsed("broken.py", true)],
        Some(Err(ClientError::Connect {
            server: "http://127.0.0.1:1".to_owned(),
            reason: "connection refused".to_owned(),
        })),
        false,
        &mut out,
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "1 file(s) with errors\n\
         error: can't reach the schedr server at http://127.0.0.1:1: connection refused"
    );
    assert!(String::from_utf8(out).unwrap().contains("ok     sync.py"));
}

#[test]
fn a_version_that_matches_leaves_the_check_successful() {
    let version = compare_active_workers(
        local_version(),
        "http://sched.example",
        &[worker("w1", "active", "0.3.2", false)],
    );
    let mut out = Vec::new();
    finish_check(
        &[parsed("sync.py", false)],
        Some(Ok(version)),
        true,
        &mut out,
    )
    .unwrap();
    let report: Value = serde_json::from_str(&String::from_utf8(out).unwrap()).unwrap();
    assert_eq!(report["version_check"]["ok"], true);
    assert_eq!(
        report["version_check"]["workers"][0]["schedr_version"],
        "0.3.2"
    );
    assert_eq!(
        report["version_check"]["message"],
        "ok: this schedr is 0.3; 1 active worker at http://sched.example runs 0.3 (w1)"
    );
}

#[tokio::test]
async fn against_reads_the_worker_list_and_sends_the_token() {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(None));
    let local = SchedrVersion::current();
    let older = if local.minor > 0 {
        SchedrVersion::new(local.major, local.minor - 1)
    } else {
        SchedrVersion::new(local.major.saturating_sub(1), 0)
    };
    assert!(local > older, "{local} has no older minor");
    let listed = schedr_api::views::WorkerList {
        workers: vec![worker("w2", "active", &format!("{older}.1"), true)],
    };
    let app = axum::Router::new()
        .route(
            "/api/v1/workers",
            axum::routing::get(move || {
                let listed = listed.clone();
                async move { axum::Json(listed) }
            }),
        )
        .layer(axum::middleware::from_fn_with_state(
            seen.clone(),
            remember_auth,
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut args = client_args(false);
    args.token = Some("s3cret".to_owned());
    let check = worker_versions(&format!("{url}/"), &args, &BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(
        check.message.unwrap(),
        format!("this schedr is {local}; older active workers at {url}: {older}: w2")
    );
    assert_eq!(seen.lock().unwrap().as_deref(), Some("Bearer s3cret"));
}

async fn remember_auth(
    axum::extract::State(seen): axum::extract::State<
        std::sync::Arc<std::sync::Mutex<Option<String>>>,
    >,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let header = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    *seen.lock().unwrap() = header;
    next.run(request).await
}
