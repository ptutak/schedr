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
        event_key: None,
        event_data: None,
        event_at: None,
        params_json: None,
        dag: "sync".to_owned(),
        planned_for: None,
        params: vec![],
        secrets: vec![],
        connections: vec![],
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
    std::fs::write(root.join("team/sync.py"), "import schedr\n").unwrap();
    std::fs::write(root.join("team/util.py"), "").unwrap();
    std::fs::write(root.join("notes.md"), "").unwrap();
    std::fs::write(root.join("old.py"), "").unwrap();
    std::fs::write(root.join(".schedrignore"), "old.py\n").unwrap();
    let found = LocalRoot::at(&root).unwrap();
    assert_eq!(found.name, "etl");
    assert_eq!(found.dir, root.canonicalize().unwrap());
    assert_eq!(found.files, ["team/sync.py"]);
    assert_eq!(found.skipped, ["team/util.py"]);
}

#[test]
fn a_file_is_a_root_of_one_file_in_its_directory() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("sync.py"), "import schedr\n").unwrap();
    let found = LocalRoot::at(&dir.path().join("sync.py")).unwrap();
    assert_eq!(found.dir, dir.path().canonicalize().unwrap());
    assert_eq!(found.files, ["sync.py"]);
}

#[test]
fn a_named_special_file_is_refused_rather_than_opened() {
    let dir = TempDir::new().unwrap();
    let pipe = dir.path().join("pipe.py");
    let made = std::process::Command::new("mkfifo")
        .arg(&pipe)
        .status()
        .unwrap();
    assert!(made.success());
    let error = LocalRoot::at(&pipe).unwrap_err();
    assert!(
        error.to_string().contains("not a file or a directory"),
        "{error}"
    );
}

/// A configuration with interpreter `bash`, and the script roots given.
fn scripts_config(roots: &[(&str, &Path)]) -> Config {
    let mut config = Config::default();
    config.interpreters.insert(
        "bash".to_owned(),
        schedr_config::Interpreter::path(PathBuf::from("/bin/bash")),
    );
    config.script_roots = roots
        .iter()
        .map(|(name, path)| schedr_config::roots::Root {
            name: (*name).to_owned(),
            path: path.to_path_buf(),
            only: None,
        })
        .collect();
    config
}

/// A script root `scripts` with a valid `team/schedr.toml` and an invalid top one.
fn script_root(dir: &TempDir) -> PathBuf {
    let root = dir.path().join("scripts");
    std::fs::create_dir_all(root.join("team")).unwrap();
    std::fs::write(root.join("team/run.sh"), "echo hi\n").unwrap();
    // A `.py` that a DAG check would import: never imported here.
    std::fs::write(root.join("team/boom.py"), "raise SystemExit(1)\n").unwrap();
    std::fs::write(
        root.join("team/schedr.toml"),
        "[jobs.daily]\nscript = \"run.sh\"\ninterpreter = \"bash\"\nschedule = \"@daily\"\n",
    )
    .unwrap();
    std::fs::write(
        root.join("schedr.toml"),
        "[jobs.bad]\nscript = \"nope.sh\"\n",
    )
    .unwrap();
    root
}

#[test]
fn a_directory_with_a_schedr_toml_is_checked_as_a_script_root() {
    let dir = TempDir::new().unwrap();
    let root = script_root(&dir);
    let files = check_scripts(&root, &scripts_config(&[])).unwrap().unwrap();
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, ["schedr.toml", "team/schedr.toml"]);
    let error = files[0].error.as_ref().unwrap();
    assert_eq!(error.exception, schedr_agent::jobs::INVALID_JOBS_EXCEPTION);
    assert!(
        error.message.contains("`interpreter` is required"),
        "{error:?}"
    );
    assert_eq!(files[1].error, None);
    assert_eq!(
        files[1].dags[0].id.as_str(),
        "scripts:team/schedr.toml#daily"
    );
}

#[test]
fn a_schedr_toml_inside_a_configured_script_root_gets_its_name() {
    let dir = TempDir::new().unwrap();
    let root = script_root(&dir);
    let config = scripts_config(&[("etl", root.as_path())]);
    let files = check_scripts(&root.join("team/schedr.toml"), &config)
        .unwrap()
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].dags[0].id.as_str(), "etl:team/schedr.toml#daily");
    // A subdirectory of it: only the files under it.
    let files = check_scripts(&root.join("team"), &config).unwrap().unwrap();
    assert_eq!(files.len(), 1);
}

#[test]
fn a_dag_file_or_a_directory_without_schedr_toml_isnt_a_script_check() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("sync.py"), "").unwrap();
    let config = scripts_config(&[]);
    assert_eq!(check_scripts(dir.path(), &config).unwrap(), None);
    assert_eq!(
        check_scripts(&dir.path().join("sync.py"), &config).unwrap(),
        None
    );
    assert!(check_scripts(&dir.path().join("nope"), &config).is_err());
}

#[test]
fn a_configured_script_root_is_never_imported_even_without_schedr_toml() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("scripts");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("side_effect.py"), "open('marker', 'w')\n").unwrap();
    let config = scripts_config(&[("scripts", root.as_path())]);
    assert_eq!(check_scripts(&root, &config).unwrap(), Some(Vec::new()));
    let error = check_scripts(&root.join("side_effect.py"), &config).unwrap_err();
    assert!(error.to_string().contains("aren't DAG files"), "{error}");
}

#[test]
fn a_configured_dag_root_is_a_dag_check_whatever_toml_it_holds() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("dags");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("broken.py"), "raise RuntimeError\n").unwrap();
    std::fs::write(root.join("schedr.toml"), "").unwrap();
    let mut config = scripts_config(&[]);
    config.dag_roots = vec![schedr_config::roots::Root {
        name: "dags".to_owned(),
        path: root.clone(),
        only: None,
    }];
    assert_eq!(check_scripts(&root, &config).unwrap(), None);
    let error = check_scripts(&root.join("schedr.toml"), &config).unwrap_err();
    assert!(
        error.to_string().contains("read only in script roots"),
        "{error}"
    );
}

#[test]
fn a_schedr_toml_linked_elsewhere_is_checked_by_its_own_name() {
    let dir = TempDir::new().unwrap();
    let root = script_root(&dir);
    std::fs::rename(
        root.join("team/schedr.toml"),
        root.join("team/declarations.toml"),
    )
    .unwrap();
    std::os::unix::fs::symlink("declarations.toml", root.join("team/schedr.toml")).unwrap();
    let files = check_scripts(&root.join("team/schedr.toml"), &scripts_config(&[]))
        .unwrap()
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].dags[0].id.as_str(), "team:schedr.toml#daily");
}

#[test]
fn a_schedr_toml_asked_for_by_name_is_read_even_if_ignored() {
    let dir = TempDir::new().unwrap();
    let root = script_root(&dir);
    std::fs::write(root.join(".schedrignore"), "schedr.toml\n").unwrap();
    let files = check_scripts(&root.join("schedr.toml"), &scripts_config(&[]))
        .unwrap()
        .unwrap();
    assert_eq!(files.len(), 1);
    assert!(files[0].error.is_some(), "{files:?}");
}

#[test]
fn a_local_check_warns_of_an_interpreter_not_configured_here() {
    let dir = TempDir::new().unwrap();
    let root = script_root(&dir);
    std::fs::write(
        root.join("team/schedr.toml"),
        "[jobs.daily]\nscript = \"run.sh\"\ninterpreter = \"ruby\"\nschedule = \"@daily\"\n",
    )
    .unwrap();
    let files = check_scripts(&root.join("team/schedr.toml"), &scripts_config(&[]))
        .unwrap()
        .unwrap();
    assert_eq!(files[0].error, None);
    assert_eq!(files[0].warnings.len(), 1, "{:?}", files[0].warnings);
}

#[test]
fn a_missing_path_is_a_bad_argument() {
    let dir = TempDir::new().unwrap();
    let error = LocalRoot::at(&dir.path().join("nope")).unwrap_err();
    assert!(error.to_string().contains("nope"), "{error}");
}

#[rstest]
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
        script_roots: Vec::new(),
        path: path.to_owned(),
        dags: vec![],
        connections: Vec::new(),
        error: error.then(|| schedr_model::runner::ErrorSummary {
            exception: "RuntimeError".to_owned(),
            message: "boom".to_owned(),
            traceback: "sync.py:1\n".to_owned(),
        }),
        warnings: Vec::new(),
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

#[test]
fn a_files_warnings_are_listed_after_it_and_dont_fail_the_check() {
    let mut warned = parsed("sync.py", false);
    warned.warnings = vec!["TypingHint: task `fetch`: outcome class `Rows` ...".to_owned()];
    let mut out = Vec::new();
    finish_check(
        &[warned.clone(), parsed("util.py", false)],
        None,
        false,
        &mut out,
    )
    .unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "ok     sync.py: no DAGs\n\
         warn   sync.py: TypingHint: task `fetch`: outcome class `Rows` ...\n\
         ok     util.py: no DAGs\n\
         2 file(s), 0 DAG(s), 0 error(s)\n"
    );
    let mut out = Vec::new();
    finish_check(&[warned, parsed("util.py", false)], None, true, &mut out).unwrap();
    let report: Value = serde_json::from_str(&String::from_utf8(out).unwrap()).unwrap();
    assert_eq!(
        report["files"][0]["warnings"],
        serde_json::json!(["TypingHint: task `fetch`: outcome class `Rows` ..."])
    );
    assert!(report["files"][1].get("warnings").is_none(), "{report}");
}

/// B210: a `--connection` that can't be read is never quoted back: its values may be secrets.
#[test]
fn a_bad_connection_value_is_not_quoted_back() {
    for bad in [
        "conn=\"conn-s3cret\"",
        "conn-s3cret",
        "conn=[\"conn-s3cret\"]",
    ] {
        let error = given_connection(bad).unwrap_err();
        assert!(
            !error.contains("s3cret") && !error.contains("Y29ubi1zM2NyZXQ"),
            "{error}"
        );
    }
}

/// B209's review: the check's log file is made new and its user's only; a link planted where
/// it goes is refused, its target left as it was.
#[test]
fn a_private_file_is_new_and_never_follows_a_link() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new().unwrap();
    let made = private_file(dir.path(), "a.log").unwrap();
    assert_eq!(
        std::fs::metadata(&made).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let victim = dir.path().join("victim");
    std::fs::write(&victim, "kept").unwrap();
    std::os::unix::fs::symlink(&victim, dir.path().join("planted.log")).unwrap();
    assert!(private_file(dir.path(), "planted.log").is_err());
    assert_eq!(std::fs::read_to_string(&victim).unwrap(), "kept");
}
