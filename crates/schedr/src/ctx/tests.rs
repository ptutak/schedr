use std::os::fd::AsRawFd;
use std::path::Path;

use pretty_assertions::assert_eq;
use std::sync::Arc;

use schedr_agent::ctx::{CtxInfo, channel, serve};
use schedr_agent::handles::AttemptHandles;
use schedr_model::ctx::CtxRun;
use schedr_model::states::RunKind;
use schedr_model::task::{CleanupRun, TaskCleanup};
use serde_json::json;

use super::*;
use crate::cli::FieldArg;

fn info(version: &str) -> CtxInfo {
    CtxInfo {
        run: CtxRun {
            dag_id: "report".into(),
            id: "r1".into(),
            kind: RunKind::Scheduled,
            planned_for: "2026-11-01T08:00:00Z".parse().unwrap(),
            since: "2026-10-31T08:00:00Z".parse().unwrap(),
        },
        params: json!({
            "region": "eu",
            "day": {"$date": "2026-10-01"},
            "window": {"$timedelta": 5_400_000_000_i64},
            "pair": {"$tuple": [1, "a"]},
            "note": "two\nlines",
        }),
        version: version.into(),
        secrets: None,
        handles: None,
    }
}

fn params(field: Option<&str>) -> DagCtxCommand {
    DagCtxCommand::Params {
        command: GetCommand::Get {
            field: FieldArg {
                field: field.map(str::to_owned),
            },
        },
    }
}

fn run(field: Option<&str>) -> DagCtxCommand {
    DagCtxCommand::Run {
        command: GetCommand::Get {
            field: FieldArg {
                field: field.map(str::to_owned),
            },
        },
    }
}

/// Runs `command` against a served channel, as a task's process would.
async fn against_agent(
    command: DagCtxCommand,
    agent_version: &str,
    caller_version: &'static str,
) -> Result<String, String> {
    let (ours, theirs) = channel().unwrap();
    let _serving = serve(ours, info(agent_version));
    let env = BTreeMap::from([(CTX_FD_ENV.to_owned(), theirs.as_raw_fd().to_string())]);
    tokio::task::spawn_blocking(move || {
        let mut out = Vec::new();
        let result = run_dag_ctx(&command, &env, caller_version, &mut out);
        drop(theirs);
        result.map(|()| String::from_utf8(out).unwrap())
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn params_get_prints_the_json_form_on_one_line() {
    let out = against_agent(params(None), "1.0.0", "1.0.0").await.unwrap();
    assert!(
        out.ends_with('\n') && out.trim_end().lines().count() == 1,
        "{out:?}"
    );
    let printed: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        printed,
        json!({"region": "eu", "day": "2026-10-01", "window": "P0DT5400.000000S", "pair": [1, "a"],
               "note": "two\nlines"})
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn field_prints_text_raw_and_other_values_as_json() {
    let raw = |field| async move {
        against_agent(params(Some(field)), "1.0.0", "1.0.0")
            .await
            .unwrap()
    };
    assert_eq!(raw("region").await, "eu\n");
    assert_eq!(raw("day").await, "2026-10-01\n");
    assert_eq!(raw("pair").await, "[1,\"a\"]\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_get_prints_the_run_information() {
    let out = against_agent(run(None), "1.0.0", "1.0.0").await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&out).unwrap(),
        json!({"dag_id": "report", "id": "r1", "kind": "scheduled",
               "planned_for": "2026-11-01T08:00:00Z", "since": "2026-10-31T08:00:00Z"})
    );
    let planned = against_agent(run(Some("planned_for")), "1.0.0", "1.0.0").await;
    assert_eq!(planned.unwrap(), "2026-11-01T08:00:00Z\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_field_is_an_error() {
    let error = against_agent(params(Some("nope")), "1.0.0", "1.0.0")
        .await
        .unwrap_err();
    assert_eq!(error, "there is no field `nope`");
}

#[tokio::test(flavor = "multi_thread")]
async fn another_schedr_version_speaking_the_protocol_is_served() {
    let out = against_agent(run(Some("id")), "1.0.0", "0.9.0")
        .await
        .unwrap();
    assert_eq!(out, "r1\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_task_that_ended_is_said_so() {
    let (ours, theirs) = channel().unwrap();
    drop(serve(ours, info("1.0.0")));
    let env = BTreeMap::from([(CTX_FD_ENV.to_owned(), theirs.as_raw_fd().to_string())]);
    let error = tokio::task::spawn_blocking(move || {
        // The aborted server drops the agent's end at its next poll. A call queued just
        // before that is dropped unread ("closed the connection"): the next one is refused.
        let mut last = Ok(());
        for _ in 0..100 {
            last = run_dag_ctx(&params(None), &env, "1.0.0", &mut Vec::new());
            if last
                .as_ref()
                .is_err_and(|e| !e.contains("closed the connection"))
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(theirs);
        last.unwrap_err()
    })
    .await
    .unwrap();
    assert!(error.contains("the task has ended"), "{error}");
}

#[test]
fn outside_a_task_and_a_bad_number_are_errors() {
    let error = run_dag_ctx(&params(None), &BTreeMap::new(), "1", &mut Vec::new()).unwrap_err();
    assert!(
        error.contains("must be run inside a schedr task"),
        "{error}"
    );
    let env = BTreeMap::from([(CTX_FD_ENV.to_owned(), "x".to_owned())]);
    let error = run_dag_ctx(&params(None), &env, "1", &mut Vec::new()).unwrap_err();
    assert_eq!(error, "SCHEDR_CTX_FD=`x` isn't a descriptor number");
}

#[test]
fn a_closed_or_reused_channel_names_pass_fds() {
    let file = std::fs::File::open("/dev/null").unwrap();
    let env = BTreeMap::from([(CTX_FD_ENV.to_owned(), file.as_raw_fd().to_string())]);
    let error = run_dag_ctx(&params(None), &env, "1", &mut Vec::new()).unwrap_err();
    assert!(
        error.contains("isn't the task's context channel"),
        "{error}"
    );
    assert!(error.contains("pass_fds"), "{error}");
    assert!(channel_lost(9, Inherited::Closed(9)).contains("pass_fds=[9]"));
}

#[test]
fn a_connection_over_the_limit_says_why() {
    let (agent_end, task_end) = std::os::unix::net::UnixDatagram::pair().unwrap();
    // As the agent at its limit: the reason on the fresh connection, then closed unread.
    let refusing = std::thread::spawn(move || {
        let mut data = [0_u8; 16];
        let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
        let mut control = rustix::net::RecvAncillaryBuffer::new(&mut space);
        rustix::net::recvmsg(
            &agent_end,
            &mut [io::IoSliceMut::new(&mut data)],
            &mut control,
            rustix::net::RecvFlags::empty(),
        )
        .unwrap();
        let mut ends = Vec::new();
        for message in control.drain() {
            if let rustix::net::RecvAncillaryMessage::ScmRights(fds) = message {
                ends.extend(fds);
            }
        }
        let mut end = UnixStream::from(ends.pop().unwrap());
        let body = serde_json::to_vec(
            &json!({"type": "error", "reason": "the task has 64 context connections open"}),
        )
        .unwrap();
        end.write_all(&u32::try_from(body.len()).unwrap().to_be_bytes())
            .unwrap();
        end.write_all(&body).unwrap();
    });
    let env = BTreeMap::from([(CTX_FD_ENV.to_owned(), task_end.as_raw_fd().to_string())]);
    let error = run_dag_ctx(&params(None), &env, "1", &mut Vec::new()).unwrap_err();
    refusing.join().unwrap();
    assert_eq!(error, "the task has 64 context connections open");
}

#[test]
fn a_full_channel_is_waited_for_until_the_deadline_not_forever() {
    let (agent_end, task_end) = std::os::unix::net::UnixDatagram::pair().unwrap();
    task_end.set_nonblocking(true).unwrap();
    // An agent not reading: the queue fills.
    while task_end.send(b"x").is_ok() {}
    // The inherited socket may be blocking: that must not matter.
    task_end.set_nonblocking(false).unwrap();
    let channel = OwnedFd::from(task_end);
    let (_mine, theirs) = UnixStream::pair().unwrap();
    let started = std::time::Instant::now();
    let error = send_end(&channel, &OwnedFd::from(theirs), Duration::from_millis(200)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    drop(agent_end);
}

/// Runs `schedr task-ctx connection` against a served channel whose task declares `db`.
async fn connection_against_agent(args: ConnectionArgs) -> Result<String, String> {
    let dir = tempfile::tempdir().unwrap();
    connection_in(args, dir.path()).await
}

/// As [`connection_against_agent`], the attempt's connection files in `files`.
async fn connection_in(args: ConnectionArgs, files: &Path) -> Result<String, String> {
    let registry: schedr_model::connection::Registry = serde_json::from_value(json!({
        "connections": {
            "db": {"name": "db", "params": {
                "dsn": {"literal": "postgres://it's@db/x"},
                "pool": {"literal": 3},
                "since": {"literal": {"$date": "2026-01-02"}},
            }},
            "other": {"name": "other", "params": {}},
            "clash": {"name": "clash", "params": {
                "host": {"literal": "lower"}, "HOST": {"literal": "upper"},
            }},
            "odd": {"name": "odd", "params": {"région": {"literal": "eu"}}},
        }
    }))
    .unwrap();
    let secrets = schedr_agent::secrets::AttemptSecrets::none(std::sync::Arc::new(
        schedr_agent::secrets::SecretStores::none(),
    ))
    .with_connections(
        std::sync::Arc::new(registry),
        ["db", "clash", "odd"].map(str::to_owned).to_vec(),
    )
    .with_connection_dir(files.to_path_buf());
    let (ours, theirs) = channel().unwrap();
    let mut served = info("0.1.0");
    served.secrets = Some(std::sync::Arc::new(secrets));
    let _serving = serve(ours, served);
    let env = BTreeMap::from([(CTX_FD_ENV.to_owned(), theirs.as_raw_fd().to_string())]);
    tokio::task::spawn_blocking(move || {
        let mut out = Vec::new();
        let result = run_task_ctx_connection(&args, &env, "0.1.0", &mut out);
        drop(theirs);
        result.map(|()| String::from_utf8(out).unwrap())
    })
    .await
    .unwrap()
}

fn connection(name: &str, env: Option<&str>) -> ConnectionArgs {
    ConnectionArgs {
        name: name.to_owned(),
        env: env.map(str::to_owned),
        json: false,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_prints_as_json_or_as_quoted_exports() {
    assert_eq!(
        connection_against_agent(connection("db", None))
            .await
            .unwrap(),
        "{\"dsn\":\"postgres://it's@db/x\",\"pool\":3,\"since\":\"2026-01-02\"}\n"
    );
    let dir = tempfile::tempdir().unwrap();
    let json = dir.path().join("db/PG/connection.json");
    assert_eq!(
        connection_in(connection("db", Some("PG")), dir.path())
            .await
            .unwrap(),
        format!(
            "export PG_DSN='postgres://it'\\''s@db/x'\nexport PG_JSON='{}'\nexport PG_POOL='3'\n\
             export PG_SINCE='2026-01-02'\n",
            json.display()
        )
    );
    // The agent wrote the file the variable names, every value in it.
    let written: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&json).unwrap()).unwrap();
    assert_eq!(
        written,
        json!({"dsn": "postgres://it's@db/x", "pool": 3, "since": "2026-01-02"})
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_not_declared_or_a_bad_prefix_is_an_error() {
    let denied = connection_against_agent(connection("other", None))
        .await
        .unwrap_err();
    assert_eq!(
        denied,
        "connection `other` isn't declared in the task's `connections=` (connection_denied)"
    );
    for bad in ["1X", "A-B", "", "A B"] {
        let error = connection_against_agent(connection("db", Some(bad)))
            .await
            .unwrap_err();
        assert!(error.starts_with(&format!("--env `{bad}`")), "{error}");
    }
    // A shell would keep one of two values, or refuse the name: neither is printed.
    let clash = connection_against_agent(connection("clash", Some("DB")))
        .await
        .unwrap_err();
    assert_eq!(
        clash,
        "connection `clash` as DB: `host` would be a variable another value has: read the \
         values with `schedr task-ctx connection NAME --json` (render_failed)"
    );
    let odd = connection_against_agent(connection("odd", Some("DB")))
        .await
        .unwrap_err();
    assert!(
        odd.starts_with("connection `odd` as DB: `région` can't be a variable's name"),
        "{odd}"
    );
    // Both still read as JSON.
    let json = connection_against_agent(connection("clash", None))
        .await
        .unwrap();
    assert_eq!(json, "{\"HOST\":\"upper\",\"host\":\"lower\"}\n");
}

/// Runs a `task-ctx cleanup` command against a served channel with `handles`.
async fn cleanup_against_agent(
    command: CleanupCommand,
    handles: Arc<AttemptHandles>,
) -> Result<String, String> {
    let (ours, theirs) = channel().unwrap();
    let served = CtxInfo {
        handles: Some(handles),
        ..info("1.2.3")
    };
    let _serving = serve(ours, served);
    let env = BTreeMap::from([(CTX_FD_ENV.to_owned(), theirs.as_raw_fd().to_string())]);
    tokio::task::spawn_blocking(move || {
        let mut out = Vec::new();
        let result = run_task_ctx_cleanup(&command, &env, "1.2.3", &mut out);
        drop(theirs);
        result.map(|()| String::from_utf8(out).unwrap())
    })
    .await
    .unwrap()
}

fn cleanup_of(handles: &[&str]) -> Arc<AttemptHandles> {
    let cleanup = TaskCleanup {
        run: CleanupRun::Python,
        timeout_ms: Some(1000),
        retries: 0,
        retry_delay_ms: 0,
        handles: (!handles.is_empty()).then(|| handles.iter().map(ToString::to_string).collect()),
        anywhere: false,
    };
    Arc::new(AttemptHandles::new(
        Some(&cleanup),
        Arc::new(schedr_agent::mask::MaskSet::new(4)),
    ))
}

fn add(handle_type: &str, fields: &[&str]) -> CleanupCommand {
    CleanupCommand::Add {
        handle_type: handle_type.to_owned(),
        fields: fields.iter().map(|f| (*f).to_owned()).collect(),
        json: None,
    }
}

fn list(handle_type: Option<&str>, field: Option<&str>) -> CleanupCommand {
    CleanupCommand::List {
        handle_type: handle_type.map(str::to_owned),
        field: FieldArg {
            field: field.map(str::to_owned),
        },
    }
}

#[tokio::test]
async fn handles_added_are_listed_in_order_as_json_lines_or_one_field() {
    let handles = cleanup_of(&[]);
    for (name, fields) in [
        ("ExportJob", vec!["job_id=j-1", "rows:=3"]),
        ("TempDir", vec!["path=/tmp/a"]),
        ("ExportJob", vec!["job_id=j-2", "rows:=4"]),
    ] {
        let added = cleanup_against_agent(add(name, &fields), Arc::clone(&handles));
        assert_eq!(added.await.unwrap(), "");
    }
    let early = cleanup_against_agent(list(None, None), Arc::clone(&handles)).await;
    assert_eq!(
        early.unwrap_err(),
        "`cleanup list` works only inside a cleanup"
    );
    handles.close();
    let all = cleanup_against_agent(list(None, None), Arc::clone(&handles)).await;
    assert_eq!(
        all.unwrap().lines().collect::<Vec<_>>(),
        [
            r#"{"handle_type":"ExportJob","value":{"job_id":"j-1","rows":3}}"#,
            r#"{"handle_type":"TempDir","value":{"path":"/tmp/a"}}"#,
            r#"{"handle_type":"ExportJob","value":{"job_id":"j-2","rows":4}}"#,
        ]
    );
    let ids = list(Some("ExportJob"), Some("job_id"));
    let ids = cleanup_against_agent(ids, Arc::clone(&handles)).await;
    assert_eq!(ids.unwrap(), "j-1\nj-2\n");
    let missing = list(Some("TempDir"), Some("job_id"));
    let missing = cleanup_against_agent(missing, handles).await;
    assert_eq!(missing.unwrap_err(), "there is no field `job_id`");
}

#[tokio::test]
async fn a_handle_of_another_type_or_in_a_cleanup_is_refused() {
    let handles = cleanup_of(&["ExportJob"]);
    let refused = cleanup_against_agent(add("TempDir", &[]), Arc::clone(&handles)).await;
    assert!(refused.unwrap_err().contains("TempDir"));
    handles.close();
    let closed = cleanup_against_agent(add("ExportJob", &[]), handles).await;
    assert!(closed.unwrap_err().contains("a cleanup can't register"));
}

#[rstest::rstest]
#[case(&["job"], None, "isn't `key=value`")]
#[case(&["=v"], None, "has no key")]
#[case(&["a:={"], None, "isn't JSON")]
#[case(&["a=1", "a=2"], None, "given twice")]
#[case(&["a=1"], Some("{}"), "not both")]
#[case(&[], Some("[1]"), "one JSON object")]
#[case(&[], Some("{"), "isn't JSON")]
fn a_handle_that_cant_be_read_is_an_error(
    #[case] fields: &[&str],
    #[case] json: Option<&str>,
    #[case] message: &str,
) {
    let fields: Vec<_> = fields.iter().map(|f| (*f).to_owned()).collect();
    let error = handle_value(&fields, json).unwrap_err();
    assert!(error.contains(message), "{error}");
}

#[test]
fn a_handle_may_be_given_whole_as_json() {
    assert_eq!(
        handle_value(&[], Some(r#"{"a": [1]}"#)).unwrap(),
        json!({"a": [1]})
    );
}
