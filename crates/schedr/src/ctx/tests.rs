use std::os::fd::AsRawFd;

use pretty_assertions::assert_eq;
use schedr_agent::ctx::{CtxInfo, channel, serve};
use schedr_model::ctx::CtxRun;
use schedr_model::states::RunKind;
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
        // The aborted server drops the agent's end at its next poll.
        let mut last = Ok(());
        for _ in 0..100 {
            last = run_dag_ctx(&params(None), &env, "1.0.0", &mut Vec::new());
            if last.is_err() {
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
