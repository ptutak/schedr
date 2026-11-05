use std::io::{IoSlice, Read, Write};
use std::os::fd::AsFd;

use pretty_assertions::assert_eq;
use rustix::net::{SendAncillaryBuffer, SendAncillaryMessage, SendFlags};
use schedr_model::states::RunKind;
use serde_json::json;

use super::*;

fn info() -> CtxInfo {
    CtxInfo {
        run: CtxRun {
            dag_id: "report".into(),
            id: "r1".into(),
            kind: RunKind::Manual,
            planned_for: "2026-11-01T08:00:00Z".parse().unwrap(),
            since: "2026-10-31T08:00:00Z".parse().unwrap(),
        },
        params: json!({"region": "eu", "day": {"$date": "2026-10-01"}}),
        version: "1.2.3".into(),
        secrets: None,
        handles: None,
        resource: None,
    }
}

fn frame(message: &serde_json::Value) -> Vec<u8> {
    let body = serde_json::to_vec(message).unwrap();
    let mut out = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
    out.extend(body);
    out
}

fn answers(mut bytes: &[u8]) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    while !bytes.is_empty() {
        let size = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
        out.push(serde_json::from_slice(&bytes[4..4 + size]).unwrap());
        bytes = &bytes[4 + size..];
    }
    out
}

/// The answers to `requests`, sent at once, then the end of the stream.
async fn conversation(requests: &[serde_json::Value]) -> Vec<serde_json::Value> {
    conversation_with(requests, &info()).await
}

async fn conversation_with(
    requests: &[serde_json::Value],
    info: &CtxInfo,
) -> Vec<serde_json::Value> {
    let (mut ours, mut theirs) = tokio::io::duplex(1 << 20);
    let mut input = Vec::new();
    for request in requests {
        input.extend(frame(request));
    }
    ours.write_all(&input).await.unwrap();
    ours.shutdown().await.unwrap();
    converse(&mut theirs, info).await.unwrap();
    drop(theirs);
    let mut output = Vec::new();
    ours.read_to_end(&mut output).await.unwrap();
    answers(&output)
}

#[tokio::test]
async fn answers_params_and_run_after_hello() {
    let got = conversation(&[
        json!({"type": "hello", "protocol": 1, "schedr_version": "1.2.3"}),
        json!({"type": "params_get"}),
        json!({"type": "run_get"}),
    ])
    .await;
    assert_eq!(
        got,
        vec![
            json!({"type": "welcome"}),
            json!({"type": "params", "params": {"region": "eu", "day": {"$date": "2026-10-01"}}}),
            json!({"type": "run", "dag_id": "report", "id": "r1", "kind": "manual",
                   "planned_for": "2026-11-01T08:00:00Z", "since": "2026-10-31T08:00:00Z"}),
        ]
    );
}

#[tokio::test]
async fn accepts_another_schedr_version_speaking_the_same_protocol() {
    let got = conversation(&[
        json!({"type": "hello", "protocol": 1, "schedr_version": "0.9.0"}),
        json!({"type": "run_get"}),
    ])
    .await;
    assert_eq!(got[0], json!({"type": "welcome"}));
    assert_eq!(got[1]["id"], "r1");
}

#[tokio::test]
async fn refuses_another_protocol_and_answers_nothing_more() {
    let got = conversation(&[
        json!({"type": "hello", "protocol": 2, "schedr_version": "2.0.0"}),
        json!({"type": "params_get"}),
    ])
    .await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0]["type"], "refused");
    let reason = got[0]["reason"].as_str().unwrap();
    assert!(
        reason.contains("2.0.0") && reason.contains("protocol 2") && reason.contains("1.2.3"),
        "{reason}"
    );
}

#[tokio::test]
async fn a_request_before_hello_is_an_error() {
    let got = conversation(&[json!({"type": "params_get"}), json!({"type": "run_get"})]).await;
    assert_eq!(
        got,
        vec![json!({"type": "error", "reason": "say `hello` first"})]
    );
}

#[tokio::test]
async fn an_unknown_request_ends_the_connection_with_an_error() {
    let got = conversation(&[
        json!({"type": "hello", "protocol": 1, "schedr_version": "1.2.3"}),
        json!({"type": "secret_get"}),
        json!({"type": "run_get"}),
    ])
    .await;
    assert_eq!(got.len(), 2);
    assert_eq!(got[1]["type"], "error");
}

#[tokio::test]
async fn a_message_over_the_limit_ends_the_connection() {
    let (mut ours, mut theirs) = tokio::io::duplex(64);
    ours.write_all(&(MAX_CTX_MESSAGE_SIZE + 1).to_be_bytes())
        .await
        .unwrap();
    let result = converse(&mut theirs, &info()).await;
    assert!(matches!(result, Err(CtxError::TooLarge(_))), "{result:?}");
}

/// Sends `fds` in one datagram over the task's end.
fn send(task_end: &OwnedFd, fds: &[OwnedFd]) {
    let borrowed: Vec<_> = fds.iter().map(AsFd::as_fd).collect();
    let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(4))];
    let mut control = SendAncillaryBuffer::new(&mut space);
    if !borrowed.is_empty() {
        assert!(control.push(SendAncillaryMessage::ScmRights(&borrowed)));
    }
    rustix::net::sendmsg(
        task_end,
        &[IoSlice::new(b"c")],
        &mut control,
        SendFlags::empty(),
    )
    .unwrap();
}

/// A blocking client: a fresh stream pair, one end sent, requests on the other.
fn call(task_end: &OwnedFd, requests: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let (mut mine, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    send(task_end, &[OwnedFd::from(theirs)]);
    for request in requests {
        mine.write_all(&frame(request)).unwrap();
    }
    mine.shutdown(std::net::Shutdown::Write).unwrap();
    let mut output = Vec::new();
    mine.read_to_end(&mut output).unwrap();
    answers(&output)
}

#[tokio::test(flavor = "multi_thread")]
async fn serves_each_sent_end_as_its_own_connection() {
    let (ours, theirs) = channel().unwrap();
    let serving = serve(ours, info());
    let task_end = Arc::new(theirs);
    let calls: Vec<_> = (0..8)
        .map(|_| {
            let task_end = Arc::clone(&task_end);
            tokio::task::spawn_blocking(move || {
                call(
                    &task_end,
                    &[
                        json!({"type": "hello", "protocol": 1, "schedr_version": "1.2.3"}),
                        json!({"type": "run_get"}),
                    ],
                )
            })
        })
        .collect();
    for handle in calls {
        let got = handle.await.unwrap();
        assert_eq!(got[0], json!({"type": "welcome"}));
        assert_eq!(got[1]["id"], "r1");
    }
    drop(serving);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_datagram_without_exactly_one_stream_end_is_skipped() {
    let (ours, theirs) = channel().unwrap();
    let _serving = serve(ours, info());
    let task_end = Arc::new(theirs);
    let skipped = Arc::clone(&task_end);
    let got = tokio::task::spawn_blocking(move || {
        // None, two, and one that isn't a stream socket: each closed, then a real call.
        send(&skipped, &[]);
        let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        send(&skipped, &[OwnedFd::from(a), OwnedFd::from(b)]);
        let (dgram, _keep) = std::os::unix::net::UnixDatagram::pair().unwrap();
        send(&skipped, &[OwnedFd::from(dgram)]);
        call(
            &skipped,
            &[
                json!({"type": "hello", "protocol": 1, "schedr_version": "1.2.3"}),
                json!({"type": "params_get"}),
            ],
        )
    })
    .await
    .unwrap();
    assert_eq!(got[1]["params"]["region"], "eu");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_after_the_attempt_ended_is_refused_by_the_socket() {
    let (ours, theirs) = channel().unwrap();
    drop(serve(ours, info()));
    // The serving task holds the agent's end; it is closed once the task is aborted.
    let task_end = theirs;
    let refused = tokio::task::spawn_blocking(move || {
        for _ in 0..100 {
            let (_mine, end) = std::os::unix::net::UnixStream::pair().unwrap();
            let borrowed = [end.as_fd()];
            let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
            let mut control = SendAncillaryBuffer::new(&mut space);
            control.push(SendAncillaryMessage::ScmRights(&borrowed));
            let sent = rustix::net::sendmsg(
                &task_end,
                &[IoSlice::new(b"c")],
                &mut control,
                SendFlags::empty(),
            );
            if let Err(errno) = sent {
                return errno == rustix::io::Errno::CONNREFUSED;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        false
    })
    .await
    .unwrap();
    assert!(
        refused,
        "sending to an ended attempt's channel kept succeeding"
    );
}

/// The descriptors open in this process (nextest runs each test in a process of its own).
#[cfg(target_os = "linux")]
fn open_descriptors() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn a_datagram_with_many_descriptors_leaves_none_open() {
    let (ours, theirs) = channel().unwrap();
    let _serving = serve(ours, info());
    let task_end = Arc::new(theirs);
    let sender = Arc::clone(&task_end);
    let (before, after, got) = tokio::task::spawn_blocking(move || {
        let before = open_descriptors();
        {
            // More than the old buffer's four, each a stream end the agent could serve.
            let pairs: Vec<_> = (0..20)
                .map(|_| std::os::unix::net::UnixStream::pair().unwrap())
                .collect();
            let ends: Vec<OwnedFd> = pairs
                .iter()
                .map(|(end, _)| OwnedFd::from(end.try_clone().unwrap()))
                .collect();
            let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(20))];
            let mut control = SendAncillaryBuffer::new(&mut space);
            let borrowed: Vec<_> = ends.iter().map(AsFd::as_fd).collect();
            assert!(control.push(SendAncillaryMessage::ScmRights(&borrowed)));
            rustix::net::sendmsg(
                &*sender,
                &[IoSlice::new(b"c")],
                &mut control,
                SendFlags::empty(),
            )
            .unwrap();
        }
        // A call after it is served: the agent read the datagram before this one.
        let got = call(
            &sender,
            &[
                json!({"type": "hello", "protocol": 1, "schedr_version": "1"}),
                json!({"type": "run_get"}),
            ],
        );
        (before, open_descriptors(), got)
    })
    .await
    .unwrap();
    assert_eq!(got[1]["id"], "r1");
    // The agent closed all twenty it received: none of them stays open in this process.
    assert_eq!(after, before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_over_the_limit_is_told_why() {
    let (ours, theirs) = channel().unwrap();
    let _serving = serve(ours, info());
    let task_end = Arc::new(theirs);
    let sender = Arc::clone(&task_end);
    let refused = tokio::task::spawn_blocking(move || {
        // Each held open after its welcome.
        let held: Vec<_> = (0..MAX_CONNECTIONS)
            .map(|_| {
                let (mut mine, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
                send(&sender, &[OwnedFd::from(theirs)]);
                mine.write_all(&frame(
                    &json!({"type": "hello", "protocol": 1, "schedr_version": "1"}),
                ))
                .unwrap();
                let mut header = [0; 4];
                mine.read_exact(&mut header).unwrap();
                let mut body = vec![0; u32::from_be_bytes(header) as usize];
                mine.read_exact(&mut body).unwrap();
                mine
            })
            .collect();
        let refused = call(&sender, &[]);
        drop(held);
        refused
    })
    .await
    .unwrap();
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert_eq!(refused[0]["type"], "error");
    let reason = refused[0]["reason"].as_str().unwrap();
    assert!(reason.contains("64 context connections open"), "{reason}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_clearing_o_nonblock_on_its_end_blocks_no_runtime_thread() {
    let (ours, theirs) = channel().unwrap();
    let _serving = serve(ours, info());
    let task_end = Arc::new(theirs);
    let sender = Arc::clone(&task_end);
    let (held, answered) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::task::spawn_blocking(move || {
            // As many as the runtime has threads: each keeps the end it sent, clears
            // `O_NONBLOCK` on it (shared with the agent's copy), then sends half a message.
            let held: Vec<_> = (0..2)
                .map(|_| {
                    let (mut mine, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
                    let kept = theirs.try_clone().unwrap();
                    send(&sender, &[OwnedFd::from(theirs)]);
                    mine.write_all(&frame(
                        &json!({"type": "hello", "protocol": 1, "schedr_version": "1"}),
                    ))
                    .unwrap();
                    let mut header = [0; 4];
                    mine.read_exact(&mut header).unwrap();
                    let mut body = vec![0; u32::from_be_bytes(header) as usize];
                    mine.read_exact(&mut body).unwrap();
                    kept.set_nonblocking(false).unwrap();
                    mine.write_all(&1024_u32.to_be_bytes()).unwrap();
                    (mine, kept)
                })
                .collect::<Vec<_>>();
            std::thread::sleep(std::time::Duration::from_millis(200));
            // The agent still answers another call.
            let answered = call(
                &sender,
                &[
                    json!({"type": "hello", "protocol": 1, "schedr_version": "1"}),
                    json!({"type": "run_get"}),
                ],
            );
            (held, answered)
        }),
    )
    .await
    .expect("the agent stopped answering: a runtime thread is blocked")
    .unwrap();
    assert_eq!(answered[1]["id"], "r1");
    drop(held);
}

#[tokio::test]
async fn reads_a_declared_secret_and_refuses_the_others() {
    use crate::secrets::{AttemptSecrets, MemoryStorage, SecretStores};
    use schedr_model::{SecretDecl, SecretName};
    let name = |path: &str, key: &str| SecretName {
        storage: None,
        path: path.into(),
        key: Some(key.into()),
        default: None,
    };
    let stores = Arc::new(SecretStores::for_test(MemoryStorage::new([(
        name("etl/db", "password"),
        "hunter22".to_owned(),
    )])));
    let declared = vec![SecretDecl::Secret(name("etl/db", "password"))];
    let secrets = AttemptSecrets::fetch(stores, &[name("etl/db", "password")], declared)
        .await
        .unwrap();
    let mut info = info();
    info.secrets = Some(Arc::new(secrets));
    let hello = json!({"type": "hello", "protocol": 1, "schedr_version": "1.2.3"});
    let get =
        |path: &str, key: &str| json!({"type": "secret_get", "secret": {"path": path, "key": key}});
    let got = conversation_with(
        &[
            hello.clone(),
            get("etl/db", "password"),
            get("etl/db", "user"),
        ],
        &info,
    )
    .await;
    assert_eq!(got[1], json!({"type": "secret", "value": "hunter22"}));
    assert_eq!(got[2]["type"], "secret_failed");
    assert_eq!(got[2]["kind"], "secret_denied");
    assert_eq!(
        info.secrets.as_ref().unwrap().failed_kind(),
        Some("secret_denied")
    );
    // Without the attempt's secrets (a parse, a test's context), every read is refused.
    let got = conversation(&[hello, get("etl/db", "password")]).await;
    assert_eq!(got[1]["kind"], "secret_denied");
}
