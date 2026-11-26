use std::time::Duration;

use pretty_assertions::assert_eq;

use super::*;

#[test]
fn the_script_ends_as_it_exits() {
    use std::os::unix::process::ExitStatusExt as _;
    let exited = |code: i32| std::process::ExitStatus::from_raw(code << 8);
    assert_eq!(ended(exited(0), None), Ok(Value::Null));
    assert_eq!(
        ended(exited(0), Some(serde_json::json!({"rows": 3}))),
        Ok(serde_json::json!({"rows": 3}))
    );
    assert_eq!(
        ended(exited(3), Some(serde_json::json!(1))),
        Err("the script exited with 3".to_owned())
    );
    assert_eq!(
        ended(std::process::ExitStatus::from_raw(15), None),
        Err("the script was ended by signal 15".to_owned())
    );
}

#[test]
fn a_refusals_kind_is_read_from_the_channels_words() {
    let refused = Error::Refused("secret_denied: `a/b` isn't declared".to_owned());
    assert_eq!(
        kind_of(&refused, "secret_unavailable"),
        (
            "secret_denied".to_owned(),
            "`a/b` isn't declared".to_owned()
        )
    );
    let plain = Error::Refused("the socket broke".to_owned());
    assert_eq!(
        kind_of(&plain, "secret_unavailable"),
        (
            "secret_unavailable".to_owned(),
            "the socket broke".to_owned()
        )
    );
}

/// An extension task's request as a worker sends it, its run carrying `extra`.
fn raw(
    cleanup: Option<serde_json::Value>,
    extra: &serde_json::Value,
) -> schedr_extension::ExtensionTask {
    let mut run = serde_json::json!({
        "dag_id": "backups", "run_id": "r-1", "kind": "manual",
        "planned_for": "2026-11-21T10:00:00Z", "since": "2026-11-21T10:00:00Z",
        "try_number": 2, "params": {"day": "mon"},
    });
    for (key, value) in extra.as_object().into_iter().flatten() {
        run[key] = value.clone();
    }
    let mut task = serde_json::json!({
        "dag_id": "backups",
        "task": {"id": "nightly", "kind": "extension", "name": "backup", "version": "1.0",
                 "payload": {"source": "/data"}, "settings": {"max_output_size": 64}},
        "run": run,
    });
    if let Some(handles) = cleanup {
        task["cleanup"] = handles;
    }
    serde_json::from_value(task).unwrap()
}

/// A plugin's message, read as the worker does: a 4-byte big-endian length, then JSON.
fn read_plugin_message(stream: &mut impl std::io::Read) -> std::io::Result<PluginMessage> {
    let mut size = [0; 4];
    stream.read_exact(&mut size)?;
    let mut body = vec![0; u32::from_be_bytes(size) as usize];
    stream.read_exact(&mut body)?;
    serde_json::from_slice(&body).map_err(std::io::Error::other)
}

/// A worker's message, written as the worker does.
fn write_worker_message(stream: &mut impl Write, message: &WorkerMessage) -> std::io::Result<()> {
    let body = serde_json::to_vec(message).map_err(std::io::Error::other)?;
    stream.write_all(&u32::try_from(body.len()).unwrap().to_be_bytes())?;
    stream.write_all(&body)
}

/// A bridge over a socket pair, and a fake worker answering each message with `answer`.
fn bridge_with(
    raw: schedr_extension::ExtensionTask,
    answer: impl Fn(PluginMessage) -> Option<WorkerMessage> + Send + 'static,
) -> Bridge {
    let (ours, mut worker) = std::os::unix::net::UnixStream::pair().unwrap();
    std::thread::spawn(move || {
        while let Ok(message) = read_plugin_message(&mut worker) {
            if let Some(reply) = answer(message) {
                let _ = write_worker_message(&mut worker, &reply);
            }
        }
    });
    let request = Request::of(raw).unwrap();
    let run: ShellRun = serde_json::from_value(request.raw.run.clone()).unwrap();
    let channel = Channel::new(ours, &request.raw);
    Bridge::new(&request, &run, channel, 64)
}

#[test]
fn a_bridge_answers_from_its_request_and_refuses_after_the_script_ended() {
    let bridge = bridge_with(raw(None, &serde_json::json!({})), |_| None);
    assert_eq!(
        bridge.answered(CtxRequest::ParamsGet),
        CtxAnswer::Params {
            params: serde_json::json!({"day": "mon"})
        }
    );
    assert!(matches!(
        bridge.answered(CtxRequest::RunGet),
        CtxAnswer::Run(CtxRun { ref id, .. }) if id == "r-1"
    ));
    // The output within the task's limit kept, past it refused.
    assert_eq!(
        bridge.answered(CtxRequest::OutputSet {
            value: serde_json::json!({"files": 3})
        }),
        CtxAnswer::Reported
    );
    assert!(matches!(
        bridge.answered(CtxRequest::OutputSet {
            value: serde_json::json!("x".repeat(100))
        }),
        CtxAnswer::ReportRefused { .. }
    ));
    assert_eq!(bridge.output(), Some(serde_json::json!({"files": 3})));
    assert!(matches!(
        bridge.answered(CtxRequest::HandleList),
        CtxAnswer::Error { .. }
    ));
    // agy's review of M5.2i6: nothing passed on once the script has ended.
    bridge.close();
    assert_eq!(
        bridge.answered(CtxRequest::RunGet),
        CtxAnswer::Error {
            reason: "the script has ended".to_owned()
        }
    );
}

#[test]
fn a_cleanups_bridge_lists_its_handles_and_registers_none() {
    let handles = serde_json::json!([{"handle_type": "Snapshot", "value": {"id": "s-1"}}]);
    let bridge = bridge_with(raw(Some(handles), &serde_json::json!({})), |_| None);
    assert_eq!(
        bridge.answered(CtxRequest::HandleList),
        CtxAnswer::Handles {
            handles: vec![CleanupHandle {
                handle_type: "Snapshot".to_owned(),
                value: serde_json::json!({"id": "s-1"}),
            }]
        }
    );
    assert!(matches!(
        bridge.answered(CtxRequest::HandleAdd {
            handle: CleanupHandle {
                handle_type: "Snapshot".to_owned(),
                value: serde_json::json!({}),
            }
        }),
        CtxAnswer::HandleRefused { .. }
    ));
}

#[test]
fn an_api_answer_keeps_its_note_and_one_past_the_channel_is_a_502() {
    // agy's review of M5.2i6: the masking note was dropped, a body over 1 MiB broke the call.
    let bridge = bridge_with(raw(None, &serde_json::json!({})), |message| match message {
        PluginMessage::ApiCall { path, .. } => Some(WorkerMessage::ApiAnswer {
            status: 200,
            body: if path == "/big" {
                serde_json::json!("x".repeat(2 << 20))
            } else {
                serde_json::json!({"ok": true})
            },
            note: Some("a secret masked".to_owned()),
        }),
        _ => None,
    });
    let call = |path: &str| CtxRequest::ApiCall {
        method: "GET".to_owned(),
        path: path.to_owned(),
        body: None,
        idempotency_key: None,
    };
    assert_eq!(
        bridge.answered(call("/small")),
        CtxAnswer::ApiAnswer {
            status: 200,
            body: serde_json::json!({"ok": true}),
            note: Some("a secret masked".to_owned()),
        }
    );
    assert!(matches!(
        bridge.answered(call("/big")),
        CtxAnswer::ApiAnswer { status: 502, .. }
    ));
}

#[test]
fn a_run_field_the_shell_doesnt_know_is_ignored() {
    // agy's review of M5.2i6: a newer worker's additive field refused the request.
    let run: ShellRun =
        serde_json::from_value(raw(None, &serde_json::json!({"future_field": 1})).run).unwrap();
    assert_eq!((run.run_id.as_str(), run.try_number), ("r-1", 2));
}

#[test]
fn a_script_leaving_a_large_payload_unread_ends_the_run_all_the_same() {
    // agy's review of M5.2i6: the payload past the pipe's buffer was written before the wait; a
    // process the script left running, holding stdin, held the plugin up for good.
    let mut big = raw(None, &serde_json::json!({}));
    big.task.payload = serde_json::json!("x".repeat(256 << 10));
    let request = Request::of(big).unwrap();
    let run: ShellRun = serde_json::from_value(request.raw.run.clone()).unwrap();
    let (ours, _worker) = std::os::unix::net::UnixStream::pair().unwrap();
    let bridge = Arc::new(Bridge::new(
        &request,
        &run,
        Channel::new(ours, &request.raw),
        64,
    ));
    let env = BTreeMap::from([("PATH".to_owned(), "/usr/bin:/bin".to_owned())]);
    let command: Vec<OsString> = ["sh", "-c", "sleep 3 >/dev/null 2>&1 & exit 0"]
        .iter()
        .map(OsString::from)
        .collect();
    let started = std::time::Instant::now();
    assert_eq!(
        run_script(&command, &env, &request, &run, &bridge),
        Ok(Value::Null)
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
}

/// M5.2i9 (agy's review): a call to a hung service holds neither the worker's requests nor the
/// plugin's result; the service's answer reaches the script.
#[test]
fn a_hung_service_call_holds_neither_other_requests_nor_the_result() {
    let (ours, mut worker) = std::os::unix::net::UnixStream::pair().unwrap();
    let (service_end, mut service) = std::os::unix::net::UnixStream::pair().unwrap();
    let request = Request::of(raw(None, &serde_json::json!({}))).unwrap();
    let run: ShellRun = serde_json::from_value(request.raw.run.clone()).unwrap();
    let channel = Channel::new(ours, &request.raw).with_service(Some(service_end));
    let bridge = Arc::new(Bridge::new(&request, &run, channel, 64));
    // A service that reads the call and never answers.
    let (got, heard) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut size = [0; 4];
        let _ = std::io::Read::read_exact(&mut service, &mut size);
        let _ = got.send(());
        std::thread::sleep(Duration::from_secs(30));
    });
    let calling = Arc::clone(&bridge);
    std::thread::spawn(move || {
        let _ = calling.answered(CtxRequest::ServiceCall {
            method: "slow".to_owned(),
            args: serde_json::Value::Null,
        });
    });
    heard.recv_timeout(Duration::from_secs(5)).unwrap();
    // Meanwhile: another request, and the result to the worker.
    assert!(matches!(
        bridge.answered(CtxRequest::ParamsGet),
        CtxAnswer::Params { .. }
    ));
    bridge.close();
    let result = schedr_extension::result_of(Ok(serde_json::json!(1)), 64);
    bridge.channel().send(&result).unwrap();
    worker
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    assert!(matches!(
        read_plugin_message(&mut worker).unwrap(),
        PluginMessage::TaskSucceeded { .. }
    ));
}

/// M5.2i9: a script's `task-ctx extension call` reaches the service and prints its result;
/// without a service, or after the script ended, it is told why.
#[test]
fn a_scripts_service_call_is_answered_or_told_why_not() {
    let (ours, _worker) = std::os::unix::net::UnixStream::pair().unwrap();
    let (service_end, mut service) = std::os::unix::net::UnixStream::pair().unwrap();
    let request = Request::of(raw(None, &serde_json::json!({}))).unwrap();
    let run: ShellRun = serde_json::from_value(request.raw.run.clone()).unwrap();
    let channel = Channel::new(ours, &request.raw).with_service(Some(service_end));
    let bridge = Bridge::new(&request, &run, channel, 64);
    std::thread::spawn(move || {
        let mut size = [0; 4];
        std::io::Read::read_exact(&mut service, &mut size).unwrap();
        let mut body = vec![0; u32::from_be_bytes(size) as usize];
        std::io::Read::read_exact(&mut service, &mut body).unwrap();
        let call: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // Another call's answer first (skipped), then its own.
        for id in [99, call["id"].as_u64().unwrap()] {
            let answer = serde_json::to_vec(&serde_json::json!(
                {"type": "answer", "id": id, "result": {"echo": call["args"]}}
            ))
            .unwrap();
            service
                .write_all(&u32::try_from(answer.len()).unwrap().to_be_bytes())
                .unwrap();
            service.write_all(&answer).unwrap();
        }
    });
    assert_eq!(
        bridge.answered(CtxRequest::ServiceCall {
            method: "echo".to_owned(),
            args: serde_json::json!([1]),
        }),
        CtxAnswer::ServiceAnswer {
            result: serde_json::json!({"echo": [1]})
        }
    );
    bridge.close();
    assert!(matches!(
        bridge.answered(CtxRequest::ServiceCall {
            method: "echo".to_owned(),
            args: serde_json::Value::Null,
        }),
        CtxAnswer::Error { .. }
    ));
    let without = bridge_with(raw(None, &serde_json::json!({})), |_| None);
    let CtxAnswer::Error { reason } = without.answered(CtxRequest::ServiceCall {
        method: "echo".to_owned(),
        args: serde_json::Value::Null,
    }) else {
        panic!("answered without a service");
    };
    assert!(reason.contains("no service"), "{reason}");
}

/// agy's review of M5.3c (rule P): the worker's folder joins a task's `PATH` only as an absolute
/// path without `:` (a `:` would add an empty, current-directory entry), once, and with no
/// `PATH` set the system's default stays after it.
#[test]
fn the_workers_folder_joins_path_only_as_a_safe_entry_and_once() {
    use std::path::Path;
    let own = Some(Path::new("/opt/schedr/bin"));
    assert_eq!(
        super::path_with(own, Some("/usr/bin:/bin")).as_deref(),
        Some("/opt/schedr/bin:/usr/bin:/bin")
    );
    assert_eq!(
        super::path_with(own, Some("/opt/schedr/bin:/usr/bin")).as_deref(),
        Some("/opt/schedr/bin:/usr/bin"),
        "first already: not twice"
    );
    assert_eq!(
        super::path_with(own, None).as_deref(),
        Some("/opt/schedr/bin:/usr/bin:/bin"),
        "no PATH: the system's default kept reachable"
    );
    for odd in ["/tmp/a:b/bin", "relative/bin", ""] {
        assert_eq!(
            super::path_with(Some(Path::new(odd)), Some("/usr/bin")).as_deref(),
            Some("/usr/bin"),
            "{odd:?} not added"
        );
    }
    assert_eq!(super::path_with(None, None), None, "nothing to set");
    assert_eq!(
        super::path_with(None, Some("/usr/bin")).as_deref(),
        Some("/usr/bin")
    );
}
