use std::os::unix::net::UnixStream;

use pretty_assertions::assert_eq;

use super::*;

struct Echo;

impl Plugin for Echo {
    fn name(&self) -> &'static str {
        "echo"
    }

    fn versions(&self) -> Vec<String> {
        vec!["1.0".to_owned()]
    }

    fn run(&mut self, request: &Request, channel: &mut Channel) -> Result<Value, String> {
        let given = channel.secret("given", Some("k"), None)?;
        let asked = channel.secret("etl/api", Some("token"), None)?;
        channel.add_handle("job", json!({"id": 7}))?;
        Ok(json!({"payload": request.payload, "given": given, "asked": asked}))
    }
}

fn request(cleanup: Option<&[Handle]>) -> WorkerMessage {
    let message = json!({
        "type": "extension_task",
        "dag_file": {"root": "etl", "root_dir": "/dags/etl", "path": "x.py"},
        "dag_id": "extended",
        "task": {
            "id": "job", "kind": "extension", "name": "echo", "version": "1.0",
            "payload": {"go": true},
            "settings": {
                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null, "queue": "default",
                "max_output_size": 1024
            },
            "output_type": {"type": "any"}
        },
        "inputs": {}, "outcomes": {},
        "run": {
            "dag_id": "extended", "run_id": "01", "kind": "manual",
            "planned_for": "2026-11-20T00:00:00Z", "since": "2026-11-19T00:00:00Z", "try_number": 1
        },
        "sys_path": [],
        "secrets": [{"name": {"path": "given", "key": "k"}, "value": "inline"}],
        "cleanup": cleanup,
    });
    serde_json::from_value(message).unwrap()
}

#[test]
fn a_task_asks_the_worker_and_ends_with_its_output() {
    let (mut worker, plugin) = UnixStream::pair().unwrap();
    let served = std::thread::spawn(move || serve(&mut Echo, plugin));
    let hello: PluginMessage = read_runner(&mut worker);
    assert_eq!(
        hello,
        PluginMessage::Hello {
            schedr_version: env!("CARGO_PKG_VERSION").to_owned(),
            mode: "extension".to_owned(),
            protocol: PROTOCOL,
        }
    );
    write_agent(&mut worker, &WorkerMessage::Welcome);
    write_agent(&mut worker, &request(None));
    let asked: PluginMessage = read_runner(&mut worker);
    assert_eq!(
        asked,
        PluginMessage::SecretGet {
            secret: SecretName {
                storage: None,
                path: "etl/api".to_owned(),
                key: Some("token".to_owned()),
            }
        }
    );
    write_agent(
        &mut worker,
        &WorkerMessage::Secret {
            value: json!("s3cret"),
        },
    );
    let PluginMessage::HandleAdd { handle } = read_runner(&mut worker) else {
        panic!("a handle");
    };
    assert_eq!(handle.value, json!({"id": 7}));
    write_agent(&mut worker, &WorkerMessage::HandleAdded);
    let result: PluginMessage = read_runner(&mut worker);
    assert_eq!(
        result,
        PluginMessage::TaskSucceeded {
            output: json!({"payload": {"go": true}, "given": "inline", "asked": "s3cret"}),
            outcome: "SUCCESS".to_owned(),
        }
    );
    assert!(served.join().unwrap().unwrap());
}

#[test]
fn a_refusal_and_a_missing_cleanup_and_a_large_output_fail() {
    let (mut worker, plugin) = UnixStream::pair().unwrap();
    let served = std::thread::spawn(move || serve(&mut Echo, plugin));
    let _: PluginMessage = read_runner(&mut worker);
    write_agent(
        &mut worker,
        &WorkerMessage::Refused {
            reason: "protocol 2".to_owned(),
        },
    );
    assert!(matches!(served.join().unwrap(), Err(Error::Refused(r)) if r == "protocol 2"));

    let (mut worker, plugin) = UnixStream::pair().unwrap();
    let served = std::thread::spawn(move || serve(&mut Echo, plugin));
    let _: PluginMessage = read_runner(&mut worker);
    write_agent(&mut worker, &WorkerMessage::Welcome);
    write_agent(&mut worker, &request(Some(&[])));
    let PluginMessage::TaskFailed { error, .. } = read_runner(&mut worker) else {
        panic!("a failure");
    };
    assert_eq!(error.message, "extension `echo` has no cleanup");
    assert!(!served.join().unwrap().unwrap());

    let PluginMessage::TaskFailed { kind, .. } = result_of(Ok(json!("x".repeat(2000))), 1024)
    else {
        panic!("a failure");
    };
    assert_eq!(kind.as_deref(), Some("output_too_large"));
}

#[test]
fn describe_and_a_missing_socket() {
    assert_eq!(run_main(Echo, &["--describe".to_owned()], None), 0);
    assert_eq!(run_main(Echo, &[], None), 2);
    assert_eq!(run_main(Echo, &[], Some("1")), 2, "stdout is no socket");
}

fn read_runner(stream: &mut UnixStream) -> PluginMessage {
    let mut header = [0; 4];
    stream.read_exact(&mut header).unwrap();
    let mut body = vec![0; u32::from_be_bytes(header) as usize];
    stream.read_exact(&mut body).unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn write_agent(stream: &mut UnixStream, message: &WorkerMessage) {
    let body = serde_json::to_vec(message).unwrap();
    stream
        .write_all(&u32::try_from(body.len()).unwrap().to_be_bytes())
        .unwrap();
    stream.write_all(&body).unwrap();
}

#[test]
fn the_workers_socket_is_copied_and_its_original_sealed() {
    use std::os::fd::AsRawFd as _;
    use std::os::unix::net::UnixDatagram;

    let (inherited, mut worker) = UnixStream::pair().unwrap();
    let mut copy = crate::socket::inherited(inherited.as_raw_fd()).unwrap();
    assert_ne!(copy.as_raw_fd(), inherited.as_raw_fd());
    let original = rustix::io::fcntl_getfd(&inherited).unwrap();
    assert!(
        original.contains(rustix::io::FdFlags::CLOEXEC),
        "a process the plugin starts doesn't get the worker's channel"
    );
    copy.write_all(b"via the copy").unwrap();
    let mut buffer = [0; 12];
    worker.read_exact(&mut buffer).unwrap();
    assert_eq!(&buffer, b"via the copy");
    let (datagram, _peer) = UnixDatagram::pair().unwrap();
    assert_eq!(
        crate::socket::inherited(datagram.as_raw_fd()).err(),
        Some(crate::socket::SocketError::NotStream(datagram.as_raw_fd()))
    );
    assert_eq!(
        crate::socket::inherited(2).err(),
        Some(crate::socket::SocketError::Reserved(2))
    );
}
