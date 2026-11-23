use super::*;

#[test]
fn restarts_wait_longer_each_time_up_to_the_most() {
    let timings = ServiceTimings::default();
    let first = timings.next(None);
    assert_eq!(first, Duration::from_secs(1));
    assert_eq!(timings.next(Some(first)), Duration::from_secs(2));
    assert_eq!(
        timings.next(Some(Duration::from_secs(50))),
        Duration::from_mins(1)
    );
}

/// Grok's review: a service's line with no end is logged in pieces, never held whole.
#[tokio::test]
async fn a_long_line_is_read_in_pieces() {
    let mut text = vec![b'a'; 40 * 1024];
    text.extend_from_slice(b"\nend\n");
    let mut lines: Vec<usize> = Vec::new();
    read_lines(text.as_slice(), |line| lines.push(line.len())).await;
    assert_eq!(lines, [MAX_LINE, MAX_LINE, 40 * 1024 - 2 * MAX_LINE + 1, 4]);
}

#[test]
fn versions_compare_by_their_numbers() {
    assert_eq!(version_order("1.10", "1.2"), std::cmp::Ordering::Greater);
    assert_eq!(version_order("2.0", "10.0"), std::cmp::Ordering::Less);
}

// The supervisor against a fake service in plain Python speaking the control protocol: the
// faults agy's review of M5.2i9 found, each proved here.

use schedr_model::extension::{Declarations, EventKind, ExtensionEvent};
use tempfile::TempDir;

use crate::secrets::MemoryStorage;

/// A Python 3: the fake service imports nothing of schedr.
fn python() -> PathBuf {
    std::env::var_os("SCHEDR_TEST_PYTHON").map_or_else(|| PathBuf::from("python3"), PathBuf::from)
}

/// The fake service: `MODE` says what it does after its hello.
const FAKE: &str = r#"
import json, os, socket, struct, sys, time
s = socket.socket(fileno=int(os.environ["SCHEDR_SERVICE_FD"]))
def send(m):
    b = json.dumps(m).encode()
    s.sendall(struct.pack(">I", len(b)) + b)
def recv():
    h = b""
    while len(h) < 4:
        c = s.recv(4 - len(h))
        if not c:
            sys.exit(0)
        h += c
    n = struct.unpack(">I", h)[0]
    b = b""
    while len(b) < n:
        b += s.recv(n - len(b))
    return json.loads(b)
mode = os.environ["MODE"]
send({"type": "hello", "schedr_version": "x", "mode": "service", "protocol": 1,
      "name": "fake", "versions": ["1.0", "2.0"]})
welcome = recv()
if mode == "never_ready":
    time.sleep(60)
if mode == "grow_starting":
    grown = bytearray(64 << 20)
    grown[::4096] = b"x" * len(grown[::4096])
    time.sleep(60)
if mode == "slow_ready":
    time.sleep(1.5)
send({"type": "ready"})
if mode == "flood":
    for _ in range(50000):
        send({"type": "secret_get", "secret": {"path": "svc", "key": "token"}})
if mode == "serve":
    while True:
        h, anc, _, _ = s.recvmsg(4, socket.CMSG_SPACE(4))
        if not h:
            sys.exit(0)
        while len(h) < 4:
            h += s.recv(4 - len(h))
        n = struct.unpack(">I", h)[0]
        b = b""
        while len(b) < n:
            b += s.recv(n - len(b))
        m = json.loads(b)
        if m["type"] == "attempt_connected":
            send({"type": "attempt_accepted", "attempt_id": m["attempt_id"]})
        elif m["type"] in ("drained", "resumed"):
            send({"type": "status", "phase": m["type"]})
if mode == "tell":
    token = welcome["secrets"][0]["value"]
    send({"type": "status", "phase": "connecting", "message": "using " + token})
    send({"type": "metrics", "points": [{"name": "jobs", "labels": {"detail": token}, "value": 1}]})
time.sleep(60)
"#;

struct Fake {
    _dir: TempDir,
    services: Services,
    figures: Figures,
    stop: watch::Sender<bool>,
}

fn start(mode: &str, change: impl FnOnce(&mut ServiceSpec)) -> Fake {
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("fake.py");
    std::fs::write(&script, FAKE).unwrap();
    let mut spec = ServiceSpec {
        extension: "fake".to_owned(),
        config: ServiceConfig {
            command: vec![
                python().to_string_lossy().into_owned(),
                script.to_string_lossy().into_owned(),
            ],
            ready_timeout: Duration::from_secs(30),
            memory: 256 << 20,
            restarts: 5,
            timeouts: 2,
            accept_timeout: Duration::from_millis(500),
            settle: Duration::from_hours(24),
            secrets: Vec::new(),
            builtin_metrics: schedr_model::extension::BUILTIN_SERVICE_METRICS
                .map(str::to_owned)
                .to_vec(),
        },
        versions: BTreeSet::from(["1.0".to_owned(), "2.0".to_owned()]),
        env: BTreeMap::from([
            ("MODE".to_owned(), mode.to_owned()),
            ("PATH".to_owned(), std::env::var("PATH").unwrap_or_default()),
        ]),
        working_dir: dir.path().to_owned(),
        grace: Duration::from_secs(1),
        timings: ServiceTimings {
            backoff_first: Duration::from_secs(10),
            backoff_max: Duration::from_secs(10),
        },
        declarations: Declarations::default(),
        max_series: 10,
    };
    change(&mut spec);
    let secret = MemoryStorage::new([(
        schedr_model::secret::SecretName::new("svc", Some("token")),
        "TOPSECRET_VALUE_1".to_owned(),
    )]);
    let figures = Figures::default();
    let deps = ServiceDeps {
        secrets: Arc::new(SecretStores::for_test(secret)),
        figures: figures.clone(),
        proc_root: PathBuf::from("/proc"),
        memory_interval: Duration::from_millis(200),
    };
    let (stop, stopping) = watch::channel(false);
    let services = Services::start(vec![spec], &deps, &stopping);
    Fake {
        _dir: dir,
        services,
        figures,
        stop,
    }
}

impl Fake {
    fn state(&self) -> State {
        self.services.state("fake").unwrap()
    }

    async fn until(&self, what: &str, mut check: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !check(self) {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what}: {:?}",
                self.state()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Events of `kind` seen so far, kept across calls in `seen`.
fn events(figures: &Figures, seen: &mut Vec<ExtensionEvent>, kind: EventKind) -> Vec<String> {
    seen.extend(figures.take_events());
    seen.iter()
        .filter(|e| e.kind == kind)
        .map(|e| e.message.clone())
        .collect()
}

/// A worker stopping while its service starts stops it, not waiting for its ready.
#[tokio::test]
async fn a_service_starting_is_stopped_with_the_worker() {
    let fake = start("never_ready", |_| {});
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(fake.state(), State::Starting);
    fake.stop.send_replace(true);
    fake.until("the service stopped", |f| f.state() == State::Stopped)
        .await;
}

/// A service not reading its control socket is taken for crashed once a write past its bound
/// fails, not waited on for ever.
#[tokio::test]
async fn a_service_not_reading_its_socket_is_taken_for_crashed() {
    let fake = start("flood", |_| {});
    let mut seen = Vec::new();
    fake.until("the crash", |f| {
        !events(&f.figures, &mut seen, EventKind::ServiceCrashed).is_empty()
    })
    .await;
    let crashed = events(&fake.figures, &mut seen, EventKind::ServiceCrashed);
    assert!(crashed[0].contains("not read within"), "{crashed:?}");
}

/// What a service tells is masked with its secrets; its metric points are kept under its
/// configured declarations, no plugin needed.
#[tokio::test]
async fn a_services_status_and_labels_are_masked_and_its_points_kept() {
    let fake = start("tell", |spec| {
        spec.config.secrets = vec![schedr_model::secret::SecretName::new("svc", Some("token"))];
        spec.declarations = serde_json::from_value(serde_json::json!({
            "metrics": [{"name": "jobs", "kind": "counter", "help": "Jobs",
                         "labels": [{"name": "detail"}]}]
        }))
        .unwrap();
    });
    let mut seen = Vec::new();
    fake.until("its status", |f| {
        !events(&f.figures, &mut seen, EventKind::ServiceStatus).is_empty()
    })
    .await;
    let status = events(&fake.figures, &mut seen, EventKind::ServiceStatus);
    assert!(!status[0].contains("TOPSECRET_VALUE_1"), "{status:?}");
    let figures = serde_json::to_string(&fake.figures.take_figures()).unwrap();
    assert!(figures.contains("\"jobs\""), "kept: {figures}");
    assert!(!figures.contains("TOPSECRET_VALUE_1"), "masked: {figures}");
}

/// Timed-out attempts count per run, and a permit left from an earlier streak doesn't reset a
/// live count: two in a row after a success take it for hung.
#[tokio::test]
async fn timeouts_count_per_run_and_an_old_streak_resets_nothing() {
    let fake = start("slow_ready", |_| {});
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(fake.state(), State::Starting);
    // A streak reaching the limit while it starts (a permit left), then a success.
    fake.services.attempt_ended("fake", 1, true);
    fake.services.attempt_ended("fake", 1, true);
    fake.services.attempt_ended("fake", 1, false);
    fake.until("ready", |f| matches!(f.state(), State::Ready(_)))
        .await;
    // Another run's attempts count for nothing.
    fake.services.attempt_ended("fake", 7, true);
    fake.services.attempt_ended("fake", 7, true);
    fake.services.attempt_ended("fake", 1, true);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        matches!(fake.state(), State::Ready(_)),
        "{:?}",
        fake.state()
    );
    fake.services.attempt_ended("fake", 1, true);
    let mut seen = Vec::new();
    fake.until("taken for hung", |f| {
        !events(&f.figures, &mut seen, EventKind::ServiceCrashed).is_empty()
    })
    .await;
    let crashed = events(&fake.figures, &mut seen, EventKind::ServiceCrashed);
    assert!(crashed[0].contains("timed out in a row"), "{crashed:?}");
}

/// A service growing past its memory while it starts is killed then, not once ready.
#[tokio::test]
async fn a_service_past_its_memory_while_starting_is_killed() {
    let fake = start("grow_starting", |spec| spec.config.memory = 16 << 20);
    let mut seen = Vec::new();
    fake.until("the memory kill", |f| {
        !events(&f.figures, &mut seen, EventKind::ServiceMemoryKilled).is_empty()
    })
    .await;
}

/// The main path: an attempt connected and accepted, a maintenance drain and resume told, and
/// the worker's stop waiting for the attempt connected.
#[tokio::test]
async fn an_attempt_connects_the_drain_is_told_and_the_stop_waits_for_it() {
    // Its extension's metrics configured: its version declared, so its built-ins are kept.
    let fake = start("serve", |spec| {
        spec.declarations = serde_json::from_value(serde_json::json!({
            "metrics": [{"name": "jobs", "kind": "counter", "help": "Jobs"}]
        }))
        .unwrap();
    });
    fake.until("ready", |f| matches!(f.state(), State::Ready(_)))
        .await;
    assert_eq!(fake.services.not_ready("fake", "1.0"), None);
    assert!(fake.services.not_ready("fake", "9.9").is_some());
    let task = ServiceTask {
        name: "fake".to_owned(),
        version: "1.0".to_owned(),
    };
    let link = fake
        .services
        .connect("a-1", task.clone(), false)
        .await
        .unwrap();
    assert_eq!(link.run(), 1);
    // Its built-in metrics (M5.2i12): its accept time, timed by the worker, and its memory,
    // measured each memory check, beside its extension's own.
    let (mut accepted, mut memory) = (false, false);
    fake.until("its built-in metrics", |f| {
        for extension in f.figures.take_figures() {
            for metric in extension.metrics {
                let observed = metric
                    .series
                    .iter()
                    .any(|s| s.value > 0.0 || s.counts.iter().sum::<u64>() > 0);
                accepted |= metric.name == "service_accept_seconds" && observed;
                memory |= metric.name == "service_memory_bytes" && observed;
            }
        }
        accepted && memory
    })
    .await;
    let unserved = ServiceTask {
        version: "9.9".to_owned(),
        ..task
    };
    assert!(fake.services.connect("a-2", unserved, false).await.is_err());
    fake.services.maintenance_drained(true);
    let mut seen = Vec::new();
    fake.until("drained told", |f| {
        events(&f.figures, &mut seen, EventKind::ServiceStatus).len() == 1
    })
    .await;
    // Its statuses are kept a second apart.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    fake.services.maintenance_drained(false);
    fake.until("resumed told", |f| {
        events(&f.figures, &mut seen, EventKind::ServiceStatus).len() == 2
    })
    .await;
    // Stopping: it waits for the connected attempt to end.
    fake.stop.send_replace(true);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(fake.state(), State::Stopped);
    let (_fd, hold) = link.split();
    let stopping = fake.services.stopped(Duration::from_secs(10));
    drop(hold);
    stopping.await;
    let mut seen = Vec::new();
    fake.until("its stop", |f| {
        !events(&f.figures, &mut seen, EventKind::ServiceStopped).is_empty()
    })
    .await;
}

/// agy's review of M5.2i12: a service whose versions nothing declared yet (no config metrics,
/// no plugin run on this worker) declares no built-ins alone: they would be its versions'
/// newest declarations, hiding a plugin's from the page.
#[tokio::test]
async fn a_services_builtins_are_never_declared_alone() {
    let fake = start("serve", |_| {});
    fake.until("ready", |f| matches!(f.state(), State::Ready(_)))
        .await;
    // A few memory checks (each 200 ms).
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(fake.figures.take_figures(), Vec::new());
}
