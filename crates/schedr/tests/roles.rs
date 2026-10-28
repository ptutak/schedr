//! The roles as separate processes (PLAN.md 2.1): `schedr master`, `schedr webserver` and
//! `schedr worker`, each its own process, on Postgres and on one SQLite file, driven through
//! the CLI like any deployment.
//!
//! Needs `SCHEDR_TEST_PYTHON` (a Python with `schedr` installed) and
//! `SCHEDR_TEST_POSTGRES_URL`; `make test-integration` sets both.

mod support;

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use schedr_agent::journal::{Journal, JournalRecord};
use schedr_proto::pb;
use schedr_tls::testing::TestCa;
use serde_json::Value;
use support::{BINARY, Cluster, postgres_database};

#[test]
fn three_processes_on_postgres_run_a_dag() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    cluster.up();
    cluster.run_chain();
    // The log comes from the worker, through the webserver.
    let run_id = cluster
        .cli_json(&["runs", "list", "chain", "--for", "2026-10-15"])
        .unwrap()["runs"][0]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (code, out, err) = cluster.cli(&["tasks", "logs", &run_id, "report"]);
    assert_eq!(code, 0, "{err}\n{}", cluster.logs());
    assert_eq!(out, "report loaded 3\n");
}

#[test]
fn three_processes_on_one_sqlite_file_run_a_dag() {
    let mut cluster = Cluster::new(None, "100ms");
    cluster.up();
    cluster.run_chain();
    let master = std::fs::read_to_string(&cluster.roles[0].log).unwrap();
    assert!(
        master.contains("on SQLite: every role must run on this host"),
        "{master}"
    );
}

#[test]
fn on_postgres_a_command_is_applied_at_once_not_at_the_next_tick() {
    // With an hour between ticks, only the notification can make the master apply a command
    // while the CLI waits for it (up to 30 s).
    let mut cluster = Cluster::new(Some(postgres_database()), "1h");
    cluster.up();
    let started = Instant::now();
    let (code, out, err) = cluster.cli(&["dags", "unpause", "chain"]);
    assert_eq!(code, 0, "{out}{err}\n{}", cluster.logs());
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "{:?}",
        started.elapsed()
    );
}

#[test]
fn the_master_refuses_a_database_not_migrated_yet() {
    let cluster = Cluster::new(Some(postgres_database()), "100ms");
    let (code, err) = cluster.schedr(&["master"]);
    assert_eq!(code, 1, "{err}");
    assert!(
        err.contains("run `schedr db migrate`, or start master with `--migrate`"),
        "{err}"
    );
}

#[test]
fn a_webserver_started_before_the_migration_serves_once_the_master_has_migrated() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    cluster.start_unready("webserver", &["webserver"]);
    cluster.wait("the webserver to say it waits for the schema", || {
        cluster.log_of("webserver").contains("waiting for it")
    });
    let (status, body) = cluster.ops_get("webserver", "/readyz");
    assert_eq!(status, 503, "{body}");
    // The chart's `migration: onStart`: the master migrates, holding its lease.
    cluster.start_args("master", &["master", "--migrate"], &[]);
    cluster.wait("the webserver to be ready", || {
        cluster.ops_get("webserver", "/readyz").0 == 200
    });
    cluster.api = cluster
        .log_of("webserver")
        .split("API on http://")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .map(|address| format!("http://{}", address.trim_end_matches(',')))
        .expect("the webserver's API address");
    cluster.start("worker");
    cluster.wait("the DAG to be parsed", || {
        cluster
            .cli_json(&["dags", "list"])
            .is_some_and(|dags| dags.to_string().contains("\"chain\""))
    });
    cluster.run_chain();
}

#[test]
fn a_worker_needs_its_master_url() {
    let cluster = Cluster::new(None, "100ms");
    let output = Command::new(BINARY)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", cluster.dir.path())
        .arg("worker")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.contains("set [worker] master_url"), "{err}");
}

#[test]
fn a_role_stops_on_sigterm() {
    let mut cluster = Cluster::new(None, "100ms");
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    cluster.start("webserver");
    cluster.wait("the API to answer", || {
        cluster.cli_json(&["dags", "list"]).is_some()
    });
    let role = &mut cluster.roles[0];
    let pid = role.child.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = role.child.try_wait().unwrap() {
            assert!(status.success(), "{status}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the webserver didn't stop on SIGTERM"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A task that runs long on its first try (leaving a marker with its process ID) and returns
/// at once after.
fn slow_first_try(marker: &std::path::Path) -> String {
    format!(
        r#"
import os, time
from datetime import timedelta
from schedr import Dag, task

@task(retries=1, retry_delay=timedelta(0))
def work() -> str:
    marker = {marker:?}
    if not os.path.exists(marker):
        with open(marker, "w") as file:
            file.write(str(os.getpid()))
        time.sleep(15)
    return "done"

with Dag("slow"):
    work()
"#,
        marker = marker.display().to_string()
    )
}

fn slow_cluster() -> Cluster {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    let marker = cluster.dir.path().join("marker");
    std::fs::write(cluster.root().join("slow.py"), slow_first_try(&marker)).unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    cluster
}

impl Cluster {
    /// The process ID the first try of `slow` wrote, once it did.
    fn first_try_pid(&self) -> String {
        let marker = self.dir.path().join("marker");
        let mut pid = String::new();
        self.wait("the first try's process ID", || {
            pid = std::fs::read_to_string(&marker).unwrap_or_default();
            !pid.is_empty()
        });
        pid
    }

    /// The worker's journal.
    fn journal(&self) -> Journal {
        Journal::new(self.dir.path().join("data/journal/w1"))
    }
}

#[test]
fn a_restarted_worker_stops_the_task_it_left_running_whose_attempt_is_lost_and_retried() {
    let mut cluster = slow_cluster();
    let run_id = cluster.trigger("slow");
    cluster.wait("the first attempt to start", || {
        cluster
            .attempts(&run_id, "work")
            .first()
            .is_some_and(|a| !a["started_at"].is_null())
    });
    let pid = cluster.first_try_pid();
    cluster.kill("worker");
    assert!(
        PathBuf::from(format!("/proc/{pid}")).exists(),
        "the task outlives its agent"
    );
    cluster.start("worker");
    cluster.wait("the task the previous agent left to be stopped", || {
        !PathBuf::from(format!("/proc/{pid}")).exists()
    });
    cluster.wait("the retry to succeed", || {
        cluster
            .cli_json(&["runs", "list", "slow"])
            .is_some_and(|runs| runs["runs"][0]["state"] == "success")
    });
    let statuses: Vec<Value> = cluster
        .attempts(&run_id, "work")
        .iter()
        .map(|a| a["status"].clone())
        .collect();
    assert_eq!(statuses, ["lost", "success"], "{}", cluster.logs());
    let lost = &cluster.attempts(&run_id, "work")[0];
    assert_eq!(lost["error_summary"]["exception"], "WorkerRestarted");
    assert_eq!(cluster.worker_state("w1").as_deref(), Some("active"));
    assert_eq!(
        cluster.journal().records().unwrap(),
        [],
        "everything acknowledged"
    );
}

#[test]
fn a_result_the_master_didnt_acknowledge_survives_a_worker_restart() {
    let mut cluster = slow_cluster();
    let run_id = cluster.trigger("slow");
    let journal = cluster.journal();
    let mut recorded = Vec::new();
    cluster.wait("the first try's process in the journal", || {
        recorded = journal.records().unwrap_or_default();
        matches!(recorded.as_slice(), [(_, JournalRecord::Running(Some(_)))])
    });
    let [(attempt_id, JournalRecord::Running(Some(process)))] = recorded.as_slice() else {
        unreachable!()
    };
    assert_eq!(process.pgid.to_string(), cluster.first_try_pid());
    // The agent dies just after the task ended and its result was journalled, before the
    // master acknowledged it.
    cluster.kill("worker");
    let group = rustix::process::Pid::from_raw(process.pgid).unwrap();
    rustix::process::kill_process_group(group, rustix::process::Signal::KILL).unwrap();
    let finished = pb::Finished {
        attempt_id: attempt_id.clone(),
        status: pb::AttemptStatus::Success.into(),
        output_json: br#""done""#.to_vec(),
        ..pb::Finished::default()
    };
    journal
        .write(attempt_id, &JournalRecord::Finished(finished))
        .unwrap();
    cluster.start("worker");
    cluster.wait("the run to succeed", || {
        cluster
            .cli_json(&["runs", "list", "slow"])
            .is_some_and(|runs| runs["runs"][0]["state"] == "success")
    });
    let statuses: Vec<Value> = cluster
        .attempts(&run_id, "work")
        .iter()
        .map(|a| a["status"].clone())
        .collect();
    assert_eq!(statuses, ["success"], "not lost: {}", cluster.logs());
    cluster.wait("the result to be acknowledged", || {
        journal.records().is_ok_and(|records| records.is_empty())
    });
}

#[test]
fn a_worker_that_never_comes_back_is_lost_and_stays_listed() {
    let mut cluster = slow_cluster();
    let run_id = cluster.trigger("slow");
    cluster.wait("the first attempt to start", || {
        cluster
            .attempts(&run_id, "work")
            .first()
            .is_some_and(|a| !a["started_at"].is_null())
    });
    cluster.kill("worker");
    cluster.wait("the worker to be lost", || {
        cluster.worker_state("w1").as_deref() == Some("lost")
    });
    cluster.wait("its attempt to end lost", || {
        cluster
            .attempts(&run_id, "work")
            .first()
            .is_some_and(|a| a["status"] == "lost")
    });
    let (code, out, _) = cluster.cli(&["workers", "list"]);
    assert_eq!(code, 0);
    assert!(out.contains("w1") && out.contains("lost"), "{out}");
}

#[test]
fn a_running_task_survives_a_master_restart() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    std::fs::write(
        cluster.root().join("pause.py"),
        "import time\nfrom schedr import Dag, task\n\n@task\ndef pause() -> int:\n    time.sleep(4)\n    return 1\n\nwith Dag(\"pause\"):\n    pause()\n",
    )
    .unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    let run_id = cluster.trigger("pause");
    cluster.wait("the attempt to start", || {
        cluster
            .attempts(&run_id, "pause")
            .first()
            .is_some_and(|a| !a["started_at"].is_null())
    });
    cluster.kill("master");
    cluster.start("master");
    cluster.wait("the run to succeed after the restart", || {
        cluster
            .cli_json(&["runs", "list", "pause"])
            .is_some_and(|runs| runs["runs"][0]["state"] == "success")
    });
    let statuses: Vec<Value> = cluster
        .attempts(&run_id, "pause")
        .iter()
        .map(|a| a["status"].clone())
        .collect();
    assert_eq!(
        statuses,
        ["success"],
        "one attempt, continued: {}",
        cluster.logs()
    );
}

#[test]
fn a_second_worker_with_the_same_id_is_refused() {
    let mut cluster = Cluster::new(None, "100ms");
    cluster.up();
    let last_log = |cluster: &Cluster| {
        std::fs::read_to_string(&cluster.roles.last().unwrap().log).unwrap_or_default()
    };
    cluster.start("worker");
    cluster.wait("the second worker to be refused its journal", || {
        last_log(&cluster).contains("another schedr worker `w1` is using")
    });
    let elsewhere = cluster.dir.path().join("elsewhere");
    cluster.start_with(
        "worker",
        &[(
            "SCHEDR_CONFIG__WORKER__DATA_DIR",
            elsewhere.to_str().unwrap(),
        )],
    );
    cluster.wait("the third worker to be refused by the master", || {
        last_log(&cluster).contains("already has a live stream")
    });
    assert_eq!(cluster.worker_state("w1").as_deref(), Some("active"));
}

/// `small` (the default queue) → `big` (queue `large`) → `rare` (queue `gpu`), and `pause`,
/// which sleeps, → `after`.
const QUEUES: &str = r#"
import time
from schedr import Dag, task

@task
def small() -> int:
    return 1

@task(queue="large")
def big(n: int) -> int:
    return n + 1

@task(queue="gpu")
def rare(n: int) -> int:
    return n + 1

with Dag("queues"):
    rare(big(small()))

@task
def pause() -> int:
    time.sleep(60)
    return 1

@task
def after(n: int) -> int:
    return n

with Dag("slow"):
    after(pause())
"#;

#[test]
fn tasks_go_to_the_workers_serving_their_queue_and_wait_for_one() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    std::fs::write(cluster.root().join("queues.py"), QUEUES).unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    cluster.start_worker("worker-large", "w2", r#"["large"]"#);
    cluster.wait("both workers", || {
        cluster.worker_state("w2").as_deref() == Some("active")
    });
    let run_id = cluster.trigger("queues");
    cluster.wait("`big` to succeed and `rare` to wait", || {
        cluster.task(&run_id, "big")["state"] == "success"
            && cluster.task(&run_id, "rare")["reason"] == "no active worker serves queue `gpu`"
    });
    let worker_of = |task: &str| cluster.attempts(&run_id, task)[0]["worker_id"].clone();
    assert_eq!(worker_of("small"), "w1");
    assert_eq!(worker_of("big"), "w2");
    assert_eq!(cluster.task(&run_id, "rare")["state"], "queued");

    cluster.start_worker("worker-gpu", "w3", r#"["gpu"]"#);
    cluster.wait("the run to succeed", || {
        cluster.run_state(&run_id) == "success"
    });
    assert_eq!(
        cluster.attempts(&run_id, "rare")[0]["worker_id"],
        "w3",
        "{}",
        cluster.logs()
    );
}

#[test]
fn a_running_task_is_cancelled_with_what_follows_it() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    std::fs::write(cluster.root().join("queues.py"), QUEUES).unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    let run_id = cluster.trigger("slow");
    cluster.wait("`pause` to start", || {
        cluster
            .attempts(&run_id, "pause")
            .first()
            .is_some_and(|a| !a["started_at"].is_null())
    });
    let (code, out, err) = cluster.cli(&["tasks", "retry", &run_id, "pause"]);
    assert_ne!(code, 0);
    assert!(err.contains("is running, not unschedulable"), "{err}");
    let (code, out2, err) = cluster.cli(&["tasks", "cancel", &run_id, "pause"]);
    assert_eq!(code, 0, "{out}{err}");
    assert_eq!(
        out2,
        format!("cancelling task pause of run {run_id} (being stopped)\n")
    );
    cluster.wait("the run to end cancelled", || {
        cluster.run_state(&run_id) == "cancelled"
    });
    assert_eq!(cluster.task(&run_id, "pause")["state"], "cancelled");
    assert_eq!(cluster.task(&run_id, "after")["state"], "cancelled");
    assert_eq!(cluster.attempts(&run_id, "pause")[0]["status"], "cancelled");
    let (code, _, err) = cluster.cli(&["tasks", "cancel", &run_id, "after"]);
    assert_ne!(code, 0);
    assert!(err.contains("already finished (cancelled)"), "{err}");
}

/// DAG `moving` in `sync.py`, and in its newer code moved to `moved.py`.
const MOVING_OLD: &str = "from schedr import Dag, task\n\n@task\ndef step() -> int:\n    return 1\n\nwith Dag(\"moving\"):\n    step()\n";
const MOVING_NEW: &str = "from schedr import Dag, task\n\n@task\ndef step() -> int:\n    return 2\n\nwith Dag(\"moving\"):\n    step()\n";

impl Cluster {
    fn dag_view(&self, dag: &str) -> Value {
        self.cli_json(&["dags", "list"])
            .and_then(|dags| {
                dags["dags"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|d| d["dag_id"] == dag)
                    .cloned()
            })
            .unwrap_or(Value::Null)
    }
}

#[test]
fn the_newest_code_runs_and_a_worker_on_older_code_blocks_nothing() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    std::fs::write(cluster.root().join("sync.py"), MOVING_OLD).unwrap();
    let newer = cluster.dir.path().join("dags-newer/etl");
    std::fs::create_dir_all(&newer).unwrap();
    std::fs::write(newer.join("moved.py"), MOVING_NEW).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    cluster.wait("DAG `moving` in sync.py", || {
        cluster.dag_view("moving")["location"]["path"] == "sync.py"
    });

    // w2 has newer code, where the DAG moved to another file: its code is used, no conflict.
    let newer_path = newer.to_str().unwrap().to_owned();
    cluster.start_as(
        "worker-newer",
        "worker",
        &[
            ("SCHEDR_CONFIG__WORKER__ID", "w2"),
            ("SCHEDR_DAG_PATH", &newer_path),
        ],
    );
    cluster.wait("DAG `moving` in moved.py", || {
        cluster.dag_view("moving")["location"]["path"] == "moved.py"
    });
    assert!(
        cluster.dag_view("moving")["conflict"].is_null(),
        "{}",
        cluster.logs()
    );
    let master_log = std::fs::read_to_string(&cluster.roles[0].log).unwrap();
    assert!(
        master_log.contains("workers report 2 code versions of the root"),
        "{master_log}"
    );

    // Only w2 has the newest code: the task runs there.
    let run_id = cluster.trigger("moving");
    cluster.wait("the run to succeed", || {
        cluster.run_state(&run_id) == "success"
    });
    assert_eq!(cluster.attempts(&run_id, "step")[0]["worker_id"], "w2");
    let run = cluster.api_get(&format!("/api/v1/runs/{run_id}")).unwrap();
    assert_eq!(run["code_hashes"].as_array().unwrap().len(), 1, "{run}");
}

/// A task that runs long on its first try (leaving a marker) and returns at once after, with
/// no retries: only a run again without using a retry lets the run succeed.
fn interrupted_first_try(marker: &std::path::Path) -> String {
    format!(
        r#"
import os, time
from schedr import Dag, task

@task
def work() -> str:
    marker = {marker:?}
    if not os.path.exists(marker):
        open(marker, "w").close()
        time.sleep(60)
    return "done"

with Dag("drained"):
    work()
"#,
        marker = marker.display().to_string()
    )
}

#[test]
fn a_worker_stopped_with_sigterm_drains_and_its_interrupted_task_runs_again_without_a_retry() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    let marker = cluster.dir.path().join("marker");
    std::fs::write(
        cluster.root().join("drained.py"),
        interrupted_first_try(&marker),
    )
    .unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    // A 2 s grace period of which 1 s to SIGKILL and 1 s to report: interrupted at once.
    cluster.start_as(
        "worker-2",
        "worker",
        &[
            ("SCHEDR_CONFIG__WORKER__ID", "w2"),
            ("SCHEDR_CONFIG__WORKER__TERMINATION_GRACE_PERIOD", "2s"),
            ("SCHEDR_CONFIG__WORKER__DRAIN_TERMINATE_PERIOD", "1s"),
            ("SCHEDR_CONFIG__WORKER__DRAIN_REPORT_PERIOD", "1s"),
        ],
    );
    cluster.wait("both workers", || {
        cluster.worker_state("w2").as_deref() == Some("active")
    });
    let run_id = cluster.trigger("drained");
    cluster.wait("the first try to start", || {
        cluster
            .attempts(&run_id, "work")
            .first()
            .is_some_and(|a| !a["started_at"].is_null())
    });
    let first = cluster.attempts(&run_id, "work")[0]["worker_id"]
        .as_str()
        .unwrap()
        .to_owned();
    // The worker running it is stopped; w1 has the default 30 s grace, w2 2 s.
    let (role, other) = if first == "w2" {
        ("worker-2", "w1")
    } else {
        ("worker", "w2")
    };
    let status = cluster.terminate(role);
    assert!(status.success(), "{status}\n{}", cluster.logs());
    cluster.wait("the run to succeed", || {
        cluster.run_state(&run_id) == "success"
    });
    let attempts = cluster.attempts(&run_id, "work");
    let summary: Vec<(Value, Value)> = attempts
        .iter()
        .map(|a| (a["status"].clone(), a["worker_id"].clone()))
        .collect();
    assert_eq!(
        summary,
        [
            (Value::from("interrupted"), Value::from(first.as_str())),
            (Value::from("success"), Value::from(other)),
        ],
        "{}",
        cluster.logs()
    );
    assert_eq!(cluster.worker_state(&first).as_deref(), Some("offline"));
}

#[test]
fn a_drained_worker_gets_no_tasks_until_resumed_even_across_a_master_restart() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    std::fs::write(cluster.root().join("pause.py"), "from schedr import Dag, task\n\n@task\ndef pause() -> int:\n    return 1\n\nwith Dag(\"pause\"):\n    pause()\n").unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    cluster.start_as("worker-2", "worker", &[("SCHEDR_CONFIG__WORKER__ID", "w2")]);
    cluster.wait("both workers", || {
        cluster.worker_state("w2").as_deref() == Some("active")
    });
    let (code, out, err) = cluster.cli(&["workers", "drain", "w1"]);
    assert_eq!(code, 0, "{err}");
    assert!(out.starts_with("draining w1: no new tasks;"), "{out}");
    let (code, _, err) = cluster.cli(&["workers", "drain", "w9"]);
    assert_ne!(code, 0);
    assert!(err.contains("no worker `w9`"), "{err}");
    cluster.kill("master");
    cluster.start("master");
    cluster.wait("w1 to be back, drained", || {
        cluster.cli_json(&["workers", "list"]).is_some_and(|list| {
            list["workers"][0]["state"] == "active" && list["workers"][0]["drained"] == true
        })
    });
    for _ in 0..3 {
        let run_id = cluster.trigger("pause");
        cluster.wait("the run to succeed", || {
            cluster.run_state(&run_id) == "success"
        });
        assert_eq!(cluster.attempts(&run_id, "pause")[0]["worker_id"], "w2");
    }
    let (code, out, err) = cluster.cli(&["workers", "resume", "w1"]);
    assert_eq!((code, out.as_str()), (0, "resumed w1\n"), "{err}");
    let (code, _, err) = cluster.cli(&["workers", "resume", "w1"]);
    assert_ne!(code, 0);
    assert!(err.contains("worker `w1` isn't drained"), "{err}");
}

impl Cluster {
    /// Over mTLS: master, webserver and workers `w1` and `w2` (its data in `w2/`), with
    /// `chain` run for 2026-10-15: its run ID.
    fn mtls_chain_on_two_workers(&mut self) -> String {
        let (code, err) = self.schedr(&["db", "migrate"]);
        assert_eq!(code, 0, "{err}");
        self.start_tls("master", "master", &["127.0.0.1"], None, &[]);
        self.start_tls("webserver", "webserver", &["schedr-webserver"], None, &[]);
        self.start_tls("worker", "worker", &["schedr-worker"], None, &[]);
        let elsewhere = self.dir.path().join("w2");
        let elsewhere = elsewhere.to_str().unwrap();
        self.start_tls(
            "worker2",
            "worker",
            &["schedr-worker"],
            None,
            &[
                ("SCHEDR_CONFIG__WORKER__ID", "w2"),
                ("SCHEDR_CONFIG__WORKER__DATA_DIR", elsewhere),
            ],
        );
        assert!(self.log_of("master").contains("(mTLS)"), "{}", self.logs());
        self.wait("both workers", || {
            self.worker_state("w1").as_deref() == Some("active")
                && self.worker_state("w2").as_deref() == Some("active")
        });
        let workers = self.cli_json(&["workers", "list"]).unwrap();
        for worker in workers["workers"].as_array().unwrap() {
            let endpoint = worker["log_endpoint"].as_str().unwrap();
            assert!(endpoint.starts_with("https://127.0.0.1:"), "{endpoint}");
        }
        self.wait("DAG `chain`", || {
            self.cli_json(&["dags", "list"])
                .is_some_and(|d| d["dags"][0]["dag_id"] == "chain")
        });
        let (code, _, err) = self.cli(&["dags", "unpause", "chain"]);
        assert_eq!(code, 0, "{err}");
        let (code, _, err) = self.cli(&["runs", "trigger", "chain", "--for", "2026-10-15"]);
        assert_eq!(code, 0, "{err}");
        self.wait("the run to succeed", || {
            self.cli_json(&["runs", "list", "chain", "--for", "2026-10-15"])
                .is_some_and(|runs| runs["runs"][0]["state"] == "success")
        });
        self.chain_run()
    }
}

#[test]
fn over_mtls_two_workers_run_a_dag_and_their_logs_come_through_the_webserver() {
    let mut cluster = Cluster::with_tls(Some(postgres_database()));
    let run_id = cluster.mtls_chain_on_two_workers();
    let elsewhere = cluster.dir.path().join("w2");

    // Every task's log, from whichever worker ran it, is the file on that worker.
    for task in ["extract", "load", "report"] {
        let attempt = cluster.attempts(&run_id, task)[0].clone();
        let worker = attempt["worker_id"].as_str().unwrap();
        let data = if worker == "w1" {
            cluster.dir.path().join("data")
        } else {
            elsewhere.clone()
        };
        let file = data.join(format!(
            "logs/{worker}/{}.log",
            attempt["attempt_id"].as_str().unwrap()
        ));
        let (code, out, err) = cluster.cli(&["tasks", "logs", &run_id, task]);
        assert_eq!(code, 0, "{err}\n{}", cluster.logs());
        assert_eq!(
            out,
            std::fs::read_to_string(&file).unwrap(),
            "{task} on {worker}"
        );
    }

    // A deleted log, then a worker gone, say so.
    let attempt = cluster.attempts(&run_id, "report")[0].clone();
    let worker = attempt["worker_id"].as_str().unwrap().to_owned();
    let data = if worker == "w1" {
        cluster.dir.path().join("data")
    } else {
        elsewhere
    };
    std::fs::remove_file(data.join(format!(
        "logs/{worker}/{}.log",
        attempt["attempt_id"].as_str().unwrap()
    )))
    .unwrap();
    let (code, _, err) = cluster.cli(&["tasks", "logs", &run_id, "report"]);
    assert_eq!(code, 1);
    assert!(
        err.contains(&format!(
            "the log file is unavailable at worker `{worker}` (it may have been deleted)"
        )),
        "{err}"
    );
    let name = if worker == "w1" { "worker" } else { "worker2" };
    let role = cluster.roles.iter().find(|r| r.name == name).unwrap();
    assert!(
        Command::new("kill")
            .args(["-TERM", &role.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    cluster.wait("the worker to go offline", || {
        cluster.worker_state(&worker).as_deref() == Some("offline")
    });
    let (code, _, err) = cluster.cli(&["tasks", "logs", &run_id, "report"]);
    assert_eq!(code, 1);
    assert!(
        err.contains(&format!(
            "worker `{worker}` is offline: the log of try 1 is unavailable"
        )),
        "{err}"
    );
}

#[test]
fn a_worker_with_a_certificate_from_another_ca_is_refused() {
    let mut cluster = Cluster::with_tls(Some(postgres_database()));
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    cluster.start_tls("master", "master", &["127.0.0.1"], None, &[]);
    cluster.start_tls("webserver", "webserver", &["schedr-webserver"], None, &[]);
    // Its certificate comes from another CA; it trusts the cluster's.
    let files = TestCa::new("someone else").write(
        &cluster.dir.path().join("tls"),
        "foreign",
        &["schedr-worker"],
    );
    std::fs::write(&files.ca, cluster.ca.as_ref().unwrap().pem()).unwrap();
    let (ca, cert, key) = (
        files.ca.display().to_string(),
        files.cert.display().to_string(),
        files.key.display().to_string(),
    );
    cluster.start_as(
        "worker",
        "worker",
        &[
            ("SCHEDR_CONFIG__TLS__CA", &ca),
            ("SCHEDR_CONFIG__TLS__CERT", &cert),
            ("SCHEDR_CONFIG__TLS__KEY", &key),
        ],
    );
    cluster.wait("the master to refuse the worker's certificate", || {
        let master = cluster.log_of("master");
        master.contains("TLS handshake refused") && master.contains("UnknownIssuer")
    });
    cluster.wait("the worker to report why", || {
        cluster.last_log().contains("master connection lost")
    });
    assert_eq!(cluster.worker_state("w1"), None);
}

#[test]
fn the_master_takes_rotated_certificates_without_a_restart() {
    let mut cluster = Cluster::with_tls(Some(postgres_database()));
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    // The master starts on the old CA's certificate and trusts both CAs' workers.
    let old = TestCa::new("old CA");
    let master = cluster.start_tls("master", "master", &["127.0.0.1"], Some(&old), &[]);
    let new = cluster.ca.take().unwrap();
    std::fs::write(&master.ca, format!("{}{}", old.pem(), new.pem())).unwrap();
    cluster.ca = Some(new);
    cluster.start_tls("webserver", "webserver", &["schedr-webserver"], None, &[]);
    // The worker knows only the new CA, so it can't trust the master yet.
    cluster.start_tls("worker", "worker", &["schedr-worker"], None, &[]);
    cluster.wait("the worker to fail to connect", || {
        cluster.last_log().contains("master connection lost")
    });
    assert_eq!(cluster.worker_state("w1"), None);

    let issued = cluster.ca.as_ref().unwrap().issue(&["127.0.0.1"]);
    std::fs::write(&master.key, issued.key).unwrap();
    std::fs::write(&master.cert, issued.cert).unwrap();
    cluster.wait("the worker to register", || {
        cluster.worker_state("w1").as_deref() == Some("active")
    });
    assert!(
        cluster
            .log_of("master")
            .contains("TLS certificates changed"),
        "{}",
        cluster.logs()
    );
}

/// A task that allocates (and touches) 400 MiB on its first try, then finishes small.
fn greedy_first_try(marker: &std::path::Path) -> String {
    format!(
        r#"
import os, time
from datetime import timedelta
from schedr import Dag, task

@task(retries=1, retry_delay=timedelta(0))
def greedy() -> str:
    marker = {marker:?}
    if not os.path.exists(marker):
        open(marker, "w").close()
        chunks = []
        for _ in range(40):
            chunks.append(b"x" * (10 << 20))
            time.sleep(0.05)
        time.sleep(30)
    return "small"

with Dag("greedy"):
    greedy()
"#,
        marker = marker.display().to_string()
    )
}

#[test]
fn a_task_over_its_memory_budget_is_killed_alone_and_retried() {
    let mut cluster = Cluster::new(None, "100ms");
    let marker = cluster.dir.path().join("marker");
    std::fs::write(cluster.root().join("greedy.py"), greedy_first_try(&marker)).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    cluster.start("master");
    cluster.start("webserver");
    cluster.start_with(
        "worker",
        &[
            ("SCHEDR_CONFIG__WORKER__MEMORY_BUDGET", "100MiB"),
            ("SCHEDR_CONFIG__WORKER__MEMORY_CHECK_INTERVAL", "100ms"),
        ],
    );
    cluster.wait("the worker to say its budget", || {
        cluster
            .log_of("worker")
            .contains("memory budget 100 MiB per task")
    });
    let run_id = cluster.trigger("greedy");
    cluster.wait("the run to succeed", || {
        cluster.run_state(&run_id) == "success"
    });
    let attempts = cluster.attempts(&run_id, "greedy");
    assert_eq!(attempts.len(), 2, "{attempts:?}");
    assert_eq!(attempts[0]["status"], "oom_killed");
    let message = attempts[0]["error_summary"]["message"].as_str().unwrap();
    assert!(
        message.starts_with("exceeded memory budget 100 MiB ("),
        "{message}"
    );
    assert!(attempts[0]["peak_memory_bytes"].as_u64().unwrap() > 100 << 20);
    assert_eq!(attempts[1]["status"], "success");
    assert_eq!(attempts[1]["start_method"], "spawn");
    // Measured, well under the budget.
    let peak = attempts[1]["peak_memory_bytes"].as_u64().unwrap();
    assert!(peak > 0 && peak < 100 << 20, "{peak}");
    // The worker itself is still there: only the task was killed.
    assert_eq!(cluster.worker_state("w1").as_deref(), Some("active"));
}

#[test]
fn every_roles_ops_port_answers_and_counts_a_run() {
    let mut cluster = Cluster::new(None, "100ms");
    cluster.up();
    cluster.run_chain();
    for role in ["master", "webserver", "worker"] {
        assert_eq!(
            cluster.ops_get(role, "/healthz"),
            (200, "ok\n".to_owned()),
            "{role}"
        );
        cluster.wait(&format!("the {role} to be ready"), || {
            cluster.ops_get(role, "/readyz") == (200, "ok\n".to_owned())
        });
        let (status, metrics) = cluster.ops_get(role, "/metrics");
        assert_eq!(status, 200, "{role}");
        assert!(
            metrics.contains("schedr_build_info{version="),
            "{role}\n{metrics}"
        );
        assert!(
            metrics.contains("process_resident_memory_bytes "),
            "{role}\n{metrics}"
        );
    }
    let (_, master) = cluster.ops_get("master", "/metrics");
    for line in [
        "schedr_runs_finished_total{dag_id=\"chain\",result=\"success\"} 1",
        "schedr_attempts_ended_total{status=\"success\"} 3",
        "schedr_assignments_total 3",
        "schedr_commands_total{kind=\"trigger_run\",decision=\"applied\"} 1",
    ] {
        assert!(master.contains(line), "{line}\n{master}");
    }
    cluster.wait("the gauges to be refreshed", || {
        cluster
            .ops_get("master", "/metrics")
            .1
            .contains("schedr_workers{state=\"active\"} 1")
    });
    let (_, worker) = cluster.ops_get("worker", "/metrics");
    for line in [
        "schedr_worker_attempts_total{status=\"success\"} 3",
        "schedr_worker_slots 4",
    ] {
        assert!(worker.contains(line), "{line}\n{worker}");
    }
    let (_, webserver) = cluster.ops_get("webserver", "/metrics");
    assert!(
        webserver.contains("schedr_http_requests_total{route=\"/api/v1/dags\",status=\"200\"}"),
        "{webserver}"
    );
}

#[test]
fn a_master_waiting_for_its_database_is_alive_but_not_ready() {
    // A "database" that accepts connections and never answers.
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = silent.local_addr().unwrap().port();
    let mut cluster = Cluster::new(
        Some(format!("postgres://schedr@127.0.0.1:{port}/schedr")),
        "100ms",
    );
    // Started under another name: only its ops address is waited for (its gRPC address
    // isn't logged before the database answers).
    cluster.start_as(
        "waiting-master",
        "master",
        &[
            ("SCHEDR_CONFIG__DATABASE__QUERY_TIMEOUT", "20s"),
            ("SCHEDR_CONFIG__MASTER__LEASE_PERIOD", "60s"),
        ],
    );
    assert_eq!(
        cluster.ops_get("waiting-master", "/healthz"),
        (200, "ok\n".to_owned())
    );
    assert_eq!(
        cluster.ops_get("waiting-master", "/readyz"),
        (503, "starting: master\n".to_owned())
    );
    drop(silent);
}

/// A DAG whose task ignores SIGTERM on its first try (it stops only for SIGKILL), noting each
/// one in `<marker>.term`, and succeeds on the next.
fn ignores_sigterm_first_try(marker: &std::path::Path) -> String {
    format!(
        r#"
import os, signal, time
from schedr import Dag, task

@task
def work() -> str:
    marker = {marker:?}
    if not os.path.exists(marker):
        # The handler first: `marker` says the task now ignores SIGTERM.
        signal.signal(signal.SIGTERM, lambda *_: open(marker + ".term", "w").close())
        open(marker, "w").close()
        time.sleep(120)
    return "done"

with Dag("stubborn"):
    work()
"#,
        marker = marker.display().to_string()
    )
}

#[test]
fn a_drained_task_that_ignores_sigterm_is_interrupted_within_the_pods_grace_period() {
    // B8: the drain's SIGTERM came at the grace period minus 5 s, but SIGKILL only after the
    // master's cancel grace (30 s), long after the pod is killed; the task then ended `lost`
    // (using a retry) instead of `interrupted`.
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    let marker = cluster.dir.path().join("marker");
    std::fs::write(
        cluster.root().join("stubborn.py"),
        ignores_sigterm_first_try(&marker),
    )
    .unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    cluster.start("master");
    cluster.start("webserver");
    // The pod's terminationGracePeriodSeconds, matched in the worker's configuration.
    let grace = Duration::from_secs(8);
    let worker_env = [("SCHEDR_CONFIG__WORKER__TERMINATION_GRACE_PERIOD", "8s")];
    cluster.start_with("worker", &worker_env);
    let run_id = cluster.trigger("stubborn");
    cluster.wait("the first try to start", || {
        cluster
            .attempts(&run_id, "work")
            .first()
            .is_some_and(|a| !a["started_at"].is_null())
    });
    // Kubernetes: SIGTERM, then SIGKILL when the grace period is over.
    let exited = cluster.terminate_within("worker", grace);
    // SIGKILL if still running (the kubelet), and gone from the cluster's roles either way.
    cluster.kill("worker");
    // The pod comes back (a new incarnation, the journal on its volume).
    cluster.start_as("worker-again", "worker", &worker_env);
    cluster.wait("the run to end", || {
        ["success", "failed", "cancelled"].contains(&cluster.run_state(&run_id).as_str())
    });
    let statuses: Vec<Value> = cluster
        .attempts(&run_id, "work")
        .iter()
        .map(|a| a["status"].clone())
        .collect();
    assert_eq!(statuses, ["interrupted", "success"], "{}", cluster.logs());
    assert!(
        exited.is_some_and(|s| s.success()),
        "the worker didn't stop within the grace period: {exited:?}\n{}",
        cluster.logs()
    );
}

#[test]
fn a_draining_worker_stops_a_task_already_being_cancelled_within_the_grace_period() {
    // B9: a task already being stopped (here cancelled, so on the master's 30 s cancel grace)
    // when the drain's time is up kept that grace: the worker outlived the pod's deadline and
    // never deregistered.
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    let marker = cluster.dir.path().join("marker");
    std::fs::write(
        cluster.root().join("stubborn.py"),
        ignores_sigterm_first_try(&marker),
    )
    .unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    cluster.start("master");
    cluster.start("webserver");
    let grace = Duration::from_secs(8);
    cluster.start_with(
        "worker",
        &[("SCHEDR_CONFIG__WORKER__TERMINATION_GRACE_PERIOD", "8s")],
    );
    let run_id = cluster.trigger("stubborn");
    cluster.wait("the first try to start", || {
        cluster
            .attempts(&run_id, "work")
            .first()
            .is_some_and(|a| !a["started_at"].is_null())
    });
    // The task is cancelled once it ignores SIGTERM (`started` comes before the runner reaches
    // the task's code), then the pod is asked to stop once the cancel's SIGTERM has reached
    // it, so the cancel's grace is running.
    cluster.wait("the task to ignore SIGTERM", || marker.exists());
    let (code, out, err) = cluster.cli(&["tasks", "cancel", &run_id, "work"]);
    assert_eq!(code, 0, "{out}{err}");
    let terminated = cluster.dir.path().join("marker.term");
    cluster.wait("the cancel's SIGTERM to reach the task", || {
        terminated.exists()
    });
    let exited = cluster.terminate_within("worker", grace);
    assert!(
        exited.is_some_and(|s| s.success()),
        "the worker didn't stop within the grace period: {exited:?}\n{}",
        cluster.logs()
    );
    assert_eq!(cluster.worker_state("w1").as_deref(), Some("offline"));
    let statuses: Vec<Value> = cluster
        .attempts(&run_id, "work")
        .iter()
        .map(|a| a["status"].clone())
        .collect();
    assert_eq!(statuses, ["cancelled"], "{}", cluster.logs());
}

#[test]
fn a_second_master_waits_as_a_standby_and_takes_over_when_the_first_dies() {
    // One master per database (2.10): the lease, renewed by one statement, works through any
    // pooler; a second master waits, and takes over once the first's lease expires.
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    let lease = [
        ("SCHEDR_CONFIG__MASTER__LEASE_PERIOD", "3s"),
        ("SCHEDR_CONFIG__DATABASE__QUERY_TIMEOUT", "1s"),
    ];
    cluster.start_with("master", &lease);
    cluster.start("webserver");
    cluster.wait("the first master to lead", || {
        cluster.ops_get("master", "/readyz") == (200, "ok\n".to_owned())
    });
    // A command is decided: some master leads.
    let decided = |cluster: &Cluster| {
        let (code, _, err) = cluster.cli(&["workers", "drain", "ghost"]);
        code != 0 && err.contains("rejected")
    };
    assert!(decided(&cluster), "{}", cluster.logs());
    // Its own port (the cluster gives a master the first one's, for restarts).
    cluster.start_as(
        "master-2",
        "master",
        &[
            lease[0],
            lease[1],
            ("SCHEDR_CONFIG__MASTER__GRPC_PORT", "0"),
        ],
    );
    let standby_log = cluster.dir.path().join("master-2.log");
    cluster.wait("the second master to wait as a standby", || {
        std::fs::read_to_string(&standby_log).is_ok_and(|log| log.contains("waiting as a standby"))
    });
    assert_eq!(
        cluster.ops_get("master-2", "/readyz"),
        (503, "starting: master\n".to_owned())
    );
    // The first dies without giving its lease up: the second takes over once it expires.
    cluster.kill("master");
    cluster.wait("the second master to lead", || {
        cluster.ops_get("master-2", "/readyz") == (200, "ok\n".to_owned())
    });
    assert!(decided(&cluster), "{}", cluster.logs());
    let (_, metrics) = cluster.ops_get("master-2", "/metrics");
    assert!(metrics.contains("schedr_master_leading 1"), "{metrics}");
}

/// `curl` of the UI at `ui`: `POST` when `form` is given (an action from its own page, as
/// `x-forwarded-user` names the person); the body.
fn ui_call(ui: &str, path: &str, form: Option<&str>) -> String {
    let mut command = Command::new("curl");
    command.args([
        "-s",
        "-H",
        "x-forwarded-user: anna",
        "-H",
        "sec-fetch-site: same-origin",
    ]);
    if let Some(form) = form {
        command.args(["-X", "POST", "--data", form]);
    }
    let output = command.arg(format!("{ui}{path}")).output().unwrap();
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The command ID in an action's fragment (its `data-command`, there whether the master has
/// decided yet or not).
fn command_id(fragment: &str) -> String {
    fragment
        .split("data-command=\"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .unwrap_or_else(|| panic!("no command in {fragment}"))
        .to_owned()
}

#[test]
fn one_ui_shows_a_federation_of_standalones_and_a_distributed_deployment() {
    // Standalones on SQLite and on Postgres: their APIs only, trusting the UI's token.
    let token = "federation-token-0123456789";
    let standalone = |database: Option<String>| {
        let mut cluster = Cluster::new(database, "100ms");
        let file = cluster.root().parent().unwrap().join("ui-token");
        std::fs::write(&file, token).unwrap();
        cluster.start_args(
            "standalone",
            &["standalone", "--migrate", "--no-ui"],
            &[(
                "SCHEDR_CONFIG__WEBSERVER__UI_TOKEN_FILE",
                file.to_str().unwrap(),
            )],
        );
        cluster
    };
    let lite = standalone(None);
    let pg = standalone(Some(postgres_database()));
    // A distributed one, whose webserver wasn't given the token.
    let mut distributed = Cluster::new(Some(postgres_database()), "100ms");
    distributed.up();
    // The UI over the three, and a fourth that is down; the person named by a header.
    let dir = tempfile::TempDir::new().unwrap();
    let token_file = dir.path().join("token");
    std::fs::write(&token_file, token).unwrap();
    let deployment = |name: &str, url: &str| {
        format!(
            "[[ui.deployments]]\nname = \"{name}\"\nurl = \"{url}\"\ntoken_file = \"{}\"\n\n",
            token_file.display()
        )
    };
    let config = format!(
        "[ui]\nbind = \"127.0.0.1\"\nport = 0\ndocs_dir = \"\"\n\n\
         [ui.auth]\nidentity_header = \"x-forwarded-user\"\n\n\
         [ops]\nbind = \"127.0.0.1\"\nport = 0\n\n{}{}{}{}",
        deployment("lite", &lite.api),
        deployment("pg", &pg.api),
        deployment("dist", &distributed.api),
        deployment("gone", "http://127.0.0.1:9"),
    );
    std::fs::write(dir.path().join("config.toml"), config).unwrap();
    let log = dir.path().join("ui.log");
    let mut ui_process = Command::new(BINARY)
        .arg("--config")
        .arg(dir.path().join("config.toml"))
        .arg("ui")
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .stdout(std::fs::File::create(&log).unwrap())
        .stderr(std::fs::File::options().append(true).open(&log).unwrap())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let ui = loop {
        let text = std::fs::read_to_string(&log).unwrap_or_default();
        if let Some(address) = text
            .split("schedr ui: on http://")
            .nth(1)
            .and_then(|rest| rest.split_whitespace().next())
        {
            break format!("http://{address}");
        }
        assert!(Instant::now() < deadline, "no banner: {text}");
        std::thread::sleep(Duration::from_millis(100));
    };
    // Every deployment's DAG in one list (with a Deployment column), the one down named.
    let deadline = Instant::now() + Duration::from_secs(60);
    let list = loop {
        let page = ui_call(&ui, "/", None);
        let shown = ["lite", "pg", "dist"]
            .iter()
            .all(|d| page.contains(&format!("href=\"/d/{d}/dags/chain\"")));
        if shown {
            break page;
        }
        assert!(
            Instant::now() < deadline,
            "not every deployment's DAG: {page}"
        );
        std::thread::sleep(Duration::from_millis(200));
    };
    assert!(list.contains("Unavailable: gone:"), "{list}");
    assert!(list.contains("<th>Deployment</th>"), "{list}");
    // An action goes to its deployment. A standalone trusts the UI's token: the person's
    // name is recorded; the distributed webserver doesn't: anonymous.
    let paused = ui_call(&ui, "/d/lite/dags/chain/pause", Some(""));
    let command = lite.api_get(&format!("/api/v1/commands/{}", command_id(&paused)));
    assert_eq!(command.unwrap()["requested_by"], "anna", "{paused}");
    let paused = ui_call(&ui, "/d/dist/dags/chain/pause", Some(""));
    let command = distributed.api_get(&format!("/api/v1/commands/{}", command_id(&paused)));
    assert_eq!(command.unwrap()["requested_by"], "anonymous", "{paused}");
    ui_process.kill().unwrap();
    ui_process.wait().unwrap();
    drop((lite, pg, distributed));
}
