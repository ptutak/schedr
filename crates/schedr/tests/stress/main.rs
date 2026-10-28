//! The stress suite (PLAN.md 2.10b; see PROJECT.md, stress suite, and the guide's "Stress
//! tests"): the real binary under load and trouble, each scenario ending with the same
//! invariants and a report in `target/stress/`. Not part of `make check` or the CI run on each
//! push: `make stress` locally, nightly in CI.
//!
//! Needs what the integration tests need (`SCHEDR_TEST_PYTHON`, `SCHEDR_TEST_POSTGRES_URL`) and
//! `SCHEDR_TEST_PGBOUNCER` (`host:port` of a `PgBouncer` in transaction pooling in front of the
//! same server, any database name passed through); `make stress` sets them. The size:
//! `SCHEDR_STRESS_PROFILE` (`quick` or `full`, see [`load::Size`]).

#[path = "../support/mod.rs"]
mod support;

mod chaos;
mod check;
mod load;

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chaos::{Proxy, follow_ready_master, pid_of, pod_death, signal};
use check::{Report, overlapping_tries, quantile};
use load::{Lcg, Stress, cli_many, command_dags};
use support::postgres_database;

const MINUTE: Duration = Duration::from_secs(60);

/// Ends a scenario: the invariants (runs in `final_states`), tries that overlapped by the
/// tasks' marks, and the report.
fn finish(stress: &Stress, mut report: Report, final_states: &[&str]) {
    report.broken(stress.db.invariants(final_states));
    report.broken(overlapping_tries(&stress.db, &stress.marks, &stress.killed));
    report.diagnose(stress.db.diagnosis());
    report.finish(|| stress.cluster.logs());
}

/// The test Postgres server's database `url` reached through `SCHEDR_TEST_PGBOUNCER` instead.
fn through_pgbouncer(url: &str) -> String {
    let bouncer = std::env::var("SCHEDR_TEST_PGBOUNCER").expect(
        "SCHEDR_TEST_PGBOUNCER: `host:port` of a `PgBouncer` in transaction pooling (`make stress` \
         starts one)",
    );
    let (credentials, rest) = url.rsplit_once('@').unwrap();
    let (_, database) = rest.split_once('/').unwrap();
    format!("{credentials}@{bouncer}/{database}")
}

/// Scenario 1: Throughput: many DAGs of short tasks, and one wide DAG, on a few workers.
fn throughput(database_url: String, scenario: &'static str, pooled: bool) {
    let mut stress = Stress::new(Some(database_url));
    let size = stress.size;
    let mut report = Report::new(scenario);
    let mut dags = command_dags(
        &stress,
        "many",
        size.n(50, 1000),
        size.n(10, 15),
        "sleep 0.1",
        0,
    );
    dags.extend(command_dags(
        &stress,
        "wide",
        1,
        size.n(200, 5000),
        "sleep 0.1",
        0,
    ));
    stress.start_core("master", &[]);
    stress.start_workers(size.n(2, 8), size.n(4, 8), &[]);
    let parsed = stress.wait_for_dags(dags.len(), 10 * MINUTE);
    report.set("parse_all_s", parsed.as_secs_f64());
    stress.trigger(&dags, 1);
    stress.wait_for_runs(dags.len(), size.time(20 * MINUTE, 90 * MINUTE), |_| {});
    stress.measure(&mut report, "master", 1.0);
    if pooled {
        report.broken(pooler_not_found(&stress, &["master"]));
    }
    finish(&stress, report, &["success"]);
}

/// What the master's log says when the connection it would listen through passes a pooler in
/// transaction mode (B16).
const POOLER_FOUND: &str = "passes a connection pooler in transaction mode";

/// The masters among `names` whose logs don't say they found the pooler.
fn pooler_not_found(stress: &Stress, names: &[&str]) -> Vec<String> {
    names
        .iter()
        .filter(|name| !stress.cluster.log_of(name).contains(POOLER_FOUND))
        .map(|name| format!("{name} didn't find the pooler in transaction mode it goes through"))
        .collect()
}

#[test]
fn stress_01_throughput() {
    throughput(postgres_database(), "01_throughput", false);
}

/// Scenario 2: Command bursts: many triggers at once, a pause/unpause storm, and cancelling many
/// running runs at once.
#[test]
fn stress_02_command_burst() {
    let mut stress = Stress::new(Some(postgres_database()));
    let size = stress.size;
    let mut report = Report::new("02_command_burst");
    let burst = command_dags(&stress, "burst", 1, 1, "true", 0);
    let flip = command_dags(&stress, "flip", 1, 1, "true", 0);
    let long = command_dags(&stress, "long", 1, 1, "sleep 600", 0);
    stress.start_core("master", &[]);
    let slots = size.n(4, 8);
    stress.start_workers(2, slots, &[]);
    stress.wait_for_dags(3, 5 * MINUTE);
    let api = stress.cluster.api.clone();
    // Long runs first, so their tasks hold the slots by the time they are cancelled.
    let cancels = size.n(10, 50);
    stress.trigger(&long, cancels);
    let triggers = size.n(150, 1000);
    let started = std::time::Instant::now();
    stress.trigger(&burst, triggers);
    report.set(
        "trigger_calls_per_s",
        f64::from(u32::try_from(triggers).unwrap()) / started.elapsed().as_secs_f64(),
    );
    let flips = (0..100)
        .map(|i| {
            vec![
                "dags".to_owned(),
                if i % 2 == 0 { "pause" } else { "unpause" }.to_owned(),
                flip[0].clone(),
            ]
        })
        .collect();
    stress.expect_ok(&cli_many(&api, flips, 16));
    // Cancel every long run while some of them run.
    stress.cluster.wait("long tasks running", || {
        stress
            .db
            .count("SELECT COUNT(*) FROM attempts WHERE status IS NULL AND started_at IS NOT NULL")
            > 0
    });
    let cancel_calls: Vec<Vec<String>> = stress
        .db
        .run_ids(&long[0])
        .into_iter()
        .map(|id| vec!["runs".to_owned(), "cancel".to_owned(), id])
        .collect();
    assert_eq!(cancel_calls.len(), cancels);
    stress.expect_ok(&cli_many(&api, cancel_calls, 16));
    stress.wait_for_runs(cancels + triggers, 15 * MINUTE, |_| {});
    let applied = stress
        .db
        .count("SELECT COUNT(*) FROM commands WHERE kind = 'trigger_run' AND status = 'applied'");
    let manual = stress
        .db
        .count("SELECT COUNT(*) FROM runs WHERE kind = 'manual'");
    if applied != manual || manual != i64::try_from(cancels + triggers).unwrap() {
        report.broken(vec![format!(
            "{applied} triggers applied, {manual} manual runs, {} asked",
            cancels + triggers
        )]);
    }
    let cancelled = stress
        .db
        .count("SELECT COUNT(*) FROM runs WHERE state = 'cancelled'");
    if cancelled != i64::try_from(cancels).unwrap() {
        report.broken(vec![format!("{cancelled} runs cancelled of {cancels}")]);
    }
    let latency = stress.db.floats(&format!(
        "SELECT ({}) - ({}) FROM commands WHERE decided_at IS NOT NULL",
        stress.db.epoch("decided_at"),
        stress.db.epoch("requested_at")
    ));
    report.at_most("command_decided_p99_s", quantile(&latency, 0.99), 30.0);
    finish(&stress, report, &["success", "cancelled"]);
}

/// Scenario 3: Many workers, with churn: random workers die with their tasks (`kill -9` of the whole
/// tree) and come back a little later.
#[test]
fn stress_03_worker_churn() {
    let mut stress = Stress::new(Some(postgres_database()));
    let size = stress.size;
    let mut report = Report::new("03_worker_churn");
    let dags = command_dags(&stress, "churn", size.n(15, 60), 10, "sleep 2", 20);
    stress.start_core("master", &[]);
    let workers = size.n(6, 30);
    stress.start_workers(workers, 2, &[]);
    stress.wait_for_dags(dags.len(), 10 * MINUTE);
    stress.trigger(&dags, 1);
    let mut random = Lcg::new(3);
    let mut down: Vec<(String, std::time::Instant)> = Vec::new();
    let mut deaths = 0_u32;
    stress.wait_for_runs(dags.len(), size.time(25 * MINUTE, 90 * MINUTE), |stress| {
        // One worker dies every 5 s or so; each comes back after 3 s.
        let now = std::time::Instant::now();
        let back: Vec<String> = down
            .iter()
            .filter(|(_, at)| now >= *at)
            .map(|(id, _)| id.clone())
            .collect();
        down.retain(|(_, at)| now < *at);
        for id in back {
            stress.start_worker(&id, 2, &[]);
        }
        if random.below(5) == 0 && down.len() < workers / 2 {
            let id = format!("w{:02}", random.below(workers) + 1);
            if !down.iter().any(|(d, _)| *d == id)
                && stress.cluster.roles.iter().any(|r| r.name == id)
            {
                let killed = pod_death(&mut stress.cluster, &id);
                stress.killed.extend(killed);
                down.push((id, now + Duration::from_secs(3)));
                deaths += 1;
            }
        }
    });
    report.set("worker_deaths", f64::from(deaths));
    report.set(
        "lost_attempts",
        f64::from(
            u32::try_from(
                stress
                    .db
                    .count("SELECT COUNT(*) FROM attempts WHERE status = 'lost'"),
            )
            .unwrap(),
        ),
    );
    stress.measure(&mut report, "master", 0.2);
    finish(&stress, report, &["success"]);
}

/// Two masters reached through a proxy that follows the ready one, as a Service would; the
/// workers' master URL.
struct Masters {
    proxy: Proxy,
    list: Arc<Mutex<Vec<(SocketAddr, SocketAddr)>>>,
    stop: Arc<AtomicBool>,
    follower: Option<std::thread::JoinHandle<()>>,
}

impl Masters {
    fn new() -> Self {
        let proxy = Proxy::new(None);
        let list = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let follower = Some(follow_ready_master(&proxy, list.clone(), stop.clone()));
        Self {
            proxy,
            list,
            stop,
            follower,
        }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.proxy.port)
    }

    /// Starts master `name` (its own gRPC port) with `env`, and puts it behind the proxy.
    fn start(&self, stress: &mut Stress, name: &str, env: &[(&str, &str)]) {
        let mut all = load::ROLE_ENV.to_vec();
        all.push(("SCHEDR_CONFIG__MASTER__GRPC_PORT", "0"));
        all.extend_from_slice(env);
        stress.cluster.start_as(name, "master", &all);
        let grpc = stress.cluster.grpc_port_of(name);
        let ops = &stress
            .cluster
            .ops
            .iter()
            .find(|(n, _)| n == name)
            .unwrap()
            .1;
        let entry = (
            SocketAddr::from(([127, 0, 0, 1], grpc)),
            ops.parse().unwrap(),
        );
        let mut list = self.list.lock().unwrap();
        list.retain(|(g, _)| g.port() != grpc);
        list.push(entry);
    }
}

impl Drop for Masters {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(follower) = self.follower.take() {
            let _ = follower.join();
        }
    }
}

/// Short leases for the failover scenarios: a master leads 6 s after a renewal, less the 2 s
/// query limit.
const SHORT_LEASE: &[(&str, &str)] = &[
    ("SCHEDR_CONFIG__MASTER__LEASE_PERIOD", "6s"),
    ("SCHEDR_CONFIG__DATABASE__QUERY_TIMEOUT", "2s"),
];

/// Waits until the role `name` exits; its exit code.
fn exit_code(stress: &mut Stress, name: &str, limit: Duration) -> Option<i32> {
    let deadline = std::time::Instant::now() + limit;
    while std::time::Instant::now() < deadline {
        let role = stress.cluster.roles.iter_mut().find(|r| r.name == name)?;
        if let Some(status) = role.child.try_wait().unwrap() {
            return status.code();
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    None
}

/// Scenario 4: Master failover under load: the leading master killed, then the next one frozen past
/// its lease and thawed (its late writes refused: it exits).
fn failover(database_url: String, scenario: &'static str, pooled: bool) {
    let mut stress = Stress::new(Some(database_url));
    let size = stress.size;
    let mut report = Report::new(scenario);
    let dags = command_dags(&stress, "fail", size.n(15, 60), 10, "sleep 1", 10);
    let masters = Masters::new();
    masters.start(&mut stress, "m1", SHORT_LEASE);
    stress
        .cluster
        .start_as("webserver", "webserver", load::ROLE_ENV);
    masters.start(&mut stress, "m2", SHORT_LEASE);
    let url = masters.url();
    stress.start_workers(
        size.n(3, 8),
        4,
        &[("SCHEDR_CONFIG__WORKER__MASTER_URL", &url)],
    );
    stress.wait_for_dags(dags.len(), 10 * MINUTE);
    stress.trigger(&dags, 1);
    stress.cluster.wait("tasks under way", || {
        stress
            .db
            .count("SELECT COUNT(*) FROM attempts WHERE status = 'success'")
            > 10
    });
    // m1 leads (it started first): it dies; m2 takes over once its lease expires.
    let killed = pod_death(&mut stress.cluster, "m1");
    stress.killed.extend(killed);
    stress.cluster.wait("m2 to lead", || {
        stress.cluster.ops_get("m2", "/readyz").0 == 200
    });
    masters.start(&mut stress, "m1", SHORT_LEASE);
    std::thread::sleep(Duration::from_secs(5));
    // m2 freezes past its lease: m1 takes over; thawed, m2's writes are refused and it exits.
    let frozen = pid_of(&stress.cluster, "m2");
    signal(frozen, "STOP");
    stress.cluster.wait("m1 to lead again", || {
        stress.cluster.ops_get("m1", "/readyz").0 == 200
    });
    signal(frozen, "CONT");
    match exit_code(&mut stress, "m2", MINUTE) {
        Some(1) => {}
        other => report.broken(vec![format!(
            "the thawed master m2 ended with {other:?}, not 1"
        )]),
    }
    stress.wait_for_runs(dags.len(), size.time(20 * MINUTE, 60 * MINUTE), |_| {});
    stress.measure(&mut report, "m1", 0.2);
    drop(masters);
    if pooled {
        report.broken(pooler_not_found(&stress, &["m1", "m2"]));
    }
    finish(&stress, report, &["success"]);
}

#[test]
fn stress_04_master_failover() {
    failover(postgres_database(), "04_master_failover", false);
}

/// Scenario 5: Database trouble: the leading master's connection to Postgres stalls, first shorter than
/// its lease (it goes on), then longer (the standby takes over; the stalled master exits).
#[test]
fn stress_05_database_stall() {
    let url = postgres_database();
    let (credentials, rest) = url.rsplit_once('@').unwrap();
    let (server, database) = rest.split_once('/').unwrap();
    let to_db = Proxy::new(Some(server.parse().unwrap_or_else(|_| {
        use std::net::ToSocketAddrs;
        server.to_socket_addrs().unwrap().next().unwrap()
    })));
    let stalled_url = format!("{credentials}@127.0.0.1:{}/{database}", to_db.port);
    let mut stress = Stress::new(Some(url));
    let size = stress.size;
    let mut report = Report::new("05_database_stall");
    let dags = command_dags(&stress, "stall", size.n(15, 60), 10, "sleep 1", 10);
    let masters = Masters::new();
    let mut m1_env = SHORT_LEASE.to_vec();
    m1_env.push(("SCHEDR_CONFIG__DATABASE__URL", &stalled_url));
    masters.start(&mut stress, "m1", &m1_env);
    stress
        .cluster
        .start_as("webserver", "webserver", load::ROLE_ENV);
    masters.start(&mut stress, "m2", SHORT_LEASE);
    let master_url = masters.url();
    stress.start_workers(
        size.n(3, 8),
        4,
        &[("SCHEDR_CONFIG__WORKER__MASTER_URL", &master_url)],
    );
    stress.wait_for_dags(dags.len(), 10 * MINUTE);
    stress.trigger(&dags, 1);
    stress.cluster.wait("tasks under way", || {
        stress
            .db
            .count("SELECT COUNT(*) FROM attempts WHERE status = 'success'")
            > 10
    });
    // Past the query limit, short of the lease: m1 keeps leading.
    to_db.freeze(true);
    std::thread::sleep(Duration::from_secs(3));
    to_db.freeze(false);
    std::thread::sleep(Duration::from_secs(5));
    if stress.cluster.ops_get("m1", "/readyz").0 != 200 {
        report.broken(vec![
            "m1 lost the lead over a stall shorter than its lease".to_owned(),
        ]);
    }
    // Past the lease: m2 takes over, and m1 exits once it can tell.
    to_db.freeze(true);
    stress.cluster.wait("m2 to lead", || {
        stress.cluster.ops_get("m2", "/readyz").0 == 200
    });
    to_db.freeze(false);
    match exit_code(&mut stress, "m1", MINUTE) {
        Some(1) => {}
        other => report.broken(vec![format!(
            "the stalled master m1 ended with {other:?}, not 1"
        )]),
    }
    stress.wait_for_runs(dags.len(), size.time(20 * MINUTE, 60 * MINUTE), |_| {});
    let lost = stress
        .db
        .count("SELECT COUNT(*) FROM attempts WHERE status = 'lost'");
    if lost != 0 {
        report.broken(vec![format!(
            "{lost} attempts lost: the stall made live workers lost"
        )]);
    }
    stress.measure(&mut report, "m2", 0.2);
    drop(masters);
    finish(&stress, report, &["success"]);
}

/// Scenario 6: Through `PgBouncer` in transaction pooling: throughput and failover again, the
/// masters finding the pooler and polling instead of listening (B16).
#[test]
fn stress_06_pgbouncer_throughput() {
    throughput(
        through_pgbouncer(&postgres_database()),
        "06_pgbouncer_throughput",
        true,
    );
}

#[test]
fn stress_06_pgbouncer_failover() {
    failover(
        through_pgbouncer(&postgres_database()),
        "06_pgbouncer_failover",
        true,
    );
}

/// Scenario 7: Rolling restart: workers stopped with SIGTERM one by one (drained) while tasks run, each
/// started again; no task may fail (an interrupted task runs again without a retry).
#[test]
fn stress_07_rolling_restart() {
    let mut stress = Stress::new(Some(postgres_database()));
    let size = stress.size;
    let mut report = Report::new("07_rolling_restart");
    let dags = command_dags(&stress, "roll", size.n(15, 60), 8, "sleep 3", 0);
    stress.start_core("master", &[]);
    let workers = size.n(3, 8);
    stress.start_workers(workers, 4, &[]);
    stress.wait_for_dags(dags.len(), 10 * MINUTE);
    stress.trigger(&dags, 1);
    let mut next = 0;
    let mut restarts = 0_u32;
    stress.wait_for_runs(dags.len(), size.time(25 * MINUTE, 90 * MINUTE), |stress| {
        let id = format!("w{:02}", next % workers + 1);
        next += 1;
        let status = stress.cluster.terminate(&id);
        assert!(
            status.success(),
            "{id} ended with {status} on SIGTERM\n{}",
            stress.cluster.logs()
        );
        stress.cluster.kill(&id);
        stress.start_worker(&id, 4, &[]);
        restarts += 1;
        std::thread::sleep(Duration::from_secs(2));
    });
    report.set("restarts", f64::from(restarts));
    report.set(
        "interrupted_attempts",
        f64::from(
            u32::try_from(
                stress
                    .db
                    .count("SELECT COUNT(*) FROM attempts WHERE status = 'interrupted'"),
            )
            .unwrap(),
        ),
    );
    stress.measure(&mut report, "master", 0.2);
    finish(&stress, report, &["success"]);
}

/// Scenario 8: Code churn: many DAG files, the code changing every few seconds while runs go on.
#[test]
fn stress_08_code_churn() {
    let mut stress = Stress::new(Some(postgres_database()));
    let size = stress.size;
    let mut report = Report::new("08_code_churn");
    let files = size.n(300, 1000);
    let dags = command_dags(&stress, "code", files, 1, "true", 2);
    stress.start_core("master", &[]);
    stress.start_workers(size.n(2, 4), 4, &[]);
    let parsed = stress.wait_for_dags(files, 15 * MINUTE);
    report.at_most("parse_all_s", parsed.as_secs_f64(), 600.0);
    let mut random = Lcg::new(8);
    let churn_until = std::time::Instant::now() + size.time(MINUTE, 5 * MINUTE);
    let mut changes = 0_u32;
    let mut triggered = 0;
    let mut last_change = check::now_s();
    while std::time::Instant::now() < churn_until {
        for _ in 0..10 {
            let id = &dags[random.below(files)];
            let path = stress.cluster.root().join(format!("{id}.py"));
            let mut source = std::fs::read_to_string(&path).unwrap();
            let _ = writeln!(source, "# change {changes}");
            std::fs::write(&path, source).unwrap();
            changes += 1;
        }
        // When the code last changed: a parse from then on has it (taken after the 2 s sleep
        // below, it failed parses that had it).
        last_change = check::now_s();
        let some: Vec<String> = (0..5).map(|_| dags[random.below(files)].clone()).collect();
        stress.trigger(&some, 1);
        triggered += some.len();
        std::thread::sleep(Duration::from_secs(2));
    }
    stress.wait_for_runs(triggered, 15 * MINUTE, |_| {});
    let caught_up = std::time::Instant::now();
    let parses_at_last_change = stress.db.count("SELECT COUNT(*) FROM parse_results");
    let newest_parse = || {
        let newest = stress.db.floats(&format!(
            "SELECT MAX({}) FROM parse_results",
            stress.db.epoch("parsed_at")
        ));
        newest.first().copied().unwrap_or(0.0)
    };
    while newest_parse() < last_change && caught_up.elapsed() < 10 * MINUTE {
        std::thread::sleep(Duration::from_secs(2));
    }
    // How the parses went, whether they caught up or not.
    let parse_count = stress.db.count("SELECT COUNT(*) FROM parse_results");
    report.set("parses", f64::from(u32::try_from(parse_count).unwrap()));
    report.set(
        "parses_after_last_change",
        f64::from(u32::try_from(parse_count - parses_at_last_change).unwrap()),
    );
    report.set(
        "newest_parse_behind_last_change_s",
        last_change - newest_parse(),
    );
    report.at_most("parse_catch_up_s", caught_up.elapsed().as_secs_f64(), 300.0);
    report.set("code_changes", f64::from(changes));
    let listed = stress
        .db
        .count("SELECT COUNT(*) FROM dags WHERE removed_at IS NULL");
    if listed != i64::try_from(files).unwrap() {
        report.broken(vec![format!("{listed} DAGs listed of {files}")]);
    }
    finish(&stress, report, &["success"]);
}

/// Scenario 9: Standalone on SQLite under load.
#[test]
fn stress_09_standalone_sqlite() {
    let mut stress = Stress::new(None);
    let size = stress.size;
    let mut report = Report::new("09_standalone_sqlite");
    let dags = command_dags(&stress, "lite", size.n(20, 100), 10, "sleep 0.1", 0);
    let mut env = load::ROLE_ENV.to_vec();
    env.push(("SCHEDR_CONFIG__WORKER__SLOTS", "8"));
    stress
        .cluster
        .start_args("standalone", &["standalone", "--migrate"], &env);
    stress.wait_for_dags(dags.len(), 10 * MINUTE);
    stress.trigger(&dags, 1);
    stress.wait_for_runs(dags.len(), size.time(20 * MINUTE, 60 * MINUTE), |_| {});
    stress.measure(&mut report, "standalone", 1.0);
    let locked = stress.cluster.logs().matches("database is locked").count();
    if locked != 0 {
        report.broken(vec![format!(
            "`database is locked` {locked} times in the logs"
        )]);
    }
    finish(&stress, report, &["success"]);
}

/// A role's resident memory (KiB) and open files, from `/proc`.
fn usage(pid: u32) -> Option<(u64, u64)> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let rss = status
        .lines()
        .find(|l| l.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    let files = std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?.count();
    Some((rss, u64::try_from(files).ok()?))
}

/// Scenario 10: Soak: a steady load for a while (3 minutes quick, an hour full); the roles' memory and
/// open files must stay flat once warmed up.
#[test]
fn stress_10_soak() {
    let mut stress = Stress::new(Some(postgres_database()));
    let size = stress.size;
    let mut report = Report::new("10_soak");
    let dags = command_dags(&stress, "soak", 10, 5, "sleep 0.5", 0);
    stress.start_core("master", &[]);
    stress.start_workers(2, 4, &[]);
    stress.wait_for_dags(dags.len(), 10 * MINUTE);
    stress.trigger(&dags, 1);
    let roles = ["master", "webserver", "w01", "w02"];
    let mut samples: Vec<Vec<(u64, u64)>> = vec![Vec::new(); roles.len()];
    let until = std::time::Instant::now() + size.time(3 * MINUTE, 60 * MINUTE);
    let mut runs = dags.len();
    while std::time::Instant::now() < until {
        stress.trigger(&dags, 1);
        runs += dags.len();
        for (role, list) in roles.iter().zip(&mut samples) {
            list.extend(usage(pid_of(&stress.cluster, role)));
        }
        std::thread::sleep(Duration::from_secs(10));
    }
    stress.wait_for_runs(runs, 15 * MINUTE, |_| {});
    for (role, list) in roles.iter().zip(&samples) {
        // The first quarter warms up; then the last quarter against the second.
        let quarter = list.len() / 4;
        if quarter == 0 {
            continue;
        }
        let mean = |part: &[(u64, u64)], pick: fn(&(u64, u64)) -> u64| {
            part.iter().map(pick).sum::<u64>() / u64::try_from(part.len()).unwrap()
        };
        let (early, late) = (&list[quarter..2 * quarter], &list[list.len() - quarter..]);
        let (rss_early, rss_late) = (mean(early, |s| s.0), mean(late, |s| s.0));
        let (fds_early, fds_late) = (mean(early, |s| s.1), mean(late, |s| s.1));
        report.set(
            &format!("{role}_rss_mib"),
            f64::from(u32::try_from(rss_late >> 10).unwrap()),
        );
        report.at_most(
            &format!("{role}_rss_growth_mib"),
            f64::from(u32::try_from(rss_late.saturating_sub(rss_early) >> 10).unwrap()),
            f64::from(u32::try_from((rss_early >> 10) / 2 + 50).unwrap()),
        );
        report.at_most(
            &format!("{role}_open_files_growth"),
            f64::from(u32::try_from(fds_late.saturating_sub(fds_early)).unwrap()),
            50.0,
        );
    }
    stress.measure(&mut report, "master", 0.5);
    finish(&stress, report, &["success"]);
}

/// Scenario 11: Heavy output: tasks writing big logs while the API reads them, and Python outputs near
/// their size limit.
#[test]
fn stress_11_heavy_output() {
    let mut stress = Stress::new(Some(postgres_database()));
    let size = stress.size;
    let mut report = Report::new("11_heavy_output");
    let loud = command_dags(
        &stress,
        "loud",
        size.n(4, 20),
        4,
        "yes xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx | head -c 5000000",
        0,
    );
    stress.write_dag("big", BIG_OUTPUT);
    stress.start_core("master", &[]);
    stress.start_workers(2, 4, &[]);
    stress.wait_for_dags(loud.len() + 1, 10 * MINUTE);
    let mut all = loud.clone();
    all.push("big".to_owned());
    stress.trigger(&all, size.n(2, 5));
    let runs = all.len() * size.n(2, 5);
    stress.wait_for_runs(runs, size.time(20 * MINUTE, 60 * MINUTE), |_| {});
    // Every log read back through the webserver, four readers at once.
    let api = stress.cluster.api.clone();
    let logs = loud_logs(&stress);
    let times = std::thread::scope(|scope| {
        logs.chunks(logs.len().div_ceil(4).max(1))
            .map(|part| {
                let api = api.clone();
                scope.spawn(move || {
                    part.iter()
                        .map(|(run, task)| read_log(&api, run, task))
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    let short = times.iter().filter(|(_, bytes)| *bytes < 5_000_000).count();
    if short != 0 {
        report.broken(vec![format!("{short} logs read back shorter than written")]);
    }
    let seconds: Vec<f64> = times.iter().map(|(s, _)| *s).collect();
    report.at_most("log_read_p99_s", quantile(&seconds, 0.99), 30.0);
    let outputs = stress
        .db
        .count("SELECT COUNT(*) FROM outputs WHERE size_bytes > 900000");
    report.set("big_outputs", f64::from(u32::try_from(outputs).unwrap()));
    stress.measure(&mut report, "master", 0.1);
    finish(&stress, report, &["success"]);
}

const BIG_OUTPUT: &str = r#"from schedr import Dag, task

@task
def big() -> str:
    return "x" * 1_000_000

@task
def size(text: str) -> int:
    return len(text)

with Dag("big"):
    size(big())
"#;

/// The (run, task) of every loud task of `loud0001`.
fn loud_logs(stress: &Stress) -> Vec<(String, String)> {
    stress
        .db
        .run_ids("loud0001")
        .into_iter()
        .flat_map(|id| (0..4).map(move |k| (id.clone(), format!("c{k:04}"))))
        .collect()
}

/// Reads a task's whole log (try 1) through the API, part by part: seconds and bytes.
fn read_log(api: &str, run: &str, task: &str) -> (f64, u64) {
    let started = std::time::Instant::now();
    let (mut from, mut bytes) = (0_u64, 0_u64);
    loop {
        let output = std::process::Command::new("curl")
            .args([
                "-sf",
                &format!("{api}/api/v1/runs/{run}/tasks/{task}/attempts/1/log?from={from}"),
            ])
            .output()
            .unwrap();
        let Ok(chunk) = serde_json::from_slice::<serde_json::Value>(&output.stdout) else {
            break;
        };
        let next = chunk["next"].as_u64().unwrap_or(from);
        bytes += next - from;
        if next >= chunk["size"].as_u64().unwrap_or(0) || next == from {
            break;
        }
        from = next;
    }
    (started.elapsed().as_secs_f64(), bytes)
}

/// Scenario 12: The memory watchdog under load: many tasks over their budget at once, beside normal
/// ones on the same workers.
#[test]
fn stress_12_memory_watchdog() {
    let mut stress = Stress::new(Some(postgres_database()));
    let size = stress.size;
    let mut report = Report::new("12_memory_watchdog");
    stress.write_dag("greedy", GREEDY);
    let normal = command_dags(&stress, "calm", 4, 4, "sleep 1", 0);
    stress.start_core("master", &[]);
    let budget = [
        ("SCHEDR_CONFIG__WORKER__MEMORY_BUDGET", "100MiB"),
        ("SCHEDR_CONFIG__WORKER__MEMORY_CHECK_INTERVAL", "100ms"),
    ];
    stress.start_workers(2, 4, &budget);
    stress.wait_for_dags(normal.len() + 1, 10 * MINUTE);
    let rounds = size.n(3, 10);
    let mut all = normal.clone();
    all.push("greedy".to_owned());
    stress.trigger(&all, rounds);
    stress.wait_for_runs(
        all.len() * rounds,
        size.time(20 * MINUTE, 60 * MINUTE),
        |_| {},
    );
    let greedy_runs = i64::try_from(rounds).unwrap();
    let killed = stress
        .db
        .count("SELECT COUNT(*) FROM attempts WHERE status = 'oom_killed'");
    if killed != greedy_runs * 2 {
        report.broken(vec![format!(
            "{killed} attempts killed over their budget, {} expected",
            greedy_runs * 2
        )]);
    }
    let workers = stress
        .db
        .count("SELECT COUNT(*) FROM workers WHERE state = 'active'");
    if workers != 2 {
        report.broken(vec![format!(
            "{workers} workers active of 2 after the kills"
        )]);
    }
    report.set("oom_killed", f64::from(u32::try_from(killed).unwrap()));
    stress.measure(&mut report, "master", 0.1);
    finish(&stress, report, &["success", "failed"]);
}

const GREEDY: &str = r#"import time

from schedr import Dag, task

def grow() -> int:
    chunks = []
    for _ in range(40):
        chunks.append(b"x" * (10 << 20))
        time.sleep(0.05)
    time.sleep(30)
    return len(chunks)

@task(retries=0)
def greedy_a() -> int:
    return grow()

@task(retries=0)
def greedy_b() -> int:
    return grow()

with Dag("greedy"):
    greedy_a()
    greedy_b()
"#;

/// Scenario 13: Python tasks as the load: chains of `@task` functions passing their outputs on, a
/// Python process for each, spawned (a new interpreter each).
#[test]
fn stress_13_python_tasks() {
    python_tasks("13_python_tasks", false);
}

/// Scenario 13, forked: the same load with the workers' fork servers on (M2.5; a short preload
/// list, as this Python has none of the image's libraries); every task forked, and how long
/// the starts took next to the spawned run's.
#[test]
fn stress_13_python_tasks_forked() {
    python_tasks("13_python_tasks_forked", true);
}

/// Scenario 13's load, with the workers' fork servers on when `forked`.
fn python_tasks(name: &'static str, forked: bool) {
    let mut stress = Stress::new(Some(postgres_database()));
    let size = stress.size;
    let mut report = Report::new(name);
    let count = size.n(20, 200);
    let dags: Vec<String> = (1..=count)
        .map(|i| {
            let id = format!("py{i:04}");
            stress.write_dag(&id, &python_dag(&id));
            id
        })
        .collect();
    let python = support::python();
    let fork_env = [
        ("SCHEDR_CONFIG__INTERPRETERS__PYTHON__PATH", python.as_str()),
        ("SCHEDR_CONFIG__INTERPRETERS__PYTHON__FORK_SERVER", "true"),
        (
            "SCHEDR_CONFIG__INTERPRETERS__PYTHON__PRELOAD",
            r#"["json", "decimal"]"#,
        ),
    ];
    let workers = size.n(2, 8);
    stress.start_core("master", &[]);
    stress.start_workers(workers, 4, if forked { &fork_env } else { &[] });
    stress.wait_for_dags(count, 10 * MINUTE);
    stress.trigger(&dags, 1);
    stress.wait_for_runs(count, size.time(20 * MINUTE, 60 * MINUTE), |_| {});
    let wrong = stress.db.count("SELECT COUNT(*) FROM outputs o JOIN task_instances ti ON ti.id = o.task_instance_id WHERE ti.task_id = 'total' AND o.value::text <> '120'");
    if wrong != 0 {
        report.broken(vec![format!(
            "{wrong} totals wrong: outputs passed on badly"
        )]);
    }
    let method = if forked { "fork" } else { "spawn" };
    let attempts = stress.db.count("SELECT COUNT(*) FROM attempts");
    let by_method = stress.db.count(&format!(
        "SELECT COUNT(*) FROM attempts WHERE start_method = '{method}'"
    ));
    #[expect(
        clippy::cast_precision_loss,
        reason = "a share of a few thousand attempts"
    )]
    let share = by_method as f64 / attempts.max(1) as f64;
    report.at_least(&format!("{method}_share"), share, 0.99);
    // The start's runner phase (to the runner's hello) on the first worker.
    let (status, metrics) = stress.cluster.ops_get("w01", "/metrics");
    let labels = format!("method=\"{method}\",phase=\"runner\"");
    let name = "schedr_worker_task_start_seconds";
    let p50 = check::labelled_quantile(&metrics, name, &labels, 0.5);
    let p99 = check::labelled_quantile(&metrics, name, &labels, 0.99);
    if status != 200 || p50 <= 0.0 {
        // Read nothing: not a fast start.
        report.broken(vec![format!(
            "no `{method}` start times on w01 (HTTP {status})"
        )]);
    }
    report.set("start_runner_p50_s", p50);
    report.set("start_runner_p99_s", p99);
    stress.measure(&mut report, "master", 0.5);
    finish(&stress, report, &["success"]);
}

/// A DAG of Python tasks: five branches of `double` → `add_one` chains, summed.
fn python_dag(id: &str) -> String {
    format!(
        r#"from schedr import Dag, task

@task
def seed(k: int) -> list[int]:
    return [k] * 4

@task
def double(values: list[int]) -> list[int]:
    return [v * 2 for v in values]

@task
def add_one(values: list[int]) -> int:
    return sum(values)

@task
def total(parts: list[int]) -> int:
    return sum(parts)

with Dag("{id}"):
    total([
        add_one.override(task_id=f"add_one_{{k}}")(
            double.override(task_id=f"double_{{k}}")(seed.override(task_id=f"seed_{{k}}")(k))
        )
        for k in range(1, 6)
    ])
"#
    )
}

/// Scenario 14: Quiet DAGs: a few busy DAGs, measured alone and then beside many daily DAGs
/// that don't fire meanwhile (PROJECT.md, "Quiet DAGs"). The rounds' times beside the quiet
/// ones, over their times alone: 7.1 before M2.5.2, at most 2 since.
#[test]
fn stress_14_quiet_dags() {
    let mut stress = Stress::new(Some(postgres_database()));
    let size = stress.size;
    let mut report = Report::new("14_quiet_dags");
    let window = size.time(MINUTE, 5 * MINUTE);
    let busy: Vec<String> = (1..=size.n(5, 10))
        .map(|i| {
            let id = format!("busy{i:04}");
            let every = "Every(timedelta(seconds=10))";
            stress.write_dag(&id, &one_task_dag(&id, every, &stress.marks));
            id
        })
        .collect();
    stress.start_core("master", &[]);
    stress.start_workers(2, 4, &[]);
    stress.wait_for_dags(busy.len(), 5 * MINUTE);
    unpause(&stress, &busy);
    let alone = rounds_over(&stress, &mut report, "alone", window, busy.len());

    // Daily, 12 hours from now: none fires while the scenario lasts.
    let hour = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / 3600
        % 24;
    let daily = format!(r#"Cron("0 {} * * *")"#, (hour + 12) % 24);
    let quiet: Vec<String> = (1..=size.n(300, 1000))
        .map(|i| {
            let id = format!("quiet{i:04}");
            stress.write_dag(&id, &one_task_dag(&id, &daily, &stress.marks));
            id
        })
        .collect();
    let parsed = stress.wait_for_dags(busy.len() + quiet.len(), 15 * MINUTE);
    report.set("parse_quiet_s", parsed.as_secs_f64());
    unpause(&stress, &quiet);
    // The steady state: every one of them quiet (idle, each turns quiet when next visited).
    let unpaused = std::time::Instant::now();
    let want = f64::from(u32::try_from(quiet.len()).unwrap());
    stress
        .cluster
        .wait_for("the quiet DAGs quiet", 3 * MINUTE, || {
            let (_, body) = stress.cluster.ops_get("master", "/metrics");
            Rounds::scraped(&body).dags.1 >= want
        });
    report.set("quiet_after_unpause_s", unpaused.elapsed().as_secs_f64());
    let beside = rounds_over(&stress, &mut report, "beside_quiet", window, busy.len());

    report.set("quiet_dags", f64::from(u32::try_from(quiet.len()).unwrap()));
    report.set("rounds_alone", alone.count);
    report.set("rounds_beside_quiet", beside.count);
    report.set("round_mean_alone_ms", alone.mean() * 1000.0);
    report.set("round_mean_beside_quiet_ms", beside.mean() * 1000.0);
    report.set("round_p99_alone_s", alone.quantile(0.99));
    report.set("round_p99_beside_quiet_s", beside.quantile(0.99));
    // Quiet DAGs (M2.5.2): the rounds beside them cost about what they cost alone (7.1 times
    // as much before), the slowest too: the p99s are bucket edges √2 apart, so 1.5 allows one
    // bucket worse (noise at an edge), not two (the full pass's 2x).
    report.at_most(
        "round_mean_ratio",
        beside.mean() / alone.mean().max(1e-6),
        2.0,
    );
    report.at_most(
        "round_p99_ratio",
        beside.quantile(0.99) / alone.quantile(0.99).max(1e-6),
        1.5,
    );
    // No quiet DAG fired.
    let fired = stress
        .db
        .count("SELECT COUNT(DISTINCT dag_id) FROM runs WHERE kind = 'scheduled'");
    if fired != i64::try_from(busy.len()).unwrap() {
        report.broken(vec![format!(
            "{fired} DAGs had scheduled runs, expected the {} busy ones",
            busy.len()
        )]);
    }
    let calls = busy
        .iter()
        .map(|d| vec!["dags".to_owned(), "pause".to_owned(), d.clone()])
        .collect();
    stress.expect_ok(&cli_many(&stress.cluster.api, calls, 16));
    let runs = usize::try_from(stress.db.runs()).unwrap();
    stress.wait_for_runs(runs, 5 * MINUTE, |_| {});
    finish(&stress, report, &["success"]);
}

/// A DAG `id` with `trigger` and one quick command task between its marks.
fn one_task_dag(id: &str, trigger: &str, marks: &std::path::Path) -> String {
    let command = check::marked(marks, "true");
    format!(
        r#"from datetime import timedelta

from schedr import CommandOperator, Cron, Dag, Every

with Dag("{id}", trigger={trigger}):
    CommandOperator("tick", {command:?})
"#
    )
}

/// Unpauses `dags`, many CLI calls at once.
fn unpause(stress: &Stress, dags: &[String]) {
    let calls = dags
        .iter()
        .map(|d| vec!["dags".to_owned(), "unpause".to_owned(), d.clone()])
        .collect();
    stress.expect_ok(&cli_many(&stress.cluster.api, calls, 16));
}

/// How long the rounds settle before a window: the unpause commands just sent each woke one.
/// Scenario 15: What announcing changes costs the master (M3.3; PROJECT.md, "How M3.3 follows
/// changes"), on Postgres: scenario 1's load four times, each on a database of its own, the
/// triggers that announce what changed off, on, on, off (so a drift over the scenario, other
/// load on the machine, weighs on both alike); with them on a client follows the webserver's
/// events (as a live page does, so the notifications are listened to). The rounds' mean and
/// p99 and the tasks a second, with over without.
#[test]
fn stress_15_change_signals() {
    change_signals("15_change_signals", Backend::Postgres);
}

/// Scenario 15b: as 15, on SQLite through `schedr standalone` (scenario 9's load), the
/// triggers there writing the `changes` table: switched off for the runs without, as
/// `[database] change_notifications = false` leaves them (each still reads the switch).
#[test]
fn stress_15b_change_signals_sqlite() {
    change_signals("15b_change_signals_sqlite", Backend::Sqlite);
}

/// Scenario 15c: as 15, with scenario 13's load (Python tasks passing outputs on, spawned): a
/// load bound by the CPU, where the triggers' work at commit competes with the tasks' starts
/// rather than hiding behind the disk's flush as with 15's `sleep` commands.
#[test]
fn stress_15c_change_signals_python() {
    change_signals("15c_change_signals_python", Backend::PostgresPython);
}

/// Scenario 15's database and load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backend {
    /// Postgres, scenario 1's load.
    Postgres,
    /// SQLite through standalone, scenario 9's load.
    Sqlite,
    /// Postgres, scenario 13's Python tasks, spawned.
    PostgresPython,
}

fn change_signals(scenario: &'static str, backend: Backend) {
    let runs: Vec<(bool, SignalRun)> = [false, true, true, false]
        .into_iter()
        .enumerate()
        .map(|(i, on)| (on, signal_load(scenario, i + 1, backend, on)))
        .collect();
    let mut report = Report::new(scenario);
    let mean = |on: bool, value: &dyn Fn(&SignalRun) -> f64| {
        let values: Vec<f64> = runs
            .iter()
            .filter(|(o, _)| *o == on)
            .map(|(_, r)| value(r))
            .collect();
        values.iter().sum::<f64>() / f64::from(u32::try_from(values.len()).unwrap())
    };
    let round_mean = |r: &SignalRun| r.rounds.mean();
    let round_p99 = |r: &SignalRun| r.rounds.quantile(0.99);
    let tasks_per_s = |r: &SignalRun| r.done.tasks_per_s;
    let start_p99 = |r: &SignalRun| r.done.start_p99_s;
    for (name, on) in [("without", false), ("with", true)] {
        report.set(
            &format!("round_mean_{name}_ms"),
            mean(on, &round_mean) * 1000.0,
        );
        report.set(&format!("round_p99_{name}_s"), mean(on, &round_p99));
        report.set(&format!("tasks_per_s_{name}"), mean(on, &tasks_per_s));
        report.set(
            &format!("assigned_to_started_p99_{name}_s"),
            mean(on, &start_p99),
        );
    }
    for (i, (on, run)) in runs.iter().enumerate() {
        let name = if *on { "with" } else { "without" };
        report.set(
            &format!("run{}_{name}_round_mean_ms", i + 1),
            run.rounds.mean() * 1000.0,
        );
        report.set(
            &format!("run{}_{name}_tasks_per_s", i + 1),
            run.done.tasks_per_s,
        );
    }
    // The triggers did announce: each run's changes reached the client.
    let events_per_run = mean(true, &|r| r.run_events / r.runs);
    report.at_least("run_events_with_per_run", events_per_run, 1.0);
    // Measured 2026-10-28 (quick, four runs each): on Postgres 1.02 / 1.00 / 0.97, on SQLite
    // 0.94 / 1.17 / 0.98, all within run-to-run noise (two runs alike differed by 8%). The
    // limits catch a change that makes announcing expensive, not the noise: the p99s are
    // bucket edges √2 apart, so 1.5 allows one bucket worse.
    report.at_most(
        "round_mean_ratio",
        mean(true, &round_mean) / mean(false, &round_mean).max(1e-6),
        1.15,
    );
    report.at_most(
        "round_p99_ratio",
        mean(true, &round_p99) / mean(false, &round_p99).max(1e-6),
        1.5,
    );
    report.at_least(
        "tasks_per_s_ratio",
        mean(true, &tasks_per_s) / mean(false, &tasks_per_s).max(1e-6),
        0.9,
    );
    report.finish(String::new);
}

/// One run of scenario 15's load, and what it measured.
struct SignalRun {
    rounds: Rounds,
    done: load::Throughput,
    runs: f64,
    run_events: f64,
}

/// Run `number` of scenario 15's load (1's on Postgres, 9's on SQLite, 13's Python) on a database
/// of its own, the change triggers `on` or not; with them on, a client follows `GET
/// /api/v1/events` meanwhile, counting `run` events.
fn signal_load(scenario: &str, number: usize, backend: Backend, on: bool) -> SignalRun {
    let mut stress = Stress::new(match backend {
        Backend::Postgres | Backend::PostgresPython => Some(postgres_database()),
        Backend::Sqlite => None,
    });
    let size = stress.size;
    let name = format!(
        "{scenario}_run{number}_{}",
        if on { "with" } else { "without" }
    );
    let mut report = Report::new(Box::leak(name.into_boxed_str()));
    if !on {
        match backend {
            Backend::Postgres | Backend::PostgresPython => {
                for table in ["runs", "task_instances", "attempts", "dags", "workers"] {
                    stress
                        .db
                        .execute(&format!("ALTER TABLE {table} DISABLE TRIGGER USER"));
                }
            }
            // As `[database] change_notifications = false` leaves it: the triggers there, each
            // reading the switch.
            Backend::Sqlite => stress.db.execute("UPDATE changes_switch SET notify = 0"),
        }
    }
    let master = match backend {
        Backend::Postgres | Backend::PostgresPython => "master",
        Backend::Sqlite => "standalone",
    };
    let mut dags = match backend {
        Backend::Postgres => command_dags(
            &stress,
            "many",
            size.n(50, 1000),
            size.n(10, 15),
            "sleep 0.1",
            0,
        ),
        Backend::Sqlite => command_dags(&stress, "lite", size.n(20, 100), 10, "sleep 0.1", 0),
        Backend::PostgresPython => (1..=size.n(20, 200))
            .map(|i| {
                let id = format!("py{i:04}");
                stress.write_dag(&id, &python_dag(&id));
                id
            })
            .collect(),
    };
    match backend {
        Backend::Postgres => {
            dags.extend(command_dags(
                &stress,
                "wide",
                1,
                size.n(200, 5000),
                "sleep 0.1",
                0,
            ));
            stress.start_core("master", &[]);
            stress.start_workers(size.n(2, 8), size.n(4, 8), &[]);
        }
        Backend::Sqlite => {
            let mut env = load::ROLE_ENV.to_vec();
            env.push(("SCHEDR_CONFIG__WORKER__SLOTS", "8"));
            stress
                .cluster
                .start_args("standalone", &["standalone", "--migrate"], &env);
        }
        // As scenario 13: spawned, 4 slots a worker.
        Backend::PostgresPython => {
            stress.start_core("master", &[]);
            stress.start_workers(size.n(2, 8), 4, &[]);
        }
    }
    stress.wait_for_dags(dags.len(), 10 * MINUTE);
    let scrape = |stress: &Stress| {
        let (status, body) = stress.cluster.ops_get(master, "/metrics");
        assert_eq!(status, 200, "{master}'s metrics: {body}");
        Rounds::scraped(&body)
    };
    let stop = Arc::new(AtomicBool::new(false));
    let follower = on.then(|| follow_events(&stress.cluster.api, Arc::clone(&stop)));
    let before = scrape(&stress);
    stress.trigger(&dags, 1);
    stress.wait_for_runs(dags.len(), size.time(20 * MINUTE, 90 * MINUTE), |_| {});
    let rounds = scrape(&stress).since(&before);
    stop.store(true, Ordering::Relaxed);
    let run_events = follower.map_or(0.0, |f| f.join().unwrap());
    let done = stress.throughput();
    report.set("tasks_per_s", done.tasks_per_s);
    report.set("round_mean_ms", rounds.mean() * 1000.0);
    report.set("run_events", run_events);
    let runs = f64::from(u32::try_from(stress.db.runs()).unwrap());
    finish(&stress, report, &["success"]);
    SignalRun {
        rounds,
        done,
        runs,
        run_events,
    }
}

/// Follows the webserver's `GET /api/v1/events` (the API's `http://host:port`) until `stop`,
/// counting its `run` events.
fn follow_events(api: &str, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<f64> {
    use std::io::{Read as _, Write as _};
    let address = api
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_owned();
    std::thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(&address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        write!(
            stream,
            "GET /api/v1/events HTTP/1.1\r\nHost: {address}\r\nAccept: text/event-stream\r\n\r\n"
        )
        .unwrap();
        let (mut events, mut tail) = (0_u32, Vec::new());
        let mut buffer = [0_u8; 16 * 1024];
        while !stop.load(Ordering::Relaxed) {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    // Counted over what was read, with the end of the last read in front (an
                    // event's name cut between reads).
                    tail.extend_from_slice(&buffer[..read]);
                    let text = String::from_utf8_lossy(&tail).into_owned();
                    let whole = text.rfind('\n').map_or(0, |at| at + 1);
                    events += u32::try_from(text[..whole].matches("event: run").count()).unwrap();
                    tail = text.as_bytes()[whole..].to_vec();
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(e) => panic!("the events stream failed: {e}"),
            }
        }
        f64::from(events)
    })
}

const SETTLE: Duration = Duration::from_secs(20);

/// The master's rounds over the next `window` (named `name`), after [`SETTLE`]: the
/// difference of its tick histogram between two scrapes. Broken in `report` unless rounds
/// were counted and each of the `busy` DAGs had a scheduled run in it.
fn rounds_over(
    stress: &Stress,
    report: &mut Report,
    name: &str,
    window: Duration,
    busy: usize,
) -> Rounds {
    std::thread::sleep(SETTLE);
    let scrape = || {
        let (status, body) = stress.cluster.ops_get("master", "/metrics");
        assert_eq!(status, 200, "the master's metrics: {body}");
        Rounds::scraped(&body)
    };
    let started = check::now_s();
    let before = scrape();
    std::thread::sleep(window);
    let after = scrape();
    for (at, scraped) in [("start", &before), ("end", &after)] {
        report.set(&format!("{name}_busy_dags_at_{at}"), scraped.dags.0);
        report.set(&format!("{name}_quiet_dags_at_{at}"), scraped.dags.1);
    }
    let rounds = after.since(&before);
    if rounds.count < 1.0 || rounds.buckets.is_empty() {
        report.broken(vec![format!(
            "no rounds counted in the {name} window ({} before, {} after: did the master restart?)",
            before.count, after.count
        )]);
    }
    let fired = stress.db.count(&format!(
        "SELECT COUNT(DISTINCT dag_id) FROM runs WHERE kind = 'scheduled' AND {} >= {started}",
        stress.db.epoch("triggered_at")
    ));
    if fired < i64::try_from(busy).unwrap() {
        report.broken(vec![format!(
            "{fired} of the {busy} busy DAGs fired in the {name} window"
        )]);
    }
    rounds
}

/// Rounds from `schedr_master_tick_duration_seconds`: how many, their total time, and the
/// cumulative buckets.
struct Rounds {
    count: f64,
    sum: f64,
    buckets: Vec<(f64, f64)>,
    /// The busy and quiet DAGs at the scrape (`schedr_master_dags`).
    dags: (f64, f64),
}

impl Rounds {
    const NAME: &str = "schedr_master_tick_duration_seconds";

    fn scraped(metrics: &str) -> Self {
        let value = |suffix: &str| {
            let prefix = format!("{}{suffix} ", Self::NAME);
            metrics
                .lines()
                .find_map(|line| line.strip_prefix(&prefix)?.trim().parse().ok())
                .unwrap_or(0.0)
        };
        let bucket = format!("{}_bucket{{le=\"", Self::NAME);
        let buckets = metrics
            .lines()
            .filter_map(|line| {
                let (le, count) = line.strip_prefix(&bucket)?.split_once("\"}")?;
                let le = if le == "+Inf" {
                    f64::INFINITY
                } else {
                    le.parse().ok()?
                };
                Some((le, count.trim().parse().ok()?))
            })
            .collect();
        let dags = |state: &str| {
            let prefix = format!("schedr_master_dags{{state=\"{state}\"}} ");
            metrics
                .lines()
                .find_map(|line| line.strip_prefix(&prefix)?.trim().parse().ok())
                .unwrap_or(0.0)
        };
        Self {
            count: value("_count"),
            sum: value("_sum"),
            buckets,
            dags: (dags("busy"), dags("quiet")),
        }
    }

    fn since(&self, earlier: &Self) -> Self {
        Self {
            count: self.count - earlier.count,
            sum: self.sum - earlier.sum,
            buckets: self
                .buckets
                .iter()
                .zip(&earlier.buckets)
                .map(|(&(le, now), &(_, then))| (le, now - then))
                .collect(),
            dags: self.dags,
        }
    }

    fn mean(&self) -> f64 {
        self.sum / self.count.max(1.0)
    }

    /// The upper bound of the bucket holding the `q` quantile.
    fn quantile(&self, q: f64) -> f64 {
        self.buckets
            .iter()
            .find(|(_, count)| *count >= q * self.count)
            .map_or(0.0, |(le, _)| *le)
    }
}
