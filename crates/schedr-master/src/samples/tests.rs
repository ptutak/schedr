use pretty_assertions::assert_eq;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn usage(resident_bytes: u64, cpu_seconds: f64) -> NodeUsage {
    NodeUsage {
        resident_bytes,
        cpu_seconds,
        container_bytes: None,
        container_limit_bytes: None,
    }
}

fn source(process: &str, role: &'static str, node: &str, cpu: f64) -> Source {
    Source {
        process_id: process.to_owned(),
        role,
        node_id: node.to_owned(),
        usage: usage(100, cpu),
        at: "2026-11-14T10:00:05Z".parse().unwrap(),
        parts: (role == "worker").then(|| schedr_model::usage::WorkerParts {
            fork_servers_bytes: 1,
            watcher_hosts_bytes: 2,
            tasks_bytes: 3,
            largest_tasks: Vec::new(),
        }),
    }
}

#[test]
fn a_standalone_is_one_sample_with_its_three_roles() {
    // The side note of 2026-11-14: one process, never charted three times.
    let mut sampler = Sampler::default();
    let webserver = BTreeMap::from([("webserver".to_owned(), "p1".to_owned())]);
    let at = ts("2026-11-14T10:00:00Z");
    let taken = sampler.samples(
        vec![
            source("p1", "master", "m-a", 1.0),
            source("p1", "worker", "standalone", 1.0),
            source("p2", "worker", "w2", 5.0),
        ],
        at,
        Some(("p1", &webserver)),
    );
    assert_eq!(taken.len(), 2);
    assert_eq!(
        taken[0].roles,
        BTreeMap::from([
            ("master".to_owned(), "m-a".to_owned()),
            ("webserver".to_owned(), "p1".to_owned()),
            ("worker".to_owned(), "standalone".to_owned()),
        ])
    );
    assert!(
        taken[0].parts.is_some(),
        "the worker's figures: they have its parts"
    );
    assert_eq!(taken[1].roles.len(), 1);
    assert_eq!(taken[0].cpu_avg, None, "its first: nothing to compare with");
    // Decision 491: its whole memory, the worker's parts with it (100 + 1 + 2 + 3).
    assert_eq!(
        (
            taken[0].resident_avg,
            taken[0].memory_avg,
            taken[0].memory_peak
        ),
        (100, 106, 106)
    );
}

#[test]
fn the_cores_are_the_cpu_time_used_over_the_time_between_measures() {
    let mut sampler = Sampler::default();
    let mut take = |cpu: f64, measured: &str| {
        let measured = ts(measured);
        sampler
            .sample(
                ("p1".to_owned(), BTreeMap::new()),
                &usage(1, cpu),
                None,
                (floor(measured, MINUTE), measured),
            )
            .cpu_avg
    };
    assert_eq!(take(10.0, "2026-11-14T10:00:00Z"), None, "its first");
    assert_eq!(take(40.0, "2026-11-14T10:01:00Z"), Some(0.5));
    // The same measure again (nothing stored since, Grok's review of M5.1i): the cores it
    // gave, and the next one counts from it, not from a zero.
    assert_eq!(take(40.0, "2026-11-14T10:01:00Z"), Some(0.5));
    assert_eq!(take(160.0, "2026-11-14T10:03:00Z"), Some(1.0));
    // Over the time between the measures, not their minutes (10:03 and 10:05 are two apart).
    assert_eq!(take(221.0, "2026-11-14T10:04:01Z"), Some(1.0));
    // Restarted under the same ID (its CPU time went back): not known.
    assert_eq!(take(2.0, "2026-11-14T10:05:00Z"), None);
}

#[test]
fn a_rollup_takes_every_role_its_window_saw_and_rounds_its_averages() {
    // Grok's review of M5.1i: MAX of the roles' text kept one set.
    let at = ts("2026-11-14T10:00:00Z");
    let minute = |roles: &[(&str, &str)], resident: u64, cpu: Option<f64>| Sample {
        process_id: "p1".to_owned(),
        step_s: MINUTE,
        at,
        roles: roles
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        resident_avg: resident,
        resident_peak: resident,
        memory_avg: resident,
        memory_peak: resident,
        minutes: 1,
        cpu_minutes: u32::from(cpu.is_some()),
        container_minutes: 0,
        covered: 1,
        cpu_seconds: Some(1.0),
        cpu_avg: cpu,
        cpu_peak: cpu,
        container_avg: None,
        container_peak: None,
        container_limit: Some(1 << 30),
        parts: None,
    };
    let rolled = rolled_up(
        &[
            minute(&[("master", "m-a")], 100, None),
            Sample {
                // Decision 491: the whole memory, a worker's parts with it.
                memory_avg: 201 + 1000,
                memory_peak: 201 + 1000,
                parts: Some(WorkerParts {
                    fork_servers_bytes: 0,
                    watcher_hosts_bytes: 0,
                    tasks_bytes: 1000,
                    largest_tasks: vec![schedr_model::usage::TaskMemory {
                        attempt_id: "a1".to_owned(),
                        dag_id: "etl".to_owned(),
                        run_id: "r1".to_owned(),
                        task_id: "load".to_owned(),
                        map_index: None,
                        bytes: 1000,
                    }],
                }),
                ..minute(
                    &[("master", "m-a"), ("worker", "standalone")],
                    201,
                    Some(0.5),
                )
            },
        ],
        at,
    );
    // The whole memory averaged and its peak; the parts averaged over every minute, 0 where it
    // ran no worker, so they and the process add up to its whole memory (Codex's review of
    // M5.1j); the largest tasks left out; the minutes it covers.
    assert_eq!((rolled[0].memory_avg, rolled[0].memory_peak), (651, 1201));
    assert_eq!(
        rolled[0].parts,
        Some(WorkerParts {
            fork_servers_bytes: 0,
            watcher_hosts_bytes: 0,
            tasks_bytes: 500,
            largest_tasks: Vec::new(),
        })
    );
    assert_eq!(rolled[0].minutes, 2);
    assert_eq!(rolled.len(), 1);
    assert_eq!(
        rolled[0]
            .roles
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["master", "worker"]
    );
    assert_eq!(
        (
            rolled[0].step_s,
            rolled[0].resident_avg,
            rolled[0].resident_peak,
            rolled[0].cpu_avg,
            rolled[0].container_avg,
            rolled[0].container_limit
        ),
        (ROLLUP, 151, 201, Some(0.5), None, Some(1 << 30))
    );
}

#[tokio::test]
async fn a_pass_makes_three_statements_and_seven_when_a_window_closed() {
    // Decision 464's bound: two reads and an insert a minute; the window's read and insert and
    // two deletes once one closes.
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let config = serde_json::json!({});
    let node = usage(64, 1.0);
    let heartbeat = async |at: &str| {
        db.heartbeat_master(
            &schedr_db::nodes::MasterNode {
                master_id: "m-a",
                process_id: "p1",
                host: "h",
                schedr_version: "1.1.0",
                config: &config,
                started_at: ts("2026-11-14T09:00:00Z"),
                problems: &[],
                usage: Some(&node),
            },
            ts(at),
        )
        .await
        .unwrap();
    };
    let settings = SampleSettings::default();
    let mut sampler = Sampler::default();
    heartbeat("2026-11-14T10:00:05Z").await;
    let first = sampler
        .pass(&db, &settings, ts("2026-11-14T10:00:10Z"), None)
        .await
        .unwrap();
    // Its first: the oldest minute read too; no window closed after it.
    assert_eq!((first.written, first.statements), (1, 4));
    heartbeat("2026-11-14T10:01:05Z").await;
    let next = sampler
        .pass(&db, &settings, ts("2026-11-14T10:01:10Z"), None)
        .await
        .unwrap();
    assert_eq!((next.written, next.statements), (1, 3));
    // The window closed at 10:15 waits out the grace (a sample stored late is in it): this
    // pass rolls only the one before, empty (its read, no insert, and the deletes).
    heartbeat("2026-11-14T10:15:05Z").await;
    let early = sampler
        .pass(&db, &settings, ts("2026-11-14T10:15:10Z"), None)
        .await
        .unwrap();
    assert_eq!((early.statements, early.rolled), (6, 0));
    heartbeat("2026-11-14T10:17:05Z").await;
    let closing = sampler
        .pass(&db, &settings, ts("2026-11-14T10:17:10Z"), None)
        .await
        .unwrap();
    assert_eq!((closing.statements, closing.rolled), (7, 1));
    let minutes = db
        .samples(
            MINUTE,
            ts("2026-11-14T10:00:00Z"),
            ts("2026-11-14T11:00:00Z"),
        )
        .await
        .unwrap();
    assert_eq!(
        minutes.iter().map(|s| s.at).collect::<Vec<_>>(),
        [
            ts("2026-11-14T10:00:00Z"),
            ts("2026-11-14T10:01:00Z"),
            ts("2026-11-14T10:15:00Z"),
            ts("2026-11-14T10:17:00Z")
        ],
        "at whole minutes"
    );
    // Nobody heard from lately: two reads, nothing written.
    let idle = sampler
        .pass(&db, &settings, ts("2026-11-14T10:20:30Z"), None)
        .await
        .unwrap();
    assert_eq!((idle.written, idle.statements), (0, 2));
}

#[tokio::test]
async fn a_new_leader_rolls_every_window_the_last_one_missed_before_deleting() {
    // Grok's review of M5.1i: only the window just closed was rolled, and the minutes of the
    // ones missed were deleted later unrolled.
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let minute = |at: &str| Sample {
        process_id: "p1".to_owned(),
        step_s: MINUTE,
        at: ts(at),
        roles: BTreeMap::from([("webserver".to_owned(), "p1".to_owned())]),
        resident_avg: 10,
        resident_peak: 10,
        memory_avg: 10,
        memory_peak: 10,
        minutes: 1,
        cpu_minutes: 1,
        container_minutes: 1,
        covered: 1,
        cpu_seconds: None,
        cpu_avg: None,
        cpu_peak: None,
        container_avg: None,
        container_peak: None,
        container_limit: None,
        parts: None,
    };
    db.insert_samples(&[
        minute("2026-11-14T10:00:00Z"),
        minute("2026-11-14T10:14:00Z"),
        minute("2026-11-14T10:15:00Z"),
        minute("2026-11-14T10:29:00Z"),
    ])
    .await
    .unwrap();
    let settings = SampleSettings {
        minutes_kept: Duration::from_mins(20),
        ..SampleSettings::default()
    };
    let mut sampler = Sampler::default();
    sampler
        .pass(&db, &settings, ts("2026-11-14T10:32:00Z"), None)
        .await
        .unwrap();
    let rolled = db
        .samples(
            ROLLUP,
            ts("2026-11-14T00:00:00Z"),
            ts("2026-11-15T00:00:00Z"),
        )
        .await
        .unwrap();
    assert_eq!(
        rolled.iter().map(|r| r.at).collect::<Vec<_>>(),
        [ts("2026-11-14T10:00:00Z"), ts("2026-11-14T10:15:00Z")],
        "both rolled before the old minutes went"
    );
    let left = db
        .samples(
            MINUTE,
            ts("2026-11-14T00:00:00Z"),
            ts("2026-11-15T00:00:00Z"),
        )
        .await
        .unwrap();
    assert_eq!(
        left.iter().map(|m| m.at).collect::<Vec<_>>(),
        [
            ts("2026-11-14T10:14:00Z"),
            ts("2026-11-14T10:15:00Z"),
            ts("2026-11-14T10:29:00Z")
        ],
        "10:00 past the 20 minutes kept, after its window was rolled"
    );
    // The next pass, a window and its grace later: only the new one.
    let next = sampler
        .pass(&db, &settings, ts("2026-11-14T10:47:00Z"), None)
        .await
        .unwrap();
    assert_eq!(
        next.statements,
        2 + 1 + 2,
        "two reads, the window's read (empty), two deletes"
    );
}

#[tokio::test]
async fn a_failed_delete_is_done_at_the_next_pass() {
    // Grok's review of M5.1i's fixes: it waited for the next window to close.
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let old = Sample {
        process_id: "p1".to_owned(),
        step_s: MINUTE,
        at: ts("2026-11-14T10:00:00Z"),
        roles: BTreeMap::new(),
        resident_avg: 1,
        resident_peak: 1,
        memory_avg: 1,
        memory_peak: 1,
        minutes: 1,
        cpu_minutes: 1,
        container_minutes: 1,
        covered: 1,
        cpu_seconds: None,
        cpu_avg: None,
        cpu_peak: None,
        container_avg: None,
        container_peak: None,
        container_limit: None,
        parts: None,
    };
    db.insert_samples(&[old]).await.unwrap();
    let settings = SampleSettings {
        minutes_kept: Duration::from_mins(20),
        ..SampleSettings::default()
    };
    let mut sampler = Sampler::default();
    // The rollup's insert is the first write, the minute delete the second.
    db.fail_write(2);
    assert!(
        sampler
            .pass(&db, &settings, ts("2026-11-14T10:31:00Z"), None)
            .await
            .is_err()
    );
    sampler
        .pass(&db, &settings, ts("2026-11-14T10:32:00Z"), None)
        .await
        .unwrap();
    assert_eq!(db.oldest_minute_sample().await.unwrap(), None);
}

#[tokio::test]
async fn a_master_is_sampled_while_heard_within_three_of_its_heartbeats() {
    // Decision 493: the window follows `SCHEDR_HIDDEN__MASTER__NODE_HEARTBEAT`; a fixed 30 s
    // dropped a master writing its row every minute.
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let config = serde_json::json!({});
    let node = usage(64, 1.0);
    db.heartbeat_master(
        &schedr_db::nodes::MasterNode {
            master_id: "m-a",
            process_id: "p1",
            host: "h",
            schedr_version: "1.1.0",
            config: &config,
            started_at: ts("2026-11-14T09:00:00Z"),
            problems: &[],
            usage: Some(&node),
        },
        ts("2026-11-14T10:00:00Z"),
    )
    .await
    .unwrap();
    let at = ts("2026-11-14T10:00:45Z");
    let every_ten = SampleSettings::default();
    let gone = Sampler::default()
        .pass(&db, &every_ten, at, None)
        .await
        .unwrap();
    assert_eq!(gone.written, 0, "45 s is past three heartbeats of 10 s");
    let every_minute = SampleSettings {
        node_heartbeat: Duration::from_mins(1),
        ..SampleSettings::default()
    };
    let heard = Sampler::default()
        .pass(&db, &every_minute, at, None)
        .await
        .unwrap();
    assert_eq!(
        heard.written, 1,
        "45 s is within three heartbeats of a minute"
    );
}
