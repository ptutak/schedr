use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;
use crate::dags::{DagChange, NewParse};
use crate::query::{in_tx, sql_text};

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

/// A database with DAG `sync` (tasks `extract`, `load`), its version's ID.
async fn with_dag() -> (Database, String) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let task = |name: &str| {
        json!({
            "id": name, "kind": "python",
            "settings": {
                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
                "queue": "default", "max_output_size": 1_048_576
            },
            "output_type": {"type": "none"}
        })
    };
    let dag = serde_json::from_value(json!({
        "produced_by": "1.0", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "none"}, "initial_since": null, "max_active_runs": 1,
        "tasks": [task("extract"), task("load"), task("t3"), task("t4"), task("t5")], "edges": []
    }))
    .unwrap();
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h1",
            schedr_version: "1.0.0",
            worker_id: "w1",
            parsed_at: ts("2026-11-01T00:00:00Z"),
            files: &[],
        })
        .await
        .unwrap();
    let change = DagChange::Define {
        dag: Box::new(dag),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts("2026-11-01T00:00:00Z"))
        .await
        .unwrap();
    let version = db.active_dags().await.unwrap()[0].version_id.clone();
    (db, version)
}

/// An ended attempt: its run planned at minute `run` of 2026-11-14 10:00, ending a minute
/// later plus `try_number`.
struct Seeded<'a> {
    run: u32,
    /// Its run planned this many days earlier, its attempt ending all the same (a backfill).
    shift_days: i64,
    task: &'a str,
    map_index: i32,
    try_number: u32,
    status: &'a str,
    peak: Option<i64>,
}

fn uuid(n: u32, kind: u32) -> String {
    format!("00000000-0000-4000-8000-{kind:04}{n:08}")
}

async fn seed(db: &Database, version: &str, attempts: &[Seeded<'_>]) {
    seed_rows(db, version, attempts).await.unwrap();
}

async fn seed_rows(db: &Database, version: &str, attempts: &[Seeded<'_>]) -> Result<(), DbError> {
    const RUN: &str = "INSERT INTO runs (id, dag_id, dag_version_id, kind, state, planned_for, \
        since, triggered_at, triggered_by, ended_at) VALUES ($1{uuid}, 'sync', $2{uuid}, \
        'manual', 'success', $3{ts}, $3{ts}, $3{ts}, 'anna', $4{ts}) \
        ON CONFLICT (id) DO UPDATE SET ended_at = EXCLUDED.ended_at";
    const INSTANCE: &str = "INSERT INTO task_instances (id, run_id, task_id, map_index, state, \
        updated_at) VALUES ($1{uuid}, $2{uuid}, $3, $4, 'success', $5{ts}) \
        ON CONFLICT (id) DO NOTHING";
    const ATTEMPT: &str = "INSERT INTO attempts (id, task_instance_id, try_number, status, \
        peak_memory_bytes, assigned_at, ended_at) \
        VALUES ($1{uuid}, $2{uuid}, $3, $4, $5, $6{ts}, $7{ts})";
    for (n, a) in (0..).zip(attempts) {
        let at = ts("2026-11-14T10:00:00Z")
            .checked_add(jiff::SignedDuration::from_mins(i64::from(a.run)))
            .unwrap();
        let ended = at
            .checked_add(jiff::SignedDuration::from_mins(1 + i64::from(a.try_number)))
            .unwrap();
        let planned = at
            .checked_sub(jiff::SignedDuration::from_hours(24 * a.shift_days))
            .unwrap();
        // The run ends a minute after its last attempt.
        let run_ended = ended
            .checked_add(jiff::SignedDuration::from_mins(1))
            .unwrap();
        let run_id = uuid(a.run, 1);
        let task_n = u32::from(a.task == "load");
        let instance_id = uuid(
            a.run * 10_000 + task_n * 1000 + a.map_index.unsigned_abs(),
            2,
        );
        let () = in_tx!(db, |tx, backend| {
            sqlx::query(sql_text(backend, RUN))
                .bind(&run_id)
                .bind(version)
                .bind(db_time(planned))
                .bind(db_time(run_ended))
                .execute(&mut *tx)
                .await?;
            sqlx::query(sql_text(backend, INSTANCE))
                .bind(&instance_id)
                .bind(&run_id)
                .bind(a.task)
                .bind(a.map_index)
                .bind(db_time(planned))
                .execute(&mut *tx)
                .await?;
            sqlx::query(sql_text(backend, ATTEMPT))
                .bind(uuid(n, 3))
                .bind(&instance_id)
                .bind(i64::from(a.try_number))
                .bind(a.status)
                .bind(a.peak)
                .bind(db_time(planned))
                .bind(db_time(ended))
                .execute(&mut *tx)
                .await?;
        });
    }
    Ok(())
}

fn ended(run: u32, task: &str, peak: i64) -> Seeded<'_> {
    Seeded {
        run,
        shift_days: 0,
        task,
        map_index: -1,
        try_number: 1,
        status: "success",
        peak: Some(peak),
    }
}

#[tokio::test]
async fn a_tasks_last_attempts_are_read_oldest_first_across_runs_and_instances() {
    let (db, version) = with_dag().await;
    seed(
        &db,
        &version,
        &[
            ended(0, "extract", 100),
            Seeded {
                status: "oom_killed",
                ..ended(1, "extract", 900)
            },
            Seeded {
                try_number: 2,
                ..ended(1, "extract", 300)
            },
            ended(1, "load", 5),
            Seeded {
                map_index: 0,
                peak: None,
                status: "lost",
                ..ended(2, "extract", 0)
            },
            Seeded {
                map_index: 1,
                ..ended(2, "extract", 200)
            },
        ],
    )
    .await;
    let peaks = db.task_attempt_peaks("sync", "extract", 30).await.unwrap();
    let seen: Vec<_> = peaks
        .iter()
        .map(|p| {
            (
                p.map_index,
                p.try_number,
                p.status.as_str(),
                p.peak_memory_bytes,
            )
        })
        .collect();
    assert_eq!(
        seen,
        [
            (None, 1, "success", Some(100)),
            (None, 1, "oom_killed", Some(900)),
            (None, 2, "success", Some(300)),
            (Some(0), 1, "lost", None),
            (Some(1), 1, "success", Some(200)),
        ]
    );
    assert_eq!(peaks[1].run_id, uuid(1, 1));
    assert_eq!(peaks[1].ended_at, ts("2026-11-14T10:03:00Z"));
    // The newest `count`, still oldest first.
    let last = db.task_attempt_peaks("sync", "extract", 2).await.unwrap();
    assert_eq!(
        last.iter().map(|p| p.map_index).collect::<Vec<_>>(),
        [Some(0), Some(1)]
    );
    assert_eq!(
        db.task_attempt_peaks("other", "extract", 30).await.unwrap(),
        []
    );
}

#[tokio::test]
async fn a_dags_peaks_are_by_task_since_a_time_with_a_peak_and_capped() {
    let (db, version) = with_dag().await;
    let mut attempts: Vec<Seeded<'_>> = (0..1003)
        .map(|run| ended(run, "load", i64::from(run)))
        .collect();
    attempts.push(Seeded {
        status: "oom_killed",
        ..ended(5, "extract", 7)
    });
    attempts.push(Seeded {
        peak: None,
        ..ended(6, "extract", 0)
    });
    seed(&db, &version, &attempts).await;
    let tasks = db
        .dag_task_peaks("sync", ts("2026-11-14T10:00:00Z"))
        .await
        .unwrap();
    assert_eq!(tasks.len(), 2);
    assert_eq!(
        (
            tasks[0].task_id.as_str(),
            tasks[0].peaks.as_slice(),
            tasks[0].oom_killed
        ),
        ("extract", [7].as_slice(), 1)
    );
    let load = &tasks[1];
    assert_eq!(load.peaks.len(), 1000);
    // The newest thousand: the first three runs' left out.
    assert_eq!(load.peaks.iter().min(), Some(&3));
    // Only attempts ended since: run 0's ended at 10:02.
    let later = db
        .dag_task_peaks("sync", ts("2026-11-14T10:08:00Z"))
        .await
        .unwrap();
    assert_eq!(later.len(), 1);
    assert_eq!(later[0].task_id, "load");
}

#[tokio::test]
async fn the_last_attempts_are_the_last_to_end_whatever_their_runs_order() {
    // Codex's review of M5.1k: an older run's attempt ending last is the newest.
    let (db, version) = with_dag().await;
    seed(
        &db,
        &version,
        &[
            Seeded {
                try_number: 9,
                ..ended(0, "extract", 1)
            },
            ended(1, "extract", 2),
            ended(2, "extract", 3),
        ],
    )
    .await;
    let peaks = db.task_attempt_peaks("sync", "extract", 2).await.unwrap();
    assert_eq!(
        peaks
            .iter()
            .map(|p| p.peak_memory_bytes)
            .collect::<Vec<_>>(),
        [Some(3), Some(1)]
    );
}

#[tokio::test]
async fn a_backfill_of_old_moments_ending_now_counts_in_the_dags_peaks() {
    // Codex's review of M5.1k: its run planned 40 days ago, its attempt ending in the range.
    let (db, version) = with_dag().await;
    seed(
        &db,
        &version,
        &[
            ended(0, "extract", 100),
            Seeded {
                shift_days: 40,
                status: "oom_killed",
                ..ended(1, "extract", 900)
            },
        ],
    )
    .await;
    let tasks = db
        .dag_task_peaks("sync", ts("2026-11-14T00:00:00Z"))
        .await
        .unwrap();
    let mut peaks = tasks[0].peaks.clone();
    peaks.sort_unstable();
    assert_eq!((peaks, tasks[0].oom_killed), (vec![100, 900], 1));
}
