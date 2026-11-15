use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;
use crate::dags::{DagChange, NewParse};
use crate::query::{in_tx, sql_text};

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

/// A database with DAG `sync` (task `extract`) and one run of it, its run's ID.
async fn with_run() -> (Database, String) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let dag = serde_json::from_value(json!({
        "produced_by": "1.0", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "none"}, "initial_since": null, "max_active_runs": 1,
        "tasks": [{
            "id": "extract", "kind": "python",
            "settings": {
                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
                "queue": "default", "max_output_size": 1_048_576
            },
            "output_type": {"type": "none"}
        }],
        "edges": []
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
    let run = "00000000-0000-4000-8000-000100000001".to_owned();
    insert_run(&db, &run, &version).await.unwrap();
    (db, run)
}

async fn insert_run(db: &Database, run: &str, version: &str) -> Result<(), DbError> {
    const RUN: &str = "INSERT INTO runs (id, dag_id, dag_version_id, kind, state, planned_for, \
        since, triggered_at, triggered_by) VALUES ($1{uuid}, 'sync', $2{uuid}, 'manual', \
        'running', $3{ts}, $3{ts}, $3{ts}, 'anna')";
    let () = in_tx!(db, |tx, backend| {
        sqlx::query(sql_text(backend, RUN))
            .bind(run)
            .bind(version)
            .bind(db_time(ts("2026-11-14T10:00:00Z")))
            .execute(&mut *tx)
            .await?;
    });
    Ok(())
}

/// An attempt of its own instance: assigned at `assigned`, started `latency_ms` after (none:
/// never started), ended at `ended` with `status` (none: still going).
struct Seeded<'a> {
    assigned: &'a str,
    latency_ms: Option<i64>,
    ended: Option<(&'a str, &'a str)>,
}

async fn seed(db: &Database, run: &str, attempts: &[Seeded<'_>]) {
    seed_rows(db, run, attempts).await.unwrap();
}

async fn seed_rows(db: &Database, run: &str, attempts: &[Seeded<'_>]) -> Result<(), DbError> {
    const INSTANCE: &str = "INSERT INTO task_instances (id, run_id, task_id, map_index, state, \
        updated_at) VALUES ($1{uuid}, $2{uuid}, 'extract', $3, 'running', $4{ts})";
    const ATTEMPT: &str = "INSERT INTO attempts (id, task_instance_id, try_number, status, \
        assigned_at, started_at, ended_at) \
        VALUES ($1{uuid}, $2{uuid}, 1, $3, $4{ts}, $5{ts}, $6{ts})";
    for (n, a) in (0_u32..).zip(attempts) {
        let instance = format!("00000000-0000-4000-8000-0002{n:08}");
        let assigned = ts(a.assigned);
        let started = a.latency_ms.map(|ms| {
            assigned
                .checked_add(jiff::SignedDuration::from_millis(ms))
                .unwrap()
        });
        let () = in_tx!(db, |tx, backend| {
            sqlx::query(sql_text(backend, INSTANCE))
                .bind(&instance)
                .bind(run)
                .bind(i64::from(n))
                .bind(db_time(assigned))
                .execute(&mut *tx)
                .await?;
            sqlx::query(sql_text(backend, ATTEMPT))
                .bind(format!("00000000-0000-4000-8000-0003{n:08}"))
                .bind(&instance)
                .bind(a.ended.map(|(_, status)| status))
                .bind(db_time(assigned))
                .bind(started.map(db_time))
                .bind(a.ended.map(|(at, _)| db_time(ts(at))))
                .execute(&mut *tx)
                .await?;
        });
    }
    Ok(())
}

#[tokio::test]
async fn the_attempts_ended_are_counted_by_bucket_and_status() {
    let (db, run) = with_run().await;
    let ended = |at, status| Seeded {
        assigned: "2026-11-14T09:00:00Z",
        latency_ms: Some(10),
        ended: Some((at, status)),
    };
    seed(
        &db,
        &run,
        &[
            ended("2026-11-14T10:00:10Z", "success"),
            ended("2026-11-14T10:00:50Z", "success"),
            ended("2026-11-14T10:00:30Z", "failed"),
            // In the minute's last half-millisecond: still in it (SQLite's `strftime` rounds at
            // the millisecond; the bucket reads the stored whole seconds).
            ended("2026-11-14T10:00:59.999600Z", "success"),
            ended("2026-11-14T10:01:00Z", "oom_killed"),
            // Before the range, at its end, and still going: left out.
            ended("2026-11-14T09:59:59Z", "success"),
            ended("2026-11-14T10:02:00Z", "success"),
            Seeded {
                assigned: "2026-11-14T10:00:00Z",
                latency_ms: Some(10),
                ended: None,
            },
        ],
    )
    .await;
    let counts = db
        .attempts_ended((ts("2026-11-14T10:00:00Z"), ts("2026-11-14T10:02:00Z")), 60)
        .await
        .unwrap();
    let row = |at, status: &str, count| EndedCount {
        at: ts(at),
        status: status.to_owned(),
        count,
    };
    assert_eq!(
        counts,
        [
            row("2026-11-14T10:00:00Z", "failed", 1),
            row("2026-11-14T10:00:00Z", "success", 3),
            row("2026-11-14T10:01:00Z", "oom_killed", 1),
        ]
    );
}

#[tokio::test]
async fn the_start_latency_is_each_buckets_exact_p50_and_p95() {
    let (db, run) = with_run().await;
    // Twenty in the first minute, 100 ms to 2 s: the nearest ranks are the 10th and the 19th.
    let mut attempts: Vec<Seeded<'_>> = (1..=20)
        .map(|i| Seeded {
            assigned: "2026-11-14T10:00:05Z",
            latency_ms: Some(i * 100),
            ended: None,
        })
        .collect();
    // One in the next: itself both; one started before its assignment (clocks apart): 0.
    attempts.push(Seeded {
        assigned: "2026-11-14T10:01:05Z",
        latency_ms: Some(300),
        ended: None,
    });
    attempts.push(Seeded {
        assigned: "2026-11-14T10:02:30Z",
        latency_ms: Some(-40),
        ended: None,
    });
    // Never started: no latency.
    attempts.push(Seeded {
        assigned: "2026-11-14T10:01:05Z",
        latency_ms: None,
        ended: None,
    });
    seed(&db, &run, &attempts).await;
    let latency = db
        .start_latency((ts("2026-11-14T10:00:00Z"), ts("2026-11-14T10:03:00Z")), 60)
        .await
        .unwrap();
    let bucket = |at, started, p50_ms, p95_ms| StartLatency {
        at: ts(at),
        started,
        p50_ms,
        p95_ms,
    };
    assert_eq!(
        latency,
        [
            bucket("2026-11-14T10:00:00Z", 20, 1000, 1900),
            bucket("2026-11-14T10:01:00Z", 1, 300, 300),
            bucket("2026-11-14T10:02:00Z", 1, 0, 0),
        ]
    );
}

#[tokio::test]
async fn each_read_goes_through_its_index() {
    // Decision 494: a range's reads stay bounded on a long history (partial indexes, so the
    // rounds' reads of unstarted attempts keep theirs).
    let (db, _) = with_run().await;
    let crate::Pool::Sqlite(pool) = db.pool() else {
        unreachable!("the tests' database is SQLite")
    };
    for (query, index) in [
        (ended_query(Backend::Sqlite), "attempts_ended"),
        (latency_query(Backend::Sqlite), "attempts_started"),
    ] {
        let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "EXPLAIN QUERY PLAN {}",
            query.replace("{ts}", "")
        )))
        .bind("2026-11-14T10:00:00.000000Z")
        .bind("2026-11-14T11:00:00.000000Z")
        .bind(60_i64)
        .fetch_all(pool)
        .await
        .unwrap();
        let plan: Vec<String> = plan.into_iter().map(|row| row.3).collect();
        assert!(
            plan.iter()
                .any(|step| step.contains(&format!("USING INDEX {index}"))),
            "{plan:?}"
        );
    }
}
