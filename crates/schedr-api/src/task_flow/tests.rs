use std::cell::Cell;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pretty_assertions::assert_eq;
use schedr_db::Database;
use schedr_db::dags::{DagChange, NewParse};
use serde_json::json;
use tower::ServiceExt;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

/// A database with DAG `sync` and one run of it, its run's ID.
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
    execute(
        &db,
        format!(
            "INSERT INTO runs (id, dag_id, dag_version_id, kind, state, planned_for, since, \
             triggered_at, triggered_by) VALUES ('{run}', 'sync', '{version}', 'manual', \
             'running', '2026-11-14T00:00:00.000000Z', '2026-11-14T00:00:00.000000Z', \
             '2026-11-14T00:00:00.000000Z', 'anna')"
        ),
    )
    .await;
    (db, run)
}

async fn execute(db: &Database, statement: String) {
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
        unreachable!()
    };
    sqlx::query(sqlx::AssertSqlSafe(statement))
        .execute(pool)
        .await
        .unwrap();
}

/// A stored timestamp: `2026-11-14T10:00:05.000000Z`.
fn stored(at: Timestamp) -> String {
    at.strftime("%Y-%m-%dT%H:%M:%S.%6fZ").to_string()
}

/// Attempt `n` (its own instance): assigned at `assigned`, started `latency_ms` after, ended
/// at `ended` with `status` (none: still going).
async fn attempt(
    db: &Database,
    run: &str,
    n: u32,
    (assigned, latency_ms): (&str, i64),
    ended: Option<(&str, &str)>,
) {
    let instance = format!("00000000-0000-4000-8000-0002{n:08}");
    let assigned = ts(assigned);
    let started = assigned
        .checked_add(SignedDuration::from_millis(latency_ms))
        .unwrap();
    execute(
        db,
        format!(
            "INSERT INTO task_instances (id, run_id, task_id, map_index, state, updated_at) \
             VALUES ('{instance}', '{run}', 'extract', {n}, 'running', '{}')",
            stored(assigned)
        ),
    )
    .await;
    let (status, ended) = ended.map_or(("NULL".to_owned(), "NULL".to_owned()), |(at, s)| {
        (format!("'{s}'"), format!("'{}'", stored(ts(at))))
    });
    execute(
        db,
        format!(
            "INSERT INTO attempts (id, task_instance_id, try_number, status, assigned_at, \
             started_at, ended_at) VALUES ('00000000-0000-4000-8000-0003{n:08}', \
             '{instance}', 1, {status}, '{}', '{}', {ended})",
            stored(assigned),
            stored(started)
        ),
    )
    .await;
}

thread_local! {
    /// The tests' clock, in seconds: a test's own (each runs on its thread, its requests on
    /// it too: a current-thread runtime).
    static CLOCK: Cell<i64> = const { Cell::new(0) };
}

fn set_clock(seconds: i64) {
    CLOCK.with(|clock| clock.set(seconds));
}

fn clock() -> Timestamp {
    Timestamp::from_second(CLOCK.with(Cell::get)).unwrap()
}

fn app(db: &Database, flow: &Arc<FlowCache>) -> axum::Router {
    let mut state = ApiState::new(db.clone(), None);
    state.clock = clock;
    state.flow = Arc::clone(flow);
    crate::router(state)
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

#[test]
fn attempts_count_as_succeeded_failed_or_stopped() {
    assert_eq!(counts("success"), (1, 0, 0));
    for failed in ["failed", "timed_out", "oom_killed"] {
        assert_eq!(counts(failed), (0, 1, 0), "{failed}");
    }
    for stopped in ["cancelled", "interrupted", "lost", "assignment_expired"] {
        assert_eq!(counts(stopped), (0, 0, 1), "{stopped}");
    }
}

#[tokio::test]
async fn an_hour_has_every_minute_its_attempts_ended_and_their_start_latency() {
    set_clock(ts("2026-11-14T10:30:20Z").as_second());
    let (db, run) = with_run().await;
    for (n, (latency, ended)) in [
        (100, ("2026-11-14T10:10:05Z", "success")),
        (300, ("2026-11-14T10:10:15Z", "success")),
        (200, ("2026-11-14T10:10:25Z", "timed_out")),
        (900, ("2026-11-14T10:10:35Z", "cancelled")),
    ]
    .into_iter()
    .enumerate()
    {
        let n = u32::try_from(n).unwrap();
        attempt(&db, &run, n, ("2026-11-14T10:10:00Z", latency), Some(ended)).await;
    }
    // One still going, started in the current minute.
    attempt(&db, &run, 9, ("2026-11-14T10:30:00Z", 50), None).await;
    let (status, body) = get(&app(&db, &Arc::default()), "/api/v1/metrics/tasks?range=1h").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let list: TaskFlowList = serde_json::from_value(body).unwrap();
    assert_eq!(
        (list.range.as_str(), list.step_s, list.from, list.to),
        (
            "1h",
            60,
            ts("2026-11-14T09:31:00Z"),
            ts("2026-11-14T10:30:20Z")
        )
    );
    assert_eq!(list.buckets.len(), 60);
    assert_eq!(list.buckets[0].at, list.from);
    let at = |text| list.buckets.iter().find(|b| b.at == ts(text)).unwrap();
    assert_eq!(
        *at("2026-11-14T10:10:00Z"),
        TaskFlowView {
            at: ts("2026-11-14T10:10:00Z"),
            succeeded: 2,
            failed: 1,
            stopped: 1,
            started: 4,
            // Nearest rank of 100, 200, 300, 900: the 2nd and the 4th.
            start_p50_ms: Some(200),
            start_p95_ms: Some(900),
        }
    );
    let now = at("2026-11-14T10:30:00Z");
    assert_eq!(
        (now.succeeded, now.started, now.start_p50_ms),
        (0, 1, Some(50))
    );
    let quiet = at("2026-11-14T10:20:00Z");
    assert_eq!((quiet.started, quiet.start_p50_ms), (0, None));
    let (status, _) = get(&app(&db, &Arc::default()), "/api/v1/metrics/tasks?range=2h").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn the_kept_buckets_give_what_a_fresh_read_gives() {
    set_clock(ts("2026-11-14T12:00:20Z").as_second());
    let (db, run) = with_run().await;
    let kept = Arc::default();
    let mut n = 0;
    // Every 7 minutes for two hours, a range re-read as they come, from one cache.
    for minute in (0..120).step_by(7) {
        let at = ts("2026-11-14T12:00:00Z")
            .checked_add(SignedDuration::from_mins(minute))
            .unwrap();
        set_clock(at.as_second() + 50);
        let assigned = at.checked_add(SignedDuration::from_secs(5)).unwrap();
        let ended = at.checked_add(SignedDuration::from_secs(40)).unwrap();
        let status = if minute % 3 == 0 { "failed" } else { "success" };
        attempt(
            &db,
            &run,
            n,
            (&assigned.to_string(), minute * 10),
            Some((&ended.to_string(), status)),
        )
        .await;
        n += 1;
        for range in ["1h", "24h"] {
            let uri = format!("/api/v1/metrics/tasks?range={range}");
            let (_, warm) = get(&app(&db, &kept), &uri).await;
            let (_, fresh) = get(&app(&db, &Arc::default()), &uri).await;
            assert_eq!(warm, fresh, "{range} at {at}");
        }
    }
    let (_, day) = get(&app(&db, &kept), "/api/v1/metrics/tasks").await;
    let list: TaskFlowList = serde_json::from_value(day).unwrap();
    let ended: u64 = list
        .buckets
        .iter()
        .map(|b| b.succeeded + b.failed + b.stopped)
        .sum();
    assert_eq!(ended, u64::from(n));
}
