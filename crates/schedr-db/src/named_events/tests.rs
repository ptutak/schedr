use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

async fn db() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db
}

fn event<'a>(key: &'a str, data: &'a serde_json::Value) -> NewNamedEvent<'a> {
    NewNamedEvent {
        name: "paid",
        key,
        data,
        at: ts("2026-11-08T10:00:00Z"),
    }
}

#[tokio::test]
async fn every_post_is_an_event_a_retried_one_is_the_same() {
    let db = db().await;
    let data = json!({"amount": 40});
    let first = db
        .post_named_event(
            &event("o1", &data),
            "anna",
            Some("k1"),
            ts("2026-11-08T10:00:01Z"),
        )
        .await
        .unwrap();
    let Posted::New(first) = first else {
        panic!("{first:?}")
    };
    // The same key again: the answer was lost, it is the same event.
    let again = db
        .post_named_event(
            &event("o1", &data),
            "anna",
            Some("k1"),
            ts("2026-11-08T10:00:02Z"),
        )
        .await
        .unwrap();
    assert_eq!(again, Posted::Again(first.clone()));
    let other = json!({"amount": 41});
    let taken = db
        .post_named_event(
            &event("o1", &other),
            "anna",
            Some("k1"),
            ts("2026-11-08T10:00:03Z"),
        )
        .await
        .unwrap();
    assert_eq!(taken, Posted::KeyTaken);
    // A repeat of the name and key without a request key: a new event.
    let repeat = db
        .post_named_event(
            &event("o1", &data),
            "anna",
            None,
            ts("2026-11-08T10:00:04Z"),
        )
        .await
        .unwrap();
    assert!(
        matches!(repeat, Posted::New(ref id) if *id != first),
        "{repeat:?}"
    );
    assert_eq!(db.named_events(Some("paid"), 10).await.unwrap().len(), 2);
}

#[tokio::test]
async fn a_waiting_step_gets_the_first_since_its_run_started_and_a_trigger_reads_after_a_cursor() {
    let db = db().await;
    let data = json!(null);
    for (key, at) in [
        ("o1", "2026-11-08T09:00:00Z"),
        ("o1", "2026-11-08T10:00:05Z"),
        ("o2", "2026-11-08T10:00:06Z"),
    ] {
        db.post_named_event(&event(key, &data), "anna", None, ts(at))
            .await
            .unwrap();
    }
    let since = ts("2026-11-08T10:00:00Z");
    let found = db
        .first_named_event("paid", Some("o1"), since, None)
        .await
        .unwrap();
    assert_eq!(
        found.unwrap().posted_at,
        ts("2026-11-08T10:00:05Z"),
        "not the one before"
    );
    assert!(
        db.first_named_event("paid", Some("o3"), since, None)
            .await
            .unwrap()
            .is_none()
    );
    let any = db
        .first_named_event("paid", None, since, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(any.key, "o1");
    // No DAG has seen a key, nor passed an event, yet: both read all from `since`.
    let unseen = db
        .named_events_unseen("paid", "sync", since, 10)
        .await
        .unwrap();
    assert_eq!(unseen.len(), 2);
    let unpassed = db
        .named_event_unpassed("paid", "sync", since)
        .await
        .unwrap();
    assert_eq!(unpassed.map(|e| e.key), Some("o1".to_owned()));
}

#[tokio::test]
async fn what_a_trigger_passed_behind_its_window_is_forgotten() {
    // B186: `trigger_passed` was never pruned.
    let db = db().await;
    let data = json!(null);
    let mut ids = Vec::new();
    for (key, at) in [
        ("o1", "2026-11-08T09:00:00Z"),
        ("o2", "2026-11-08T10:00:00Z"),
    ] {
        let Posted::New(id) = db
            .post_named_event(&event(key, &data), "anna", None, ts(at))
            .await
            .unwrap()
        else {
            panic!("new");
        };
        ids.push(id);
    }
    let crate::Pool::Sqlite(pool) = db.pool() else {
        unreachable!("SQLite")
    };
    for id in &ids {
        sqlx::query("INSERT INTO trigger_passed (dag_id, item) VALUES ('sync', ?)")
            .bind(id)
            .execute(pool)
            .await
            .unwrap();
    }
    let forgotten = db
        .forget_passed_before("sync", "paid", ts("2026-11-08T09:30:00Z"))
        .await
        .unwrap();
    assert_eq!(forgotten, 1, "the one behind it only");
    let left: Vec<String> = sqlx::query_scalar("SELECT item FROM trigger_passed")
        .fetch_all(pool)
        .await
        .unwrap();
    assert_eq!(left, [ids[1].clone()]);
}
