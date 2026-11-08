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
        .first_named_event("paid", Some("o1"), since)
        .await
        .unwrap();
    assert_eq!(
        found.unwrap().posted_at,
        ts("2026-11-08T10:00:05Z"),
        "not the one before"
    );
    assert!(
        db.first_named_event("paid", Some("o3"), since)
            .await
            .unwrap()
            .is_none()
    );
    let any = db
        .first_named_event("paid", None, since)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(any.key, "o1");
    let all = db
        .named_events_after("paid", since, None, 10)
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    let after = db
        .named_events_after("paid", since, Some((all[0].posted_at, &all[0].id)), 10)
        .await
        .unwrap();
    assert_eq!(
        after.iter().map(|e| e.key.as_str()).collect::<Vec<_>>(),
        ["o2"]
    );
}
