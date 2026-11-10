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

fn notification(channel: &str, url: Option<&str>) -> NewNotification {
    NewNotification {
        dag_id: "etl".into(),
        channel: channel.into(),
        kind: "webhook".into(),
        url: url.map(Into::into),
        event: "RUN_FAILED".into(),
        body: json!({"text": "DAG `etl`: run failed", "tasks": ["load"]}),
    }
}

#[tokio::test]
async fn a_queued_notification_is_due_at_once_retried_later_and_deleted() {
    let db = db().await;
    let now = ts("2026-11-09T10:00:00Z");
    db.queue_notifications(
        &[
            notification("ops-slack", None),
            notification("relay", Some("https://relay/x")),
        ],
        now,
    )
    .await
    .unwrap();
    let due = db.due_notifications(now, 10).await.unwrap();
    assert_eq!(due.len(), 2);
    assert_eq!(due[0].channel, "ops-slack");
    assert_eq!(due[0].url, None);
    assert_eq!(
        due[0].body,
        json!({"text": "DAG `etl`: run failed", "tasks": ["load"]})
    );
    assert_eq!(due[0].created_at, now);
    assert_eq!(due[1].url.as_deref(), Some("https://relay/x"));
    assert_eq!(db.due_notifications(now, 1).await.unwrap().len(), 1);

    db.retry_notification(&due[0].id, ts("2026-11-09T10:01:00Z"), "503")
        .await
        .unwrap();
    let still = db.due_notifications(now, 10).await.unwrap();
    assert_eq!(
        still.iter().map(|n| n.channel.as_str()).collect::<Vec<_>>(),
        ["relay"]
    );
    let later = db
        .due_notifications(ts("2026-11-09T10:01:00Z"), 10)
        .await
        .unwrap();
    assert_eq!(later.len(), 2);
    assert_eq!(later.iter().find(|n| n.id == due[0].id).unwrap().tries, 1);

    db.delete_notification(&due[1].id).await.unwrap();
    let left = db
        .due_notifications(ts("2026-11-09T11:00:00Z"), 10)
        .await
        .unwrap();
    assert_eq!(
        left.iter().map(|n| n.id.as_str()).collect::<Vec<_>>(),
        [due[0].id.as_str()]
    );
}

#[tokio::test]
async fn nothing_to_queue_writes_nothing() {
    let db = db().await;
    db.queue_notifications(&[], ts("2026-11-09T10:00:00Z"))
        .await
        .unwrap();
    assert_eq!(
        db.due_notifications(ts("2026-11-09T11:00:00Z"), 10)
            .await
            .unwrap(),
        []
    );
}
