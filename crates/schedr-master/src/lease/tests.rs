use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pretty_assertions::assert_eq;

use super::*;

async fn file_db(dir: &tempfile::TempDir) -> Database {
    let url = format!("sqlite://{}", dir.path().join("lease.db").display());
    // Queries limited to 100 ms: a renewal leads for the period less that.
    let db = Database::connect_with_timeout(&url, Duration::from_millis(100))
        .await
        .unwrap();
    db.migrate().await.unwrap();
    db
}

#[tokio::test]
async fn a_second_master_waits_as_a_standby_and_takes_over_when_the_first_gives_up() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let period = Duration::from_millis(600);
    let first = Leadership::take(db.clone(), "a".to_owned(), period, || {}).await;
    assert!(first.leads(Instant::now()));
    assert_eq!(first.epoch(), 1);
    let waited = Arc::new(AtomicBool::new(false));
    let second = tokio::spawn({
        let (db, waited) = (db.clone(), waited.clone());
        async move {
            Leadership::take(db, "b".to_owned(), period, move || {
                waited.store(true, Ordering::SeqCst);
            })
            .await
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        waited.load(Ordering::SeqCst),
        "the second master is a standby"
    );
    assert!(!second.is_finished());
    first.release().await;
    let second = tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.epoch(), 2);
    assert_eq!(second.period(), period);
}

#[tokio::test]
async fn a_renewal_keeps_leading_and_a_takeover_is_seen() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let period = Duration::from_millis(300);
    let mut first = Leadership::take(db.clone(), "a".to_owned(), period, || {}).await;
    let now = Instant::now();
    assert_eq!(first.renew(now).await, Renewal::Held);
    // A renewal leads for the period less the query limit, by this master's own clock.
    assert!(first.leads(now + Duration::from_millis(150)));
    assert!(!first.leads(now + Duration::from_millis(200)));
    // It expires; another master takes it; this one's renewal sees the takeover.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(db.take_lease("b", period).await.unwrap(), Some(2));
    assert_eq!(first.renew(Instant::now()).await, Renewal::TakenOver);
    // Even once it could take it back, the counter moved on: still a takeover.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(first.renew(Instant::now()).await, Renewal::TakenOver);
}

#[tokio::test]
async fn a_master_taken_over_never_writes_itself_back_as_the_holder() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let period = Duration::from_millis(300);
    let mut first = Leadership::take(db.clone(), "a".to_owned(), period, || {}).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(db.take_lease("b", period).await.unwrap(), Some(2));
    // b dies; its lease expires; a's renewal finds the counter moved on.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(first.renew(Instant::now()).await, Renewal::TakenOver);
    // Then a third master takes it at once: a didn't take the expired lease back on its way
    // out (it would hold it, dead, for a whole period).
    assert_eq!(db.take_lease("c", period).await.unwrap(), Some(3));
}

#[tokio::test]
async fn a_database_that_doesnt_answer_is_a_failed_renewal() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let mut lead = Leadership::take(db.clone(), "a".to_owned(), DEFAULT_LEASE_PERIOD, || {}).await;
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
        unreachable!()
    };
    pool.close().await;
    let now = Instant::now();
    assert_eq!(lead.renew(now).await, Renewal::Failed);
    // Still leading on the earlier renewal, but not past it.
    assert!(lead.leads(now));
    assert!(!lead.leads(now + DEFAULT_LEASE_PERIOD));
    lead.release().await;
}

#[tokio::test]
async fn one_try_takes_a_free_lease_and_gives_way_to_a_holder() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let period = Duration::from_secs(5);
    let first = Leadership::try_take(db.clone(), "a".to_owned(), period)
        .await
        .unwrap()
        .expect("a free lease is taken");
    assert_eq!(first.holder(), "a");
    let second = Leadership::try_take(db.clone(), "b".to_owned(), period)
        .await
        .unwrap();
    assert!(second.is_none());
    first.release().await;
}

#[tokio::test]
async fn a_master_writes_through_its_lease() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let period = Duration::from_millis(300);
    let lead = Leadership::take(db.clone(), "a".to_owned(), period, || {}).await;
    assert!(lead.until() > Instant::now());
    let fenced = lead.fenced_db();
    fenced
        .worker_seen("w1", jiff::Timestamp::now())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(db.take_lease("b", period).await.unwrap(), Some(2));
    let refused = fenced.worker_seen("w1", jiff::Timestamp::now()).await;
    assert!(matches!(refused, Err(DbError::NotLeading)), "{refused:?}");
}
