use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pretty_assertions::assert_eq;
use sqlx::Connection as _;

use super::*;

/// One statement's limit in these tests. Long enough that a loaded machine (coverage, a
/// busy disk) still finishes it. A renewal leads for the period less this.
const QUERY_LIMIT: Duration = Duration::from_secs(2);

fn db_url(dir: &tempfile::TempDir) -> String {
    format!("sqlite://{}", dir.path().join("lease.db").display())
}

async fn file_db(dir: &tempfile::TempDir) -> Database {
    let db = Database::connect_with_timeout(&db_url(dir), QUERY_LIMIT)
        .await
        .unwrap();
    db.migrate().await.unwrap();
    db
}

#[tokio::test]
async fn a_second_master_waits_as_a_standby_and_takes_over_when_the_first_gives_up() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let period = QUERY_LIMIT * 3;
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
    // A try that times out never calls on_wait, and the next one is a third of the period
    // later. Several periods covers a late try; the flag is usually set on the first.
    let standby = tokio::time::timeout(period * 3, async {
        while !waited.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(standby.is_ok(), "the second master is a standby");
    assert!(!second.is_finished());
    first.release().await;
    // The retry after a timeout is two query limits, then another try: several periods.
    let second = tokio::time::timeout(period * 3, second)
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
    let period = QUERY_LIMIT * 3;
    let mut first = Leadership::take(db.clone(), "a".to_owned(), period, || {}).await;
    let now = Instant::now();
    assert_eq!(first.renew(now).await, Renewal::Held);
    // What `lead_time` subtracts: this database's query limit. Just inside that edge, and not
    // on it (`leads` is strict).
    let leads_for = period.checked_sub(db.query_timeout()).unwrap();
    let inside = leads_for.checked_sub(Duration::from_millis(10)).unwrap();
    assert!(first.leads(now + inside));
    assert!(!first.leads(now + leads_for));
    // It expires; another master takes it; this one's renewal sees the takeover.
    tokio::time::sleep(period + QUERY_LIMIT).await;
    assert_eq!(db.take_lease("b", period).await.unwrap(), Some(2));
    assert_eq!(first.renew(Instant::now()).await, Renewal::TakenOver);
    // Even once it could take it back, the counter moved on: still a takeover.
    tokio::time::sleep(period + QUERY_LIMIT).await;
    assert_eq!(first.renew(Instant::now()).await, Renewal::TakenOver);
}

#[tokio::test]
async fn a_master_taken_over_never_writes_itself_back_as_the_holder() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    // No `leads` check, so the period can be short. The query limit stays generous, and the
    // sleep is still the period plus that limit.
    let period = QUERY_LIMIT / 2;
    let mut first = Leadership::take(db.clone(), "a".to_owned(), period, || {}).await;
    tokio::time::sleep(period + QUERY_LIMIT).await;
    assert_eq!(db.take_lease("b", period).await.unwrap(), Some(2));
    // b dies; its lease expires; a's renewal finds the counter moved on.
    tokio::time::sleep(period + QUERY_LIMIT).await;
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
    let period = QUERY_LIMIT * 3;
    let lead = Leadership::take(db.clone(), "a".to_owned(), period, || {}).await;
    assert!(lead.until() > Instant::now());
    let fenced = lead.fenced_db();
    fenced
        .worker_seen("w1", jiff::Timestamp::now())
        .await
        .unwrap();
    tokio::time::sleep(period + QUERY_LIMIT).await;
    assert_eq!(db.take_lease("b", period).await.unwrap(), Some(2));
    let refused = fenced.worker_seen("w1", jiff::Timestamp::now()).await;
    assert!(matches!(refused, Err(DbError::NotLeading)), "{refused:?}");
}

/// Holds SQLite's write lock for `hold`, and tells `locked` once the lock is taken.
async fn hold_write_lock(url: &str, hold: Duration, locked: tokio::sync::oneshot::Sender<()>) {
    let mut connection = sqlx::SqliteConnection::connect(url).await.unwrap();
    // Opening can find the file busy for a moment; the stall under test is the lock below.
    sqlx::query("PRAGMA busy_timeout = 30000")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut connection)
        .await
        .unwrap();
    locked.send(()).unwrap();
    tokio::time::sleep(hold).await;
    sqlx::query("COMMIT")
        .execute(&mut connection)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_stall_shorter_than_the_query_limit_still_takes_the_lease() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let url = db_url(&dir);
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    // A small share of the query limit: the take has room to wait the lock out.
    let hold = QUERY_LIMIT / 4;
    let holder = tokio::spawn(async move {
        hold_write_lock(&url, hold, locked_tx).await;
    });
    locked_rx.await.unwrap();
    let started = Instant::now();
    assert_eq!(db.take_lease("a", QUERY_LIMIT * 3).await.unwrap(), Some(1));
    let elapsed = started.elapsed();
    // A timer can wake a little early. A take that never blocked is nowhere near the hold.
    assert!(
        elapsed + Duration::from_millis(20) >= hold,
        "the take waited out the {hold:?} lock, took {elapsed:?}"
    );
    holder.await.unwrap();
}

#[tokio::test]
async fn a_stall_past_the_query_limit_is_a_failed_renewal() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let url = db_url(&dir);
    let period = QUERY_LIMIT * 3;
    let mut lead = Leadership::take(db.clone(), "a".to_owned(), period, || {}).await;
    let until = lead.until();
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let holder = tokio::spawn(async move {
        // Twice the limit: a renewal that starts late under load still meets the lock.
        hold_write_lock(&url, QUERY_LIMIT * 2, locked_tx).await;
    });
    locked_rx.await.unwrap();
    assert_eq!(lead.renew(Instant::now()).await, Renewal::Failed);
    assert_eq!(lead.until(), until);
    holder.await.unwrap();
}
