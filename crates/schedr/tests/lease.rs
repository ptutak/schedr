//! The master's lease held through a migration (B204), and a stopping master's last
//! registration (B200), on Postgres: needs `SCHEDR_TEST_POSTGRES_URL` (`make
//! test-integration` sets it). The last two came from Codex's review of the fixes, as its proofs.

mod support;

use std::time::Duration;

use schedr_bin::roles::{migrate_holding, take_lease};
use schedr_db::{Database, database::Pool};
use schedr_master::{
    core::{MasterCore, Settings},
    lease::Leadership,
    service::Hub,
};
use schedr_proto::{
    PROTOCOL_VERSION,
    pb::{self, master_client::MasterClient, worker_message},
};
use sqlx::Connection;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};

/// Waits until a session of this database matching `which` (SQL over `pg_stat_activity`)
/// waits on a lock: the state a test needs, seen rather than slept for.
async fn lock_wait(locker: &mut sqlx::PgConnection, which: &str) {
    let query = format!(
        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname = current_database() \
         AND wait_event_type = 'Lock' AND {which})"
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            sqlx::query("SELECT pg_stat_clear_snapshot()")
                .execute(&mut *locker)
                .await
                .unwrap();
            let waiting: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(query.clone()))
                .fetch_one(&mut *locker)
                .await
                .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no session waits on a lock where {which}"));
}

/// B204: switching `change_notifications` waits on a lock past the lease's period; the
/// lease is renewed meanwhile, so no other master takes it while the switch is applied.
#[tokio::test]
async fn the_lease_is_renewed_while_change_notifications_wait_on_a_lock() {
    let url = support::postgres_database();
    let db = Database::connect(&url).await.unwrap();
    db.migrate().await.unwrap();
    // An open write on a table the switch alters (its lock, as an insert holds):
    // `ALTER TABLE ... DISABLE TRIGGER` waits for it.
    let mut locker = sqlx::PgConnection::connect(&url).await.unwrap();
    sqlx::query("BEGIN").execute(&mut locker).await.unwrap();
    sqlx::query("LOCK TABLE runs IN ROW EXCLUSIVE MODE")
        .execute(&mut locker)
        .await
        .unwrap();
    let period = Duration::from_secs(1);
    let lead = take_lease(db.clone(), period, "master", "first".to_owned()).await;
    let migrating = tokio::spawn({
        let db = db.clone();
        async move { migrate_holding(lead, &db, false).await.map(|_| ()) }
    });
    tokio::time::sleep(period * 3).await;
    assert!(
        !migrating.is_finished(),
        "the switch didn't wait for the lock"
    );
    let second = tokio::time::timeout(
        period * 2,
        take_lease(db.clone(), period, "master", "second".to_owned()),
    )
    .await;
    assert!(
        second.is_err(),
        "another master took the lease during the switch"
    );
    sqlx::query("COMMIT").execute(&mut locker).await.unwrap();
    migrating.await.unwrap().unwrap();
    assert_eq!(db.lease_holder().await.unwrap().as_deref(), Some("first"));
    assert!(!db.change_notifications().await.unwrap());
}

/// B204's review: renewals failing (the lease's database gone) while the migration's own
/// connection works: once the lease's time ran out and another master took it, the
/// migration fails instead of succeeding.
#[tokio::test]
async fn a_migration_cannot_succeed_after_failed_renewals_and_takeover() {
    let url = support::postgres_database();
    let db = Database::connect(&format!("{url}?application_name=review_migration"))
        .await
        .unwrap();
    db.migrate().await.unwrap();
    // A short query limit (the lease's time is its period less it), not so short that
    // connecting under load times out; its renewals fail at once anyway, its pool closed.
    let lease_db = Database::connect_with_timeout(&url, Duration::from_secs(1))
        .await
        .unwrap();
    let period = Duration::from_secs(3);
    let lead = Leadership::take(lease_db.clone(), "first".into(), period, || {}).await;
    let mut locker = sqlx::PgConnection::connect(&url).await.unwrap();
    sqlx::raw_sql("BEGIN; LOCK TABLE _sqlx_migrations IN ACCESS EXCLUSIVE MODE")
        .execute(&mut locker)
        .await
        .unwrap();
    // Fail every renewal, including after takeover; the migration has an independent working pool.
    let Pool::Postgres(pool) = lease_db.pool() else {
        unreachable!()
    };
    pool.close().await;
    let migrating = tokio::spawn({
        let db = db.clone();
        async move { migrate_holding(lead, &db, false).await.map(|_| ()) }
    });
    lock_wait(&mut locker, "application_name = 'review_migration'").await;
    // Success proves the original lease actually expired, without a guessed sleep.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if db
                .take_lease("second", Duration::from_secs(60))
                .await
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("another master acquired the expired lease");
    assert_eq!(db.lease_holder().await.unwrap().as_deref(), Some("second"));
    sqlx::query("COMMIT").execute(&mut locker).await.unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(15), migrating)
        .await
        .unwrap()
        .unwrap();
    let observed = match outcome {
        Ok(()) => "migration succeeded".to_owned(),
        Err(e) => e.to_string(),
    };
    db.release_lease("second").await.unwrap();
    drop(locker);
    drop(db);
    drop(lease_db);
    println!("migration after takeover: {observed}");
    assert!(
        observed.contains("lease") && !observed.contains("succeeded"),
        "{observed}"
    );
}

/// B200's review: a registration held in the database while its master stops, and finishing
/// after the master closed its streams, is refused: no stream is left open for the server's
/// graceful shutdown to wait for.
#[tokio::test]
async fn a_registration_finishing_after_shutdown_is_refused() {
    let url = support::postgres_database();
    let db = Database::connect_with_timeout(&url, Duration::from_secs(20))
        .await
        .unwrap();
    db.migrate().await.unwrap();
    let lead = Leadership::take(db.clone(), "first".into(), Duration::from_secs(120), || {}).await;
    let core = MasterCore::new(lead.fenced_db(), Settings::default());
    let mut registry = prometheus_client::registry::Registry::default();
    core.metrics().register(&mut registry);
    let hub = Hub::new(core);
    let (stop, stopped) = oneshot::channel();
    let running = tokio::spawn({
        let hub = hub.clone();
        async move {
            hub.run_leading(Duration::from_secs(3600), None, lead, async {
                let _ = stopped.await;
            })
            .await
        }
    });
    // Observe completion of the initial round, rather than sleeping until it probably ended.
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let mut encoded = String::new();
            prometheus_client::encoding::text::encode(&mut encoded, &registry).unwrap();
            if encoded.contains("master_tick_duration_seconds_count 1") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("initial round completed");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(hub.service())
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let mut client = MasterClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let mut locker = sqlx::PgConnection::connect(&url).await.unwrap();
    sqlx::raw_sql("BEGIN; LOCK TABLE workers IN SHARE MODE")
        .execute(&mut locker)
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(16);
    tx.send(pb::WorkerMessage {
        message: Some(worker_message::Message::Register(pb::Register {
            worker_id: "review_worker".into(),
            incarnation_id: uuid::Uuid::now_v7().to_string(),
            protocol: Some(PROTOCOL_VERSION),
            schedr_version: "1.0.0".into(),
            slots: 1,
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let opening = tokio::spawn(async move { client.control(ReceiverStream::new(rx)).await });
    // pg_stat_activity proves registration passed the first leading check and reached its write.
    lock_wait(&mut locker, "query ILIKE '%INSERT INTO workers%'").await;
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(20), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!*hub.leading().borrow());
    hub.close_streams().await;
    sqlx::query("COMMIT").execute(&mut locker).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(20), opening)
        .await
        .unwrap()
        .unwrap();
    let observed = match result {
        Err(status) => {
            println!("registration status: {status}");
            format!("{:?}", status.code())
        }
        Ok(response) => {
            drop(response);
            "accepted a stream after shutdown".to_owned()
        }
    };
    drop(tx);
    server.abort();
    let _ = server.await;
    drop(locker);
    drop(hub);
    drop(db);
    println!("late registration: {observed}");
    assert_eq!(observed, "Unavailable");
}
