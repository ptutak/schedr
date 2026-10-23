use std::time::Duration;

use pretty_assertions::assert_eq;

use sqlx::AssertSqlSafe;

use super::*;

const MEMORY: &str = "sqlite::memory:";

async fn migrated() -> Database {
    let db = Database::connect(MEMORY).await.unwrap();
    db.migrate().await.unwrap();
    db
}

fn pool(db: &Database) -> &SqlitePool {
    match db.pool() {
        crate::database::Pool::Sqlite(pool) => pool,
        crate::database::Pool::Postgres(_) => panic!("expected SQLite"),
    }
}

/// Runs test SQL; the statements are fixed strings written in this file, so they are safe.
async fn execute(db: &Database, sql: &str) -> Result<(), sqlx::Error> {
    sqlx::query(AssertSqlSafe(sql.to_owned()))
        .execute(pool(db))
        .await
        .map(|_| ())
}

/// Inserts a DAG, a parse result and a DAG version, so runs can reference them.
async fn insert_dag(db: &Database) {
    execute(
        db,
        "INSERT INTO dags (dag_id, first_seen_at) VALUES ('sync', '2026-10-21T00:00:00Z')",
    )
    .await
    .unwrap();
    execute(
        db,
        "INSERT INTO parse_results (id, root, content_hash, schedr_version, worker_id, parsed_at) \
         VALUES ('p1', 'etl', 'h1', '0.1', 'w1', '2026-10-21T00:00:00Z')",
    )
    .await
    .unwrap();
    execute(
        db,
        "INSERT INTO dag_versions (id, dag_id, parse_result_id, dag_json, created_at) \
         VALUES ('v1', 'sync', 'p1', '{}', '2026-10-21T00:00:00Z')",
    )
    .await
    .unwrap();
}

fn run_sql(id: &str, kind: &str, planned_for: &str) -> String {
    format!(
        "INSERT INTO runs (id, dag_id, dag_version_id, kind, state, planned_for, since, \
         triggered_at, triggered_by) VALUES ('{id}', 'sync', 'v1', '{kind}', 'queued', \
         '{planned_for}', '2026-10-20T03:00:00Z', '2026-10-21T03:00:00Z', 'scheduler')"
    )
}

#[tokio::test]
async fn connects_to_sqlite_and_reports_the_backend() {
    let db = Database::connect(MEMORY).await.unwrap();
    assert_eq!(db.backend(), Backend::Sqlite);
}

#[tokio::test]
async fn waiting_for_a_connection_is_limited_by_the_query_timeout() {
    // In memory there is one connection: hold it, and the next query can't get one.
    let db = Database::connect_with_timeout(MEMORY, Duration::from_millis(100))
        .await
        .unwrap();
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    let _held = pool.acquire().await.unwrap();
    let started = std::time::Instant::now();
    let error = db.state_counts().await.unwrap_err();
    assert!(matches!(error, DbError::Sqlx(_)), "{error:?}");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(DEFAULT_QUERY_TIMEOUT, Duration::from_secs(10));
}

#[tokio::test]
async fn rejects_unsupported_urls() {
    let err = Database::connect("mysql://u:pw@h/db").await.unwrap_err();
    assert!(matches!(err, DbError::UnsupportedUrl(_)), "{err:?}");
    assert!(!err.to_string().contains("pw"), "{err}");
}

#[tokio::test]
async fn rejects_malformed_sqlite_urls() {
    let err = Database::connect("sqlite:?mode=bogus").await.unwrap_err();
    assert!(matches!(err, DbError::Sqlx(_)), "{err:?}");
}

#[tokio::test]
async fn a_new_database_is_uninitialized_and_not_usable() {
    let db = Database::connect(MEMORY).await.unwrap();
    assert_eq!(db.applied_migrations().await.unwrap(), Vec::<i64>::new());
    assert_eq!(
        db.schema_status().await.unwrap(),
        SchemaStatus::Uninitialized
    );
    let err = db.require_usable_schema().await.unwrap_err();
    assert!(matches!(
        &err,
        DbError::SchemaNotUsable {
            status: SchemaStatus::Uninitialized
        }
    ));
    assert!(
        err.to_string().contains("run `schedr db migrate` first"),
        "{err}"
    );
}

#[tokio::test]
async fn migrate_brings_the_schema_up_to_date_and_is_idempotent() {
    let db = migrated().await;
    assert_eq!(
        db.applied_migrations().await.unwrap(),
        Backend::Sqlite.migration_versions()
    );
    assert_eq!(db.schema_status().await.unwrap(), SchemaStatus::UpToDate);
    db.migrate().await.unwrap();
    assert_eq!(
        db.require_usable_schema().await.unwrap(),
        SchemaStatus::UpToDate
    );
}

#[tokio::test]
async fn a_schema_migrated_by_a_newer_schedr_is_still_usable() {
    let db = migrated().await;
    execute(
        &db,
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
         VALUES (9999, 'future', TRUE, x'00', 0)",
    )
    .await
    .unwrap();
    let status = db.require_usable_schema().await.unwrap();
    assert_eq!(
        status,
        SchemaStatus::Newer {
            unknown: vec![9999]
        }
    );
}

#[tokio::test]
async fn migrating_a_database_with_unknown_migrations_fails() {
    let db = migrated().await;
    execute(
        &db,
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
         VALUES (9999, 'future', TRUE, x'00', 0)",
    )
    .await
    .unwrap();
    let err = db.migrate().await.unwrap_err();
    assert!(matches!(err, DbError::Migrate(_)), "{err:?}");
    assert!(err.to_string().starts_with("migration failed: "), "{err}");
}

#[tokio::test]
async fn foreign_keys_are_enforced() {
    let db = migrated().await;
    let err = execute(&db, &run_sql("r1", "manual", "2026-10-21T03:00:00Z"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("FOREIGN KEY"), "{err}");
}

#[tokio::test]
async fn new_dags_are_paused_by_default() {
    let db = migrated().await;
    insert_dag(&db).await;
    let paused: i64 = sqlx::query_scalar("SELECT paused FROM dags WHERE dag_id = 'sync'")
        .fetch_one(pool(&db))
        .await
        .unwrap();
    assert_eq!(paused, 1);
}

#[tokio::test]
async fn a_scheduled_run_can_exist_only_once_per_planned_for() {
    let db = migrated().await;
    insert_dag(&db).await;
    execute(&db, &run_sql("r1", "scheduled", "2026-10-21T03:00:00Z"))
        .await
        .unwrap();
    let err = execute(&db, &run_sql("r2", "scheduled", "2026-10-21T03:00:00Z"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("UNIQUE"), "{err}");
    // Manual runs for the same time are allowed, any number of them.
    execute(&db, &run_sql("r3", "manual", "2026-10-21T03:00:00Z"))
        .await
        .unwrap();
    execute(&db, &run_sql("r4", "manual", "2026-10-21T03:00:00Z"))
        .await
        .unwrap();
}

#[tokio::test]
async fn states_outside_the_model_are_rejected() {
    let db = migrated().await;
    insert_dag(&db).await;
    let err = execute(&db, &run_sql("r1", "sometimes", "2026-10-21T03:00:00Z"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("CHECK"), "{err}");
    execute(&db, &run_sql("r2", "manual", "2026-10-21T03:00:00Z"))
        .await
        .unwrap();
    let err = execute(
        &db,
        "INSERT INTO task_instances (id, run_id, task_id, state, updated_at) \
         VALUES ('t1', 'r2', 'extract', 'paused', '2026-10-21T03:00:00Z')",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("CHECK"), "{err}");
}

#[tokio::test]
async fn task_instances_are_unique_per_run_task_and_map_index() {
    let db = migrated().await;
    insert_dag(&db).await;
    execute(&db, &run_sql("r1", "manual", "2026-10-21T03:00:00Z"))
        .await
        .unwrap();
    let insert = |id: &str, map_index: i64| {
        format!(
            "INSERT INTO task_instances (id, run_id, task_id, map_index, state, updated_at) \
             VALUES ('{id}', 'r1', 'process', {map_index}, 'scheduled', '2026-10-21T03:00:00Z')"
        )
    };
    execute(&db, &insert("t1", -1)).await.unwrap();
    execute(&db, &insert("t2", 0)).await.unwrap();
    let err = execute(&db, &insert("t3", 0)).await.unwrap_err();
    assert!(err.to_string().contains("UNIQUE"), "{err}");
}

#[tokio::test]
async fn deleting_a_run_deletes_its_task_instances_attempts_and_outputs() {
    let db = migrated().await;
    insert_dag(&db).await;
    execute(&db, &run_sql("r1", "manual", "2026-10-21T03:00:00Z"))
        .await
        .unwrap();
    execute(
        &db,
        "INSERT INTO task_instances (id, run_id, task_id, state, updated_at) \
         VALUES ('t1', 'r1', 'extract', 'success', '2026-10-21T03:00:00Z')",
    )
    .await
    .unwrap();
    execute(
        &db,
        "INSERT INTO attempts (id, task_instance_id, try_number, status, assigned_at) \
         VALUES ('a1', 't1', 1, 'success', '2026-10-21T03:00:00Z')",
    )
    .await
    .unwrap();
    execute(
        &db,
        "INSERT INTO outputs (task_instance_id, outcome, value, size_bytes) \
         VALUES ('t1', 'SUCCESS', '42', 2)",
    )
    .await
    .unwrap();
    execute(&db, "DELETE FROM runs WHERE id = 'r1'")
        .await
        .unwrap();
    for table in ["task_instances", "attempts", "outputs"] {
        let count: i64 = sqlx::query_scalar(AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(pool(&db))
            .await
            .unwrap();
        assert_eq!(count, 0, "{table}");
    }
}

#[tokio::test]
async fn commands_start_pending() {
    let db = migrated().await;
    execute(
        &db,
        "INSERT INTO commands (id, kind, payload, requested_by, requested_at) \
         VALUES ('c1', 'pause', '{}', 'anna', '2026-10-21T03:00:00Z')",
    )
    .await
    .unwrap();
    let status: String = sqlx::query_scalar("SELECT status FROM commands WHERE id = 'c1'")
        .fetch_one(pool(&db))
        .await
        .unwrap();
    assert_eq!(status, "pending");
}

#[test]
fn errors_from_sqlx_are_wrapped_with_context() {
    let err = DbError::from(sqlx::Error::RowNotFound);
    assert!(err.to_string().starts_with("database error: "), "{err}");
}

#[test]
fn a_failed_start_is_a_database_error_unless_the_fence_refused_it() {
    // The fence's refusal (SR001) is read on Postgres: tests/postgres.rs.
    assert!(matches!(
        DbError::from_begin(sqlx::Error::PoolTimedOut),
        DbError::Sqlx(sqlx::Error::PoolTimedOut)
    ));
}
