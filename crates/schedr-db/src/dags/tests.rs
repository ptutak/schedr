use pretty_assertions::assert_eq;
use schedr_model::runner::ErrorSummary;
use serde_json::json;
use sqlx::SqlitePool;

use super::*;

async fn migrated() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db
}

fn pool(db: &Database) -> &SqlitePool {
    match db {
        Database::Sqlite(pool) => pool,
        Database::Postgres(_) => panic!("expected SQLite"),
    }
}

/// Runs test SQL; the statements are fixed strings written in this file, so they are safe.
async fn execute(db: &Database, query: &str) {
    sqlx::query(AssertSqlSafe(query.to_owned()))
        .execute(pool(db))
        .await
        .unwrap();
}

fn now() -> Timestamp {
    "2026-10-21T08:00:00Z".parse().unwrap()
}

fn dag(id: &str, path: &str) -> Dag {
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": id, "location": {"root": "etl", "path": path},
        "schedule": {"kind": "none"}, "start": null, "end": null, "initial_since": null,
        "catchup": false, "max_active_runs": 1, "tasks": [], "edges": []
    }))
    .unwrap()
}

fn files() -> Vec<ParsedFile> {
    vec![
        ParsedFile {
            path: "sync.py".to_owned(),
            dags: vec![dag("sync", "sync.py")],
            error: None,
        },
        ParsedFile {
            path: "bad.py".to_owned(),
            dags: vec![],
            error: Some(ErrorSummary {
                exception: "ImportError".to_owned(),
                message: "no module".to_owned(),
                traceback: "Traceback ...".to_owned(),
            }),
        },
    ]
}

async fn store_parse(db: &Database, hash: &str, files: &[ParsedFile]) -> String {
    db.insert_parse(&NewParse {
        root: "etl",
        content_hash: hash,
        schedr_version: "0.1.0",
        worker_id: "w1",
        parsed_at: now(),
        files,
    })
    .await
    .unwrap()
}

fn id(text: &str) -> DagId {
    DagId::new(text).unwrap()
}

fn place(path: &str) -> DagLocation {
    DagLocation {
        root: "etl".to_owned(),
        path: path.to_owned(),
    }
}

async fn version_count(db: &Database) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM dag_versions")
        .fetch_one(pool(db))
        .await
        .unwrap()
}

#[tokio::test]
async fn parses_are_found_by_root_hash_and_version_and_keep_their_files() {
    let db = migrated().await;
    assert_eq!(
        db.find_parse("etl", "sha256:a", "0.1.0").await.unwrap(),
        None
    );
    let parse_id = store_parse(&db, "sha256:a", &files()).await;
    assert_eq!(
        db.find_parse("etl", "sha256:a", "0.1.0").await.unwrap(),
        Some(parse_id.clone())
    );
    assert_eq!(
        db.find_parse("etl", "sha256:a", "0.2.0").await.unwrap(),
        None
    );
    let mut expected = files();
    expected.sort_by(|a, b| a.path.cmp(&b.path));
    assert_eq!(db.parse_files(&parse_id).await.unwrap(), expected);
}

#[tokio::test]
async fn storing_the_same_parse_again_keeps_the_first() {
    let db = migrated().await;
    let first = store_parse(&db, "sha256:a", &files()).await;
    let second = store_parse(&db, "sha256:a", &[]).await;
    assert_eq!(first, second);
    assert_eq!(db.parse_files(&first).await.unwrap().len(), 2);
}

#[tokio::test]
async fn a_new_dag_is_paused_with_its_version() {
    let db = migrated().await;
    let parse_id = store_parse(&db, "sha256:a", &files()).await;
    let change = DagChange::Define {
        dag: Box::new(dag("sync", "sync.py")),
        parse_id: parse_id.clone(),
        pause: false,
    };
    db.apply_dag_changes(std::slice::from_ref(&change), now())
        .await
        .unwrap();
    let stored = db.stored_dags().await.unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].dag_id, id("sync"));
    assert!(stored[0].paused);
    assert!(!stored[0].removed);
    assert_eq!(stored[0].conflict, None);
    let current = stored[0].current.as_ref().unwrap();
    assert_eq!(current.parse_id, parse_id);
    assert_eq!(current.dag, dag("sync", "sync.py"));

    db.apply_dag_changes(&[change], now()).await.unwrap();
    assert_eq!(version_count(&db).await, 1);
}

#[tokio::test]
async fn a_new_parse_becomes_the_current_version_and_keeps_the_paused_setting() {
    let db = migrated().await;
    let first = store_parse(&db, "sha256:a", &files()).await;
    let second = store_parse(&db, "sha256:b", &files()).await;
    let define = |parse_id: &str| DagChange::Define {
        dag: Box::new(dag("sync", "sync.py")),
        parse_id: parse_id.to_owned(),
        pause: false,
    };
    db.apply_dag_changes(&[define(&first)], now())
        .await
        .unwrap();
    execute(&db, "UPDATE dags SET paused = 0").await;
    db.apply_dag_changes(&[define(&second)], now())
        .await
        .unwrap();
    let stored = &db.stored_dags().await.unwrap()[0];
    assert!(!stored.paused);
    assert_eq!(stored.current.as_ref().unwrap().parse_id, second);
    assert_eq!(version_count(&db).await, 2);
    db.apply_dag_changes(&[define(&first)], now())
        .await
        .unwrap();
    let stored = &db.stored_dags().await.unwrap()[0];
    assert_eq!(stored.current.as_ref().unwrap().parse_id, first);
    assert_eq!(version_count(&db).await, 2);
}

#[tokio::test]
async fn removing_hides_a_dag_and_coming_back_can_pause_it() {
    let db = migrated().await;
    let parse_id = store_parse(&db, "sha256:a", &files()).await;
    let define = |pause| DagChange::Define {
        dag: Box::new(dag("sync", "sync.py")),
        parse_id: parse_id.clone(),
        pause,
    };
    db.apply_dag_changes(&[define(false)], now()).await.unwrap();
    execute(&db, "UPDATE dags SET paused = 0").await;
    db.apply_dag_changes(&[DagChange::Remove { dag_id: id("sync") }], now())
        .await
        .unwrap();
    let stored = &db.stored_dags().await.unwrap()[0];
    assert!(stored.removed);
    assert!(stored.current.is_some());
    db.apply_dag_changes(&[define(true)], now()).await.unwrap();
    let stored = &db.stored_dags().await.unwrap()[0];
    assert!(!stored.removed);
    assert!(stored.paused);
}

#[tokio::test]
async fn conflicts_record_the_places_and_clear_with_a_definition() {
    let db = migrated().await;
    let parse_id = store_parse(&db, "sha256:a", &files()).await;
    let places = vec![place("a.py"), place("b.py")];
    db.apply_dag_changes(
        &[DagChange::Conflict {
            dag_id: id("dup"),
            places: places.clone(),
            pause: false,
        }],
        now(),
    )
    .await
    .unwrap();
    let stored = &db.stored_dags().await.unwrap()[0];
    assert_eq!(stored.conflict, Some(places));
    assert!(stored.paused);
    assert_eq!(stored.current, None);

    db.apply_dag_changes(
        &[DagChange::Define {
            dag: Box::new(dag("dup", "a.py")),
            parse_id,
            pause: false,
        }],
        now(),
    )
    .await
    .unwrap();
    let stored = &db.stored_dags().await.unwrap()[0];
    assert_eq!(stored.conflict, None);
    assert!(stored.current.is_some());
}

#[tokio::test]
async fn changes_are_stored_all_or_none() {
    let db = migrated().await;
    let change = DagChange::Define {
        dag: Box::new(dag("sync", "sync.py")),
        parse_id: Uuid::now_v7().to_string(), // no such parse: the version can't be stored
        pause: false,
    };
    let error = db
        .apply_dag_changes(
            &[
                DagChange::Conflict {
                    dag_id: id("dup"),
                    places: vec![place("a.py")],
                    pause: false,
                },
                change,
            ],
            now(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::Sqlx(_)));
    assert_eq!(db.stored_dags().await.unwrap(), []);
}

#[tokio::test]
async fn stored_data_that_doesnt_read_back_is_an_error() {
    let db = migrated().await;
    let parse_id = store_parse(&db, "sha256:a", &files()).await;
    execute(&db, "UPDATE parse_files SET dags = '{\"not\": \"a list\"}'").await;
    let error = db.parse_files(&parse_id).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid stored data in parse_files.dags: "),
        "{error}"
    );
    execute(
        &db,
        "INSERT INTO dags (dag_id, first_seen_at, conflict) \
         VALUES ('bad id!', '2026-10-21T00:00:00Z', NULL)",
    )
    .await;
    let error = db.stored_dags().await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid stored data in dags.dag_id: "),
        "{error}"
    );
}
