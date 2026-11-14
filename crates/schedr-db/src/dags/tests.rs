use pretty_assertions::assert_eq;
use schedr_model::runner::ErrorSummary;
use serde_json::json;
use sqlx::{AssertSqlSafe, SqlitePool};

use super::*;

async fn migrated() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
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
        "produced_by": "1.0", "id": id, "location": {"root": "etl", "path": path},
        "trigger": {"kind": "cron", "expression": "0 3 * * *", "timezone": "UTC",
            "catchup": false},
        "initial_since": null, "max_active_runs": 1, "tasks": [], "edges": []
    }))
    .unwrap()
}

fn files() -> Vec<ParsedFile> {
    vec![
        ParsedFile {
            script_roots: Vec::new(),
            path: "sync.py".to_owned(),
            dags: vec![dag("sync", "sync.py")],
            connections: Vec::new(),
            error: None,
            warnings: Vec::new(),
        },
        ParsedFile {
            script_roots: Vec::new(),
            path: "bad.py".to_owned(),
            dags: vec![],
            connections: Vec::new(),
            error: Some(ErrorSummary {
                exception: "ImportError".to_owned(),
                message: "no module".to_owned(),
                traceback: "Traceback ...".to_owned(),
            }),
            warnings: Vec::new(),
        },
    ]
}

async fn store_parse(db: &Database, hash: &str, files: &[ParsedFile]) -> String {
    db.insert_parse(&NewParse {
        root: "etl",
        content_hash: hash,
        schedr_version: "1.0.0",
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
        db.find_parse("etl", "sha256:a", "1.0.0").await.unwrap(),
        None
    );
    let parse_id = store_parse(&db, "sha256:a", &files()).await;
    assert_eq!(
        db.find_parse("etl", "sha256:a", "1.0.0").await.unwrap(),
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
    assert_eq!(stored[0].paused, Some(true));
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
    execute(&db, "UPDATE dag_triggers SET paused = 0").await;
    db.apply_dag_changes(&[define(&second)], now())
        .await
        .unwrap();
    let stored = &db.stored_dags().await.unwrap()[0];
    assert_eq!(stored.paused, Some(false));
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
    execute(&db, "UPDATE dag_triggers SET paused = 0").await;
    db.apply_dag_changes(&[DagChange::Remove { dag_id: id("sync") }], now())
        .await
        .unwrap();
    let stored = &db.stored_dags().await.unwrap()[0];
    assert!(stored.removed);
    assert!(stored.current.is_some());
    db.apply_dag_changes(&[define(true)], now()).await.unwrap();
    let stored = &db.stored_dags().await.unwrap()[0];
    assert!(!stored.removed);
    assert_eq!(stored.paused, Some(true));
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
    assert_eq!(stored.paused, None, "no trigger known yet");
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

#[tokio::test]
async fn dags_can_be_paused_and_unpaused() {
    let db = migrated().await;
    let parse_id = store_parse(&db, "sha256:a", &files()).await;
    let change = DagChange::Define {
        dag: Box::new(dag("sync", "sync.py")),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], now()).await.unwrap();
    assert!(db.set_paused(&id("sync"), false).await.unwrap());
    assert_eq!(db.stored_dags().await.unwrap()[0].paused, Some(false));
    assert!(db.set_paused(&id("sync"), true).await.unwrap());
    assert_eq!(db.stored_dags().await.unwrap()[0].paused, Some(true));
    assert!(!db.set_paused(&id("nope"), true).await.unwrap());
}

#[tokio::test]
async fn the_parses_of_a_hash_are_listed_oldest_first() {
    let db = migrated().await;
    assert_eq!(db.parses_of("etl", "h1").await.unwrap(), []);
    let first = store_parse(&db, "h1", &files()).await;
    let later: Timestamp = "2026-10-22T08:00:00Z".parse().unwrap();
    let second = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h1",
            schedr_version: "0.2.0",
            worker_id: "w2",
            parsed_at: later,
            files: &files(),
        })
        .await
        .unwrap();
    store_parse(&db, "h2", &files()).await;
    let parses = db.parses_of("etl", "h1").await.unwrap();
    assert_eq!(
        parses,
        [
            StoredParse {
                id: first,
                schedr_version: "1.0.0".to_owned(),
                parsed_at: now(),
            },
            StoredParse {
                id: second,
                schedr_version: "0.2.0".to_owned(),
                parsed_at: later,
            },
        ]
    );
}

#[tokio::test]
async fn a_switch_to_a_hash_makes_it_the_most_recent_and_a_first_report_only_records_it() {
    let db = migrated().await;
    let t = |s: &str| -> Timestamp { s.parse().unwrap() };
    db.record_root_hash("etl", "a", false, t("2026-10-20T00:00:00Z"))
        .await
        .unwrap();
    db.record_root_hash("etl", "b", true, t("2026-10-21T00:00:00Z"))
        .await
        .unwrap();
    db.record_root_hash("etl", "a", false, t("2026-10-22T00:00:00Z"))
        .await
        .unwrap();
    db.record_root_hash("ml", "x", false, t("2026-10-22T00:00:00Z"))
        .await
        .unwrap();
    let switches = db.root_hash_switches("etl").await.unwrap();
    assert_eq!(
        switches,
        BTreeMap::from([
            ("a".to_owned(), t("2026-10-20T00:00:00Z")),
            ("b".to_owned(), t("2026-10-21T00:00:00Z")),
        ])
    );
    // A revert: a worker switches back to `a`.
    db.record_root_hash("etl", "a", true, t("2026-10-23T00:00:00Z"))
        .await
        .unwrap();
    assert_eq!(
        db.root_hash_switches("etl").await.unwrap()["a"],
        t("2026-10-23T00:00:00Z")
    );
}

#[tokio::test]
async fn the_current_parses_import_errors_are_listed_and_replaced_by_the_next_current_one() {
    let db = migrated().await;
    assert_eq!(db.current_import_errors().await.unwrap(), vec![]);
    let broken = store_parse(&db, "h1", &files()).await;
    // A parse that isn't current shows nothing.
    assert_eq!(db.current_import_errors().await.unwrap(), vec![]);
    db.set_current_parse("etl", &broken, now()).await.unwrap();
    let errors = db.current_import_errors().await.unwrap();
    assert_eq!(
        errors,
        [ImportErrorRecord {
            root: "etl".to_owned(),
            content_hash: "h1".to_owned(),
            schedr_version: "1.0.0".to_owned(),
            worker_id: "w1".to_owned(),
            master_id: None,
            parsed_at: now(),
            path: "bad.py".to_owned(),
            error: files()[1].error.clone().unwrap(),
        }]
    );
    // A retry of the file (decision 72.2), on another worker: its error is that worker's,
    // when it parsed it (decision 93), stored by the master holding the lease (decision 129).
    let retried_at: Timestamp = "2026-10-21T08:05:00Z".parse().unwrap();
    let epoch = db
        .take_lease("m-1", std::time::Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    db.fenced("m-1", epoch)
        .replace_parse_files(&broken, &files()[1..], "w2", retried_at)
        .await
        .unwrap();
    let errors = db.current_import_errors().await.unwrap();
    assert_eq!(
        errors
            .iter()
            .map(|e| (
                e.path.as_str(),
                e.worker_id.as_str(),
                e.master_id.as_deref(),
                e.parsed_at
            ))
            .collect::<Vec<_>>(),
        [("bad.py", "w2", Some("m-1"), retried_at)]
    );
    // The fixed code's parse becomes current: its root has no errors any more.
    let fixed = store_parse(&db, "h2", &files()[..1]).await;
    db.set_current_parse("etl", &fixed, now()).await.unwrap();
    assert_eq!(db.current_import_errors().await.unwrap(), vec![]);
}

#[tokio::test]
async fn a_bad_stored_time_names_the_column_it_is_in() {
    let db = migrated().await;
    let parse = store_parse(&db, "h1", &files()).await;
    db.set_current_parse("etl", &parse, now()).await.unwrap();
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
        unreachable!("the tests' database is SQLite")
    };
    sqlx::query("UPDATE parse_files SET parsed_at = 'not-a-time'")
        .execute(pool)
        .await
        .unwrap();
    let error = db.current_import_errors().await.unwrap_err().to_string();
    assert!(error.contains("parse_files.parsed_at"), "{error}");
    sqlx::query("UPDATE parse_results SET parsed_at = 'not-a-time'")
        .execute(pool)
        .await
        .unwrap();
    let error = db.parses_of("etl", "h1").await.unwrap_err().to_string();
    assert!(error.contains("parse_results.parsed_at"), "{error}");
}

#[tokio::test]
async fn a_files_script_roots_are_stored_and_read_back_with_it() {
    let db = migrated().await;
    let mut asked = files();
    asked[0].script_roots = vec!["etl-scripts".to_owned(), "ml-scripts".to_owned()];
    let parse_id = store_parse(&db, "h1", &asked).await;
    let read = db.parse_files(&parse_id).await.unwrap();
    let stored = read.iter().find(|f| f.path == asked[0].path).unwrap();
    assert_eq!(stored.script_roots, ["etl-scripts", "ml-scripts"]);
}

#[tokio::test]
async fn a_dag_is_live_since_it_last_came_on_and_never_while_paused_removed_or_in_conflict() {
    let db = migrated().await;
    let parse_id = store_parse(&db, "sha256:a", &files()).await;
    let define = |pause| DagChange::Define {
        dag: Box::new(dag("sync", "sync.py")),
        parse_id: parse_id.clone(),
        pause,
    };
    let at = |seconds: i64| {
        now()
            .checked_add(jiff::SignedDuration::from_secs(seconds))
            .unwrap()
    };
    db.apply_dag_changes(&[define(false)], at(0)).await.unwrap();
    assert_eq!(db.live_since("sync").await.unwrap(), None, "new is paused");
    db.set_paused_at(&id("sync"), false, at(1)).await.unwrap();
    assert_eq!(db.live_since("sync").await.unwrap(), Some(at(1)));
    db.apply_dag_changes(&[define(false)], at(2)).await.unwrap();
    db.set_paused_at(&id("sync"), false, at(3)).await.unwrap();
    assert_eq!(
        db.live_since("sync").await.unwrap(),
        Some(at(1)),
        "a new version or unpausing again keeps it"
    );
    db.set_paused_at(&id("sync"), true, at(4)).await.unwrap();
    assert_eq!(db.live_since("sync").await.unwrap(), None);
    db.set_paused_at(&id("sync"), false, at(5)).await.unwrap();
    assert_eq!(db.live_since("sync").await.unwrap(), Some(at(5)));
    db.apply_dag_changes(&[DagChange::Remove { dag_id: id("sync") }], at(6))
        .await
        .unwrap();
    assert_eq!(db.live_since("sync").await.unwrap(), None);
    db.set_paused_at(&id("sync"), true, at(7)).await.unwrap();
    db.set_paused_at(&id("sync"), false, at(8)).await.unwrap();
    assert_eq!(
        db.live_since("sync").await.unwrap(),
        None,
        "unpaused while removed"
    );
    db.apply_dag_changes(&[define(false)], at(9)).await.unwrap();
    assert_eq!(
        db.live_since("sync").await.unwrap(),
        Some(at(9)),
        "back, from its return"
    );
    db.apply_dag_changes(
        &[DagChange::Conflict {
            dag_id: id("sync"),
            places: vec![place("a.py"), place("b.py")],
            pause: false,
        }],
        at(10),
    )
    .await
    .unwrap();
    assert_eq!(db.live_since("sync").await.unwrap(), None);
    db.apply_dag_changes(&[define(false)], at(11))
        .await
        .unwrap();
    assert_eq!(db.live_since("sync").await.unwrap(), Some(at(11)));
    let changed = |trigger: serde_json::Value, hash: &str| {
        let mut other = dag("sync", "sync.py");
        other.trigger = serde_json::from_value(trigger).unwrap();
        let (files, db, hash) = (files(), &db, hash.to_owned());
        async move {
            DagChange::Define {
                dag: Box::new(other),
                parse_id: store_parse(db, &hash, &files).await,
                pause: false,
            }
        }
    };
    let cron = json!({"kind": "cron", "expression": "0 4 * * *", "timezone": "UTC",
        "catchup": false});
    db.apply_dag_changes(&[changed(cron, "sha256:b").await], at(12))
        .await
        .unwrap();
    assert_eq!(
        db.live_since("sync").await.unwrap(),
        Some(at(12)),
        "another trigger of its kind is live from now"
    );
    let run_of = json!({"kind": "run_of", "run_of": {"dag": "orders"}});
    db.apply_dag_changes(&[changed(run_of, "sha256:c").await], at(13))
        .await
        .unwrap();
    assert_eq!(
        db.live_since("sync").await.unwrap(),
        None,
        "another kind is paused (decision 459)"
    );
}

#[tokio::test]
async fn a_dags_trigger_has_its_switch_while_it_has_one() {
    // Decision 459: a trigger appearing or changing kind is paused; one of the same kind keeps
    // the switch; none, no switch, and pausing is refused.
    let db = migrated().await;
    let define = |trigger: serde_json::Value, hash: &'static str| {
        let mut changed = dag("sync", "sync.py");
        changed.trigger = serde_json::from_value(trigger).unwrap();
        let db = &db;
        async move {
            let change = DagChange::Define {
                dag: Box::new(changed),
                parse_id: store_parse(db, hash, &files()).await,
                pause: false,
            };
            db.apply_dag_changes(&[change], now()).await.unwrap();
            db.stored_dag(&id("sync")).await.unwrap().unwrap().paused
        }
    };
    let cron = |hour: u8| {
        json!({"kind": "cron", "expression": format!("0 {hour} * * *"), "timezone": "UTC",
            "catchup": false})
    };
    assert_eq!(define(json!({"kind": "none"}), "h1").await, None);
    assert!(
        !db.set_paused(&id("sync"), false).await.unwrap(),
        "no switch"
    );
    assert_eq!(define(cron(3), "h2").await, Some(true), "a new trigger");
    assert!(db.set_paused(&id("sync"), false).await.unwrap());
    assert_eq!(define(cron(4), "h3").await, Some(false), "the same kind");
    let every = json!({"kind": "every", "interval_ms": 60_000, "catchup": false});
    assert_eq!(define(every, "h4").await, Some(false), "a schedule still");
    let chained = json!({"kind": "chained"});
    assert_eq!(define(chained, "h5").await, Some(true), "another kind");
    assert!(db.set_paused(&id("sync"), false).await.unwrap());
    assert_eq!(define(json!({"kind": "none"}), "h6").await, None, "gone");
    assert_eq!(define(cron(3), "h7").await, Some(true), "back, paused");
}

#[test]
fn every_write_of_a_trigger_row_sets_its_next_fire_time() {
    // Decision 485: a scheduled DAG is visited by its stored `next_fire_at`, and nothing sweeps
    // the others; a statement changing a trigger's row (its kind, its switch, its liveness)
    // must clear or set that time with it, or the DAG may never be visited again. Every
    // `INSERT INTO` or `UPDATE` of `dag_triggers` in this crate's code, up to its statement's
    // end, names `next_fire_at`.
    fn sources(dir: &std::path::Path, found: &mut Vec<(std::path::PathBuf, String)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n != "tests") {
                    sources(&path, found);
                }
            } else if path.extension().is_some_and(|e| e == "rs")
                && path.file_name().is_some_and(|n| n != "tests.rs")
            {
                found.push((path.clone(), std::fs::read_to_string(&path).unwrap()));
            }
        }
    }
    let mut files = Vec::new();
    sources(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    let mut writes = 0;
    for (path, text) in files {
        // Statements are split over continued lines: whitespace runs read as one space.
        let text = text.replace("\\\n", " ");
        let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
        for start in ["INSERT INTO dag_triggers", "UPDATE dag_triggers"] {
            for (at, _) in text.match_indices(start) {
                let statement = &text[at..];
                let end = statement.find("\";").unwrap_or(statement.len());
                let statement = &statement[..end];
                assert!(
                    statement.contains("next_fire_at"),
                    "{}: a write of a trigger row leaves its next fire time: {statement}",
                    path.display()
                );
                writes += 1;
            }
        }
    }
    assert_eq!(
        writes, 8,
        "the writes known: SET, the trigger loop's CLAIM and SET if unchanged, PAUSE, SWITCH, \
         CONFLICT_SWITCH, REMOVE_SWITCH, and a posted event's LISTENERS (decision 486)"
    );
}
