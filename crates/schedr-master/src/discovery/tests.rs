use pretty_assertions::assert_eq;
use schedr_db::dags::StoredDag;
use schedr_model::Dag;
use serde_json::json;

use super::*;
use crate::parse_jobs::PARSE_JOB_TIMEOUT;
use crate::validate::INVALID_DAG_EXCEPTION;

async fn discovery() -> Discovery {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    Discovery::new(
        db,
        SchedrVersion::new(0, 1),
        ParseJobs::new(PARSE_JOB_TIMEOUT),
    )
}

fn now() -> Timestamp {
    "2026-10-21T08:00:00Z".parse().unwrap()
}

fn key(hash: &str) -> ParseKey {
    ParseKey {
        root: "etl".to_owned(),
        content_hash: hash.to_owned(),
        schedr_version: "0.1.0".to_owned(),
    }
}

fn dag(id: &str, path: &str, max_active_runs: u32) -> Dag {
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": id, "location": {"root": "etl", "path": path},
        "trigger": {"kind": "none"}, "initial_since": null,
        "max_active_runs": max_active_runs, "tasks": [], "edges": []
    }))
    .unwrap()
}

fn upload(hash: &str, files: Vec<(&str, Vec<Dag>)>) -> ParseUpload {
    ParseUpload {
        worker_id: "w1".to_owned(),
        key: key(hash),
        files: files
            .into_iter()
            .map(|(path, dags)| ParsedFile {
                path: path.to_owned(),
                dags,
                error: None,
            })
            .collect(),
    }
}

async fn dags(discovery: &Discovery) -> Vec<StoredDag> {
    discovery.db.stored_dags().await.unwrap()
}

#[tokio::test]
async fn a_new_hash_gets_a_parse_job_and_its_upload_defines_the_dags_paused() {
    let mut discovery = discovery().await;
    let job = discovery
        .root_reported("w1", key("h1"), now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((job.worker_id.as_str(), job.key.clone()), ("w1", key("h1")));
    assert_eq!(discovery.jobs().len(), 1);
    assert_eq!(dags(&discovery).await, []);

    let parse_id = discovery
        .parse_uploaded(
            upload("h1", vec![("a.py", vec![dag("a", "a.py", 1)])]),
            now(),
        )
        .await
        .unwrap();
    assert!(discovery.jobs().is_empty());
    let stored = dags(&discovery).await;
    assert_eq!(stored.len(), 1);
    assert!(stored[0].paused);
    assert_eq!(stored[0].current.as_ref().unwrap().parse_id, parse_id);
}

#[tokio::test]
async fn a_hash_parsed_before_needs_no_job() {
    let mut discovery = discovery().await;
    discovery
        .root_reported("w1", key("h1"), now())
        .await
        .unwrap();
    discovery
        .parse_uploaded(
            upload("h1", vec![("a.py", vec![dag("a", "a.py", 1)])]),
            now(),
        )
        .await
        .unwrap();
    discovery
        .root_reported("w1", key("h2"), now())
        .await
        .unwrap();
    discovery
        .parse_uploaded(upload("h2", vec![]), now())
        .await
        .unwrap();
    assert!(dags(&discovery).await[0].removed);

    // Back to the first hash (e.g. a revert): its parse is reused and the DAG comes back.
    assert_eq!(
        discovery
            .root_reported("w1", key("h1"), now())
            .await
            .unwrap(),
        None
    );
    let stored = &dags(&discovery).await[0];
    assert!(!stored.removed);
    assert!(stored.paused);
}

#[tokio::test]
async fn until_a_new_parse_is_stored_the_old_one_stays_in_use() {
    let mut discovery = discovery().await;
    discovery
        .root_reported("w1", key("h1"), now())
        .await
        .unwrap();
    let first = discovery
        .parse_uploaded(
            upload("h1", vec![("a.py", vec![dag("a", "a.py", 1)])]),
            now(),
        )
        .await
        .unwrap();
    assert!(
        discovery
            .root_reported("w1", key("h2"), now())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        dags(&discovery).await[0].current.as_ref().unwrap().parse_id,
        first
    );
}

#[tokio::test]
async fn a_parse_of_a_hash_no_longer_reported_is_stored_but_not_used() {
    let mut discovery = discovery().await;
    discovery
        .root_reported("w1", key("h1"), now())
        .await
        .unwrap();
    discovery
        .root_reported("w1", key("h2"), now())
        .await
        .unwrap();
    let late = discovery
        .parse_uploaded(
            upload("h1", vec![("a.py", vec![dag("a", "a.py", 1)])]),
            now(),
        )
        .await
        .unwrap();
    assert_eq!(dags(&discovery).await, []);
    assert_eq!(
        discovery.db.find_parse("etl", "h1", "0.1.0").await.unwrap(),
        Some(late)
    );
    assert_eq!(discovery.jobs().len(), 1);
}

#[tokio::test]
async fn an_invalid_dag_is_stored_as_an_import_error_and_keeps_the_last_good_version() {
    let mut discovery = discovery().await;
    discovery
        .root_reported("w1", key("h1"), now())
        .await
        .unwrap();
    let good = discovery
        .parse_uploaded(
            upload("h1", vec![("a.py", vec![dag("a", "a.py", 1)])]),
            now(),
        )
        .await
        .unwrap();
    discovery
        .root_reported("w1", key("h2"), now())
        .await
        .unwrap();
    let bad = discovery
        .parse_uploaded(
            upload("h2", vec![("a.py", vec![dag("a", "a.py", 0)])]),
            now(),
        )
        .await
        .unwrap();
    let files = discovery.db.parse_files(&bad).await.unwrap();
    assert_eq!(
        files[0].error.as_ref().unwrap().exception,
        INVALID_DAG_EXCEPTION
    );
    let stored = &dags(&discovery).await[0];
    assert!(!stored.removed);
    assert_eq!(stored.current.as_ref().unwrap().parse_id, good);
}

#[tokio::test]
async fn refreshing_without_changes_stores_nothing() {
    let mut discovery = discovery().await;
    assert_eq!(discovery.refresh(now()).await.unwrap(), []);
    discovery
        .root_reported("w1", key("h1"), now())
        .await
        .unwrap();
    discovery
        .parse_uploaded(
            upload("h1", vec![("a.py", vec![dag("a", "a.py", 1)])]),
            now(),
        )
        .await
        .unwrap();
    assert_eq!(discovery.refresh(now()).await.unwrap(), []);
    discovery.jobs_mut().worker_lost("w1", now());
}
