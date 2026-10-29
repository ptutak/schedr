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
                warnings: Vec::new(),
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
    let wakes = crate::quiet::Wakes::default();
    discovery.set_wakes(wakes.clone());
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
    // The new DAG woken: the scheduler visits it without waiting for the ID check.
    let woken: Vec<String> = wakes.take().into_keys().map(|d| d.to_string()).collect();
    assert_eq!(woken, ["a"]);
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
    // The newest parse is the root's current one, so its error is listed for the API.
    let errors = discovery.db.current_import_errors().await.unwrap();
    assert_eq!(
        errors
            .iter()
            .map(|e| (
                e.content_hash.as_str(),
                e.path.as_str(),
                e.error.exception.as_str()
            ))
            .collect::<Vec<_>>(),
        [("h2", "a.py", INVALID_DAG_EXCEPTION)]
    );
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

fn key_of(hash: &str, version: &str) -> ParseKey {
    ParseKey {
        schedr_version: version.to_owned(),
        ..key(hash)
    }
}

fn at(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn code(discovery: &Discovery) -> Vec<(String, String)> {
    discovery
        .current_code()
        .map(|(r, h, _, _)| (r.to_owned(), h.to_owned()))
        .collect()
}

fn etl(hash: &str) -> Vec<(String, String)> {
    vec![("etl".to_owned(), hash.to_owned())]
}

/// `w1` reports `h1` (parsed at 08:00, DAG `a` in `a.py`), `w2` reports `h2` (parsed at 08:10,
/// `a` moved to `b.py`).
async fn two_workers() -> Discovery {
    let mut discovery = discovery().await;
    discovery
        .root_reported("w1", key("h1"), at("2026-10-21T08:00:00Z"))
        .await
        .unwrap();
    discovery
        .parse_uploaded(
            upload("h1", vec![("a.py", vec![dag("a", "a.py", 1)])]),
            at("2026-10-21T08:00:00Z"),
        )
        .await
        .unwrap();
    discovery
        .root_reported("w2", key("h2"), at("2026-10-21T08:10:00Z"))
        .await
        .unwrap();
    discovery
        .parse_uploaded(
            upload("h2", vec![("b.py", vec![dag("a", "b.py", 1)])]),
            at("2026-10-21T08:10:00Z"),
        )
        .await
        .unwrap();
    discovery
}

#[tokio::test]
async fn the_newest_code_is_used_and_a_lagging_worker_changes_nothing() {
    let mut discovery = two_workers().await;
    assert_eq!(code(&discovery), etl("h2"));
    let stored = &dags(&discovery).await[0];
    assert_eq!(
        stored.conflict, None,
        "older code on another worker is no conflict"
    );
    assert_eq!(stored.current.as_ref().unwrap().dag.location.path, "b.py");

    // w1 reports its older code again (a reconnect): still h2.
    discovery
        .root_reported("w1", key("h1"), at("2026-10-21T08:20:00Z"))
        .await
        .unwrap();
    assert_eq!(code(&discovery), etl("h2"));

    // w2 goes: only h1 is reported now.
    discovery
        .worker_gone("w2", at("2026-10-21T08:30:00Z"))
        .await
        .unwrap();
    assert_eq!(code(&discovery), etl("h1"));
    // Nobody reports the root any more: it keeps its parse and its DAGs.
    discovery
        .worker_gone("w1", at("2026-10-21T08:40:00Z"))
        .await
        .unwrap();
    discovery
        .worker_gone("w1", at("2026-10-21T08:40:00Z"))
        .await
        .unwrap();
    assert_eq!(code(&discovery), etl("h1"));
    assert!(!dags(&discovery).await[0].removed);
}

#[tokio::test]
async fn a_restarted_master_decides_the_same_whatever_the_order_of_reports() {
    let first = two_workers().await;
    let mut restarted = Discovery::new(
        first.db.clone(),
        SchedrVersion::new(0, 1),
        ParseJobs::new(PARSE_JOB_TIMEOUT),
    );
    restarted
        .root_reported("w2", key("h2"), at("2026-10-21T09:00:00Z"))
        .await
        .unwrap();
    restarted
        .root_reported("w1", key("h1"), at("2026-10-21T09:00:01Z"))
        .await
        .unwrap();
    assert_eq!(code(&restarted), etl("h2"));
}

#[tokio::test]
async fn after_an_upgrade_the_new_parse_takes_over_once_stored() {
    let mut discovery = discovery().await;
    discovery
        .root_reported("w1", key("h1"), now())
        .await
        .unwrap();
    let old = discovery
        .parse_uploaded(
            upload("h1", vec![("a.py", vec![dag("a", "a.py", 1)])]),
            now(),
        )
        .await
        .unwrap();
    let job = discovery
        .root_reported("w2", key_of("h1", "0.2.0"), now())
        .await
        .unwrap();
    assert!(job.is_some(), "a newer version parses the hash again");

    assert_eq!(
        current_parse(&discovery).await,
        old,
        "until the new parse is stored"
    );
    let new = discovery
        .parse_uploaded(
            ParseUpload {
                worker_id: "w2".to_owned(),
                key: key_of("h1", "0.2.0"),
                ..upload("h1", vec![("a.py", vec![dag("a", "a.py", 1)])])
            },
            now(),
        )
        .await
        .unwrap();
    assert_eq!(current_parse(&discovery).await, new);
    // Only the old worker left: the parse of its version is used again.
    discovery.worker_gone("w2", now()).await.unwrap();
    assert_eq!(current_parse(&discovery).await, old);
}

async fn current_parse(discovery: &Discovery) -> String {
    dags(discovery).await[0]
        .current
        .as_ref()
        .unwrap()
        .parse_id
        .clone()
}

#[tokio::test]
async fn a_revert_is_a_switch_and_makes_the_earlier_code_newest() {
    let mut discovery = discovery().await;
    discovery
        .root_reported("w1", key("h1"), at("2026-10-21T08:00:00Z"))
        .await
        .unwrap();
    discovery
        .parse_uploaded(
            upload("h1", vec![("a.py", vec![dag("a", "a.py", 1)])]),
            at("2026-10-21T08:00:00Z"),
        )
        .await
        .unwrap();
    discovery
        .root_reported("w1", key("h2"), at("2026-10-21T08:10:00Z"))
        .await
        .unwrap();
    discovery
        .parse_uploaded(
            upload("h2", vec![("b.py", vec![dag("a", "b.py", 1)])]),
            at("2026-10-21T08:10:00Z"),
        )
        .await
        .unwrap();
    discovery
        .root_reported("w2", key("h2"), at("2026-10-21T08:11:00Z"))
        .await
        .unwrap();
    assert_eq!(code(&discovery), etl("h2"));
    // w1 reverts; w2's git-sync is stuck on h2.
    discovery
        .root_reported("w1", key("h1"), at("2026-10-21T08:20:00Z"))
        .await
        .unwrap();
    assert_eq!(code(&discovery), etl("h1"));
}

#[tokio::test]
async fn a_worker_registering_again_reports_afresh() {
    let mut discovery = two_workers().await;
    assert_eq!(code(&discovery), etl("h2"));
    // w2 restarts without root `etl`: its h2 stops counting.
    let jobs = discovery
        .registered("w2", vec![], at("2026-10-21T09:00:00Z"))
        .await
        .unwrap();
    assert_eq!(jobs, []);
    assert_eq!(code(&discovery), etl("h1"));
}

#[tokio::test]
async fn a_worker_catching_up_through_older_code_doesnt_roll_it_back() {
    let mut discovery = discovery().await;
    let step = |worker: &'static str, hash: &'static str, minute: u32| {
        let at = at(&format!("2026-10-21T08:{minute:02}:00Z"));
        (worker, hash, at)
    };
    let mut parsed = BTreeSet::new();
    for (worker, hash, when) in [
        step("w1", "h0", 0),
        step("w2", "h0", 1),
        step("w1", "h1", 10),
        step("w1", "h2", 20),
        // w2 polls less often: it reaches h1 after w1 reached h2.
        step("w2", "h1", 30),
    ] {
        discovery
            .root_reported(worker, key(hash), when)
            .await
            .unwrap();
        if parsed.insert(hash) {
            discovery
                .parse_uploaded(
                    upload(hash, vec![("a.py", vec![dag("a", "a.py", 1)])]),
                    when,
                )
                .await
                .unwrap();
        }
    }
    assert_eq!(code(&discovery), etl("h2"));
}

#[tokio::test]
async fn a_revert_made_while_disconnected_counts_when_the_worker_registers_again() {
    let mut discovery = discovery().await;
    discovery
        .root_reported("w1", key("h1"), at("2026-10-21T08:00:00Z"))
        .await
        .unwrap();
    discovery
        .parse_uploaded(
            upload("h1", vec![("a.py", vec![dag("a", "a.py", 1)])]),
            at("2026-10-21T08:00:00Z"),
        )
        .await
        .unwrap();
    discovery
        .root_reported("w1", key("h2"), at("2026-10-21T08:10:00Z"))
        .await
        .unwrap();
    discovery
        .parse_uploaded(
            upload("h2", vec![("a.py", vec![dag("a", "a.py", 1)])]),
            at("2026-10-21T08:10:00Z"),
        )
        .await
        .unwrap();
    discovery
        .root_reported("w2", key("h2"), at("2026-10-21T08:11:00Z"))
        .await
        .unwrap();
    discovery
        .registered("w1", vec![key("h1")], at("2026-10-21T08:30:00Z"))
        .await
        .unwrap();
    assert_eq!(code(&discovery), etl("h1"));
}

#[tokio::test]
async fn workers_on_newer_code_whose_parse_is_on_its_way_are_accepted_too() {
    let mut discovery = two_workers().await;
    discovery
        .root_reported("w1", key("h3"), at("2026-10-21T08:20:00Z"))
        .await
        .unwrap();
    let (_, current, accepted, _) = discovery.current_code().next().unwrap();
    assert_eq!(current, "h2");
    assert_eq!(
        accepted.iter().map(String::as_str).collect::<Vec<_>>(),
        ["h2", "h3"]
    );
}

#[tokio::test]
async fn a_draining_worker_takes_no_parse_jobs_and_hands_its_own_over() {
    let mut discovery = discovery().await;
    let job = discovery
        .root_reported("w1", key("h1"), now())
        .await
        .unwrap();
    assert_eq!(job.unwrap().worker_id, "w1");
    discovery
        .root_reported("w2", key("h1"), now())
        .await
        .unwrap();
    let moved = discovery.stop_parsing("w1", now());
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].worker_id, "w2");
    // A new hash from the draining worker waits for another worker to report it.
    assert_eq!(
        discovery
            .root_reported("w1", key("h2"), now())
            .await
            .unwrap(),
        None
    );
    let job = discovery
        .root_reported("w2", key("h2"), now())
        .await
        .unwrap();
    assert_eq!(job.unwrap().worker_id, "w2");
    // Only w1 reports h3 while drained: nobody parses it until w1 is resumed.
    let job = discovery
        .root_reported("w1", key("h3"), now())
        .await
        .unwrap();
    assert_eq!(job, None);
    let jobs = discovery.allow_parsing("w1", now()).await.unwrap();
    let assigned: Vec<(&str, &str)> = jobs
        .iter()
        .map(|j| (j.worker_id.as_str(), j.key.content_hash.as_str()))
        .collect();
    assert_eq!(assigned, [("w1", "h3")]);
    assert_eq!(discovery.allow_parsing("w1", now()).await.unwrap(), []);
}
