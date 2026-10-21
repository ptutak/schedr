use pretty_assertions::assert_eq;

use super::*;

fn key(hash: &str) -> ParseKey {
    ParseKey {
        root: "etl".to_owned(),
        content_hash: hash.to_owned(),
        schedr_version: "0.1.0".to_owned(),
    }
}

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_second(1_800_000_000 + seconds).unwrap()
}

#[test]
fn the_first_worker_reporting_a_hash_gets_the_job_once() {
    let mut jobs = ParseJobs::new(PARSE_JOB_TIMEOUT);
    assert!(jobs.is_empty());
    let first = jobs.request(key("h1"), "w1", at(0)).unwrap();
    assert_eq!(
        (first.worker_id.as_str(), first.key.clone()),
        ("w1", key("h1"))
    );
    assert_eq!(jobs.request(key("h1"), "w2", at(1)), None);
    assert_eq!(jobs.request(key("h1"), "w1", at(2)), None);
    let other = jobs.request(key("h2"), "w2", at(3)).unwrap();
    assert_ne!(other.job_id, first.job_id);
    assert_eq!(jobs.len(), 2);
}

#[test]
fn finishing_removes_the_job_and_a_new_report_starts_another() {
    let mut jobs = ParseJobs::new(PARSE_JOB_TIMEOUT);
    let first = jobs.request(key("h1"), "w1", at(0)).unwrap();
    assert_eq!(jobs.finish(&key("h1")), Some(first.job_id.clone()));
    assert_eq!(jobs.finish(&key("h1")), None);
    let again = jobs.request(key("h1"), "w1", at(1)).unwrap();
    assert_ne!(again.job_id, first.job_id);
}

#[test]
fn a_lost_workers_jobs_go_to_another_candidate_or_are_dropped() {
    let mut jobs = ParseJobs::new(PARSE_JOB_TIMEOUT);
    let shared = jobs.request(key("h1"), "w1", at(0)).unwrap();
    jobs.request(key("h1"), "w2", at(0));
    jobs.request(key("h2"), "w1", at(0));
    let moved = jobs.worker_lost("w1", at(5));
    assert_eq!(
        moved,
        [ParseAssignment {
            job_id: shared.job_id,
            worker_id: "w2".to_owned(),
            key: key("h1"),
        }]
    );
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs.worker_lost("w2", at(6)), []);
    assert!(jobs.is_empty());
}

#[test]
fn losing_a_candidate_that_isnt_parsing_changes_nothing() {
    let mut jobs = ParseJobs::new(PARSE_JOB_TIMEOUT);
    jobs.request(key("h1"), "w1", at(0));
    jobs.request(key("h1"), "w2", at(0));
    assert_eq!(jobs.worker_lost("w2", at(1)), []);
    assert_eq!(jobs.len(), 1);
}

#[test]
fn a_job_past_the_timeout_goes_to_the_next_candidate_in_turn() {
    let timeout = Duration::from_mins(10);
    let mut jobs = ParseJobs::new(timeout);
    jobs.request(key("h1"), "w2", at(0));
    jobs.request(key("h1"), "w1", at(0));
    jobs.request(key("h1"), "w3", at(0));
    assert_eq!(jobs.expire(at(599)), []);
    let workers: Vec<String> = [600, 1200, 1800]
        .into_iter()
        .map(|t| jobs.expire(at(t))[0].worker_id.clone())
        .collect();
    assert_eq!(workers, ["w3", "w1", "w2"]);
}

#[test]
fn a_single_candidate_gets_its_timed_out_job_again() {
    let mut jobs = ParseJobs::new(Duration::from_mins(10));
    let first = jobs.request(key("h1"), "w1", at(0)).unwrap();
    let again = jobs.expire(at(600));
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].worker_id, "w1");
    assert_eq!(again[0].job_id, first.job_id);
    assert_eq!(jobs.expire(at(900)), []);
}

#[test]
fn a_waiting_job_goes_to_the_next_worker_that_reports_it() {
    let mut jobs = ParseJobs::new(PARSE_JOB_TIMEOUT);
    jobs.request(key("h1"), "w1", at(0));
    jobs.request(key("h1"), "w2", at(0));
    // Both candidates are lost one after the other: the first loss moves it to w2, the
    // second leaves no candidate, so the job is dropped and the next report starts a new one.
    jobs.worker_lost("w1", at(1));
    jobs.worker_lost("w2", at(2));
    assert!(jobs.request(key("h1"), "w3", at(3)).is_some());
    assert_eq!(jobs.expire(at(4)), []);
}
