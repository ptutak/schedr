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

#[test]
fn a_waiting_job_goes_to_the_next_worker_that_reports_the_hash() {
    let mut jobs = ParseJobs::new(PARSE_JOB_TIMEOUT);
    jobs.wait(key("h1"));
    jobs.wait(key("h1"));
    assert_eq!(jobs.len(), 1);
    let assigned = jobs.request(key("h1"), "w2", at(0)).unwrap();
    assert_eq!(assigned.worker_id, "w2");
}

#[test]
fn a_retry_waits_its_delay_then_goes_to_another_worker_and_the_delay_doubles() {
    let mut jobs = ParseJobs::new(PARSE_JOB_TIMEOUT);
    let first = jobs.request(key("h1"), "w1", at(0)).unwrap();
    let both: BTreeSet<String> = ["w1".to_owned(), "w2".to_owned()].into();
    jobs.retry(key("h1"), both.clone(), Some("w1"), at(10));
    assert!(jobs.has(&key("h1")));
    // Not before its delay, whether asked by a report or by the round's expiry.
    assert_eq!(jobs.request(key("h1"), "w1", at(39)), None);
    assert_eq!(jobs.expire(at(39)), []);
    let [retry] = jobs.expire(at(40)).try_into().unwrap();
    assert_ne!(retry.job_id, first.job_id);
    assert_eq!((retry.worker_id.as_str(), retry.key), ("w2", key("h1")));
    // Failing again: twice the delay; the only candidate gets it even if it failed.
    jobs.retry(key("h1"), ["w2".to_owned()].into(), Some("w2"), at(50));
    assert_eq!(jobs.expire(at(109)), []);
    let [again] = jobs.expire(at(110)).try_into().unwrap();
    assert_eq!(again.worker_id, "w2");
    // Never longer than the longest delay.
    for _ in 0..10 {
        jobs.retry(key("h1"), both.clone(), None, at(0));
    }
    let longest = i64::try_from(MAX_RETRY_DELAY.as_secs()).unwrap();
    assert_eq!(jobs.expire(at(longest - 1)), []);
    assert_eq!(jobs.expire(at(longest)).len(), 1);
    // A stored parse ends it as any job.
    assert!(jobs.finish(&key("h1")).is_some());
    assert!(!jobs.has(&key("h1")));
}

#[test]
fn a_due_retry_goes_to_the_first_worker_reporting_the_hash() {
    let mut jobs = ParseJobs::new(PARSE_JOB_TIMEOUT);
    // A restarted master: no candidate known yet, the report brings one.
    jobs.retry(key("h1"), BTreeSet::new(), None, at(0));
    assert_eq!(jobs.expire(at(100)), []);
    let job = jobs.request(key("h1"), "w3", at(100)).unwrap();
    assert_eq!(job.worker_id, "w3");
}

fn workers(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|w| (*w).to_owned()).collect()
}

#[test]
fn a_retry_outlives_its_last_candidate_and_goes_to_the_next_one() {
    let mut jobs = ParseJobs::new(PARSE_JOB_TIMEOUT);
    jobs.retry(key("h1"), workers(&["w1"]), Some("w1"), at(0));
    // Its only worker drains: the retry waits for a worker to report the hash again.
    assert_eq!(jobs.worker_lost("w1", at(1)), []);
    assert!(jobs.has(&key("h1")));
    let resumed = jobs.request(key("h1"), "w1", at(30)).unwrap();
    assert_eq!(resumed.worker_id, "w1");
}

#[test]
fn a_retry_goes_to_the_worker_that_failed_only_when_no_other_can_take_it() {
    let mut jobs = ParseJobs::new(PARSE_JOB_TIMEOUT);
    jobs.retry(key("h1"), workers(&["w1", "w2", "w3"]), Some("w1"), at(0));
    let [first] = jobs.expire(at(30)).try_into().unwrap();
    assert_eq!(first.worker_id, "w2");
    // Its worker is lost: the next one that didn't fail, not w1.
    let [next] = jobs.worker_lost("w2", at(31)).try_into().unwrap();
    assert_eq!(next.worker_id, "w3");

    let mut jobs = ParseJobs::new(PARSE_JOB_TIMEOUT);
    jobs.retry(key("h1"), workers(&["w1", "w2"]), Some("w1"), at(0));
    // The worker that failed asks first: it waits for the round to give it to w2.
    assert_eq!(jobs.request(key("h1"), "w1", at(30)), None);
    let [due] = jobs.expire(at(30)).try_into().unwrap();
    assert_eq!(due.worker_id, "w2");
}

#[test]
fn a_worker_reporting_other_code_is_no_candidate_for_the_old() {
    let mut jobs = ParseJobs::new(PARSE_JOB_TIMEOUT);
    jobs.retry(key("h1"), workers(&["w1", "w2"]), Some("w1"), at(0));
    // w2 moved to other code before the retry was due: only w1 still has `h1`.
    jobs.moved_away(&key("h1"), "w2", at(10));
    let [due] = jobs.expire(at(30)).try_into().unwrap();
    assert_eq!(due.worker_id, "w1");
    // The worker it went to moves away too: it is handed out again at the next round.
    jobs.request(key("h1"), "w3", at(31));
    jobs.moved_away(&key("h1"), "w1", at(32));
    let [again] = jobs.expire(at(32)).try_into().unwrap();
    assert_eq!(again.worker_id, "w3");
}

#[test]
fn a_timed_out_retry_skips_the_worker_that_failed_too() {
    let mut jobs = ParseJobs::new(PARSE_JOB_TIMEOUT);
    jobs.retry(key("h1"), workers(&["a", "b", "c"]), Some("b"), at(0));
    let [first] = jobs.expire(at(30)).try_into().unwrap();
    assert_eq!(first.worker_id, "a");
    // `a` never answers: the next in turn is `c`, not `b`, whose parse failed.
    let timeout = i64::try_from(PARSE_JOB_TIMEOUT.as_secs()).unwrap();
    let [next] = jobs.expire(at(30 + timeout)).try_into().unwrap();
    assert_eq!(next.worker_id, "c");
}
