use std::sync::Mutex;

use pretty_assertions::assert_eq;
use schedr_proto::pb;

use super::*;

fn finished(attempt_id: &str) -> JournalRecord {
    JournalRecord::Finished(pb::Finished {
        attempt_id: attempt_id.to_owned(),
        ..pb::Finished::default()
    })
}

/// What the writer reported, in order: (attempt, tag, on disk).
type Reported = Arc<Mutex<Vec<(String, &'static str, bool)>>>;

/// A writer over a fresh journal, and what it reported.
fn writer(stall: Duration) -> (tempfile::TempDir, Journal, Writer<&'static str>, Reported) {
    let dir = tempfile::TempDir::new().unwrap();
    let journal = Journal::new(dir.path().join("journal"));
    let reported = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&reported);
    let writer = Writer::start(
        journal.clone(),
        move |w: Written<&'static str>| {
            seen.lock()
                .unwrap()
                .push((w.attempt_id, w.then, w.result.is_ok()));
        },
        stall,
    );
    (dir, journal, writer, reported)
}

fn ids(journal: &Journal) -> Vec<String> {
    let mut ids: Vec<String> = journal
        .records()
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    ids.sort();
    ids
}

#[tokio::test]
async fn each_request_is_on_disk_when_it_is_reported() {
    let (_dir, journal, writer, reported) = writer(Duration::ZERO);
    writer.submit("a1", Op::Write(Box::new(finished("a1"))), "a1 written");
    writer.submit("a2", Op::Write(Box::new(finished("a2"))), "a2 written");
    writer.flush().await;
    assert_eq!(ids(&journal), ["a1", "a2"]);
    let mut seen = reported.lock().unwrap().clone();
    seen.sort();
    assert_eq!(
        seen,
        [
            ("a1".to_owned(), "a1 written", true),
            ("a2".to_owned(), "a2 written", true),
        ]
    );
}

#[tokio::test]
async fn an_attempts_requests_keep_their_order() {
    // Its `running`, then its `finished`, then its removal, handed in at once: each waits for
    // the group after the one before it (in one group, two writes of one file would race, and
    // the removal could come before them).
    let (_dir, journal, writer, reported) = writer(Duration::from_millis(20));
    writer.submit(
        "a1",
        Op::Write(Box::new(JournalRecord::Running(None))),
        "running",
    );
    writer.submit("a1", Op::Write(Box::new(finished("a1"))), "finished");
    writer.submit("a2", Op::Write(Box::new(finished("a2"))), "other");
    writer.flush().await;
    let records = journal.records().unwrap();
    let a1 = records.iter().find(|(id, _)| id == "a1").map(|(_, r)| r);
    assert_eq!(a1, Some(&finished("a1")), "the later write last");
    writer.submit("a1", Op::Remove, "removed");
    writer.submit("a1", Op::Write(Box::new(finished("a1"))), "written again");
    writer.flush().await;
    let order: Vec<&str> = reported
        .lock()
        .unwrap()
        .iter()
        .filter(|(id, _, _)| id == "a1")
        .map(|(_, tag, _)| *tag)
        .collect();
    assert_eq!(order, ["running", "finished", "removed", "written again"]);
    assert_eq!(
        ids(&journal),
        ["a1", "a2"],
        "the write after the removal stays"
    );
}

#[tokio::test]
async fn requests_pending_together_are_written_as_one_group() {
    // A slow disk: the first request's group holds the writer; the five that arrive meanwhile
    // go out together in the next, not one by one (six stalls).
    let stall = Duration::from_millis(100);
    let (_dir, journal, writer, _reported) = writer(stall);
    let began = Instant::now();
    writer.submit("a0", Op::Write(Box::new(finished("a0"))), "first");
    tokio::time::sleep(Duration::from_millis(20)).await;
    for id in ["a1", "a2", "a3", "a4", "a5"] {
        writer.submit(id, Op::Write(Box::new(finished(id))), "later");
    }
    writer.flush().await;
    assert_eq!(ids(&journal).len(), 6);
    assert!(
        began.elapsed() < stall * 4,
        "two groups, not six: {:?}",
        began.elapsed()
    );
}

#[tokio::test]
async fn flush_waits_for_what_is_still_being_written() {
    let (_dir, journal, writer, _reported) = writer(Duration::from_millis(100));
    writer.submit("a1", Op::Write(Box::new(finished("a1"))), "written");
    writer.flush().await;
    assert_eq!(ids(&journal), ["a1"]);
}

#[tokio::test]
async fn a_removal_of_files_never_written_is_no_error() {
    let (_dir, _journal, writer, reported) = writer(Duration::ZERO);
    writer.submit("gone", Op::Remove, "removed");
    writer.flush().await;
    assert_eq!(
        reported.lock().unwrap().clone(),
        [("gone".to_owned(), "removed", true)]
    );
}

#[tokio::test]
async fn another_attempts_request_doesnt_wait_for_a_group_in_flight() {
    // Two groups at once: a2, handed in while a1's group is on a slow disk, is written beside
    // it, not after it (one stall's time for both, not two). The stall long beside the bound's
    // slack: at 300 ms against 500, a loaded `make check` took 673 ms (2026-11-23).
    let stall = Duration::from_secs(1);
    let (_dir, journal, writer, _reported) = writer(stall);
    let began = Instant::now();
    writer.submit("a1", Op::Write(Box::new(finished("a1"))), "first");
    tokio::time::sleep(Duration::from_millis(30)).await;
    writer.submit("a2", Op::Write(Box::new(finished("a2"))), "second");
    writer.flush().await;
    assert_eq!(ids(&journal), ["a1", "a2"]);
    assert!(
        began.elapsed() < Duration::from_millis(1700),
        "beside it, not after it: {:?}",
        began.elapsed()
    );
}
