use std::fs;

use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::*;

const SETTLE: Duration = Duration::from_secs(5);

fn root_with(text: &str) -> TempDir {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("a.py"), text).unwrap();
    dir
}

fn snapshot_of(text: &str) -> (TempDir, Snapshot) {
    let dir = root_with(text);
    let snapshot = scan(dir.path()).unwrap();
    (dir, snapshot)
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

#[test]
fn a_new_scan_waits_for_the_settle_time() {
    let (_dir, snapshot) = snapshot_of("x");
    let start = Instant::now();
    let mut detector = ChangeDetector::new(SETTLE);
    assert_eq!(
        detector.observe(start, snapshot.clone()),
        Observation::Settling(SETTLE)
    );
    assert_eq!(
        detector.observe(start + secs(2), snapshot.clone()),
        Observation::Settling(secs(3))
    );
    assert_eq!(
        detector.observe(start + secs(5), snapshot.clone()),
        Observation::Settled(snapshot.clone())
    );
    detector.accept(snapshot.clone());
    assert_eq!(
        detector.observe(start + secs(30), snapshot),
        Observation::Unchanged
    );
}

#[test]
fn a_further_change_restarts_the_settle_time() {
    let (_one, first) = snapshot_of("x");
    let (_two, second) = snapshot_of("y");
    let start = Instant::now();
    let mut detector = ChangeDetector::new(SETTLE);
    detector.observe(start, first);
    assert_eq!(
        detector.observe(start + secs(4), second.clone()),
        Observation::Settling(SETTLE)
    );
    assert_eq!(
        detector.observe(start + secs(9), second.clone()),
        Observation::Settled(second)
    );
}

#[test]
fn going_back_to_the_settled_scan_cancels_the_change() {
    let (_one, first) = snapshot_of("x");
    let (_two, second) = snapshot_of("y");
    let start = Instant::now();
    let mut detector = ChangeDetector::new(SETTLE);
    detector.accept(first.clone());
    detector.observe(start, second.clone());
    assert_eq!(
        detector.observe(start + secs(1), first),
        Observation::Unchanged
    );
    assert_eq!(
        detector.observe(start + secs(2), second),
        Observation::Settling(SETTLE)
    );
}

#[test]
fn without_a_settle_time_a_change_settles_at_once() {
    let (_dir, snapshot) = snapshot_of("x");
    let mut detector = ChangeDetector::new(Duration::ZERO);
    assert_eq!(
        detector.observe(Instant::now(), snapshot.clone()),
        Observation::Settled(snapshot)
    );
}

#[test]
fn a_watcher_reports_a_hash_once_settled_then_only_new_hashes() {
    let dir = root_with("x");
    let start = Instant::now();
    let mut watcher = RootWatcher::new(dir.path(), SETTLE);
    assert_eq!(watcher.dir(), dir.path());
    assert_eq!(watcher.poll(start).unwrap(), Poll::Settling(SETTLE));
    assert_eq!(watcher.hash(), None);
    let Poll::Changed { hash, snapshot } = watcher.poll(start + SETTLE).unwrap() else {
        panic!("expected a change");
    };
    assert_eq!(watcher.hash(), Some(hash.as_str()));
    assert_eq!(snapshot.python_files(), ["a.py"]);
    assert_eq!(watcher.poll(start + secs(30)).unwrap(), Poll::Unchanged);

    fs::write(dir.path().join("b.py"), "y").unwrap();
    assert_eq!(
        watcher.poll(start + secs(60)).unwrap(),
        Poll::Settling(SETTLE)
    );
    let Poll::Changed { hash: second, .. } = watcher.poll(start + secs(65)).unwrap() else {
        panic!("expected a change");
    };
    assert_ne!(hash, second);
}

#[test]
fn touching_files_without_changing_them_reports_nothing() {
    let dir = root_with("x");
    let start = Instant::now();
    let mut watcher = RootWatcher::new(dir.path(), Duration::ZERO);
    assert!(matches!(watcher.poll(start).unwrap(), Poll::Changed { .. }));
    let file = fs::File::options()
        .write(true)
        .open(dir.path().join("a.py"))
        .unwrap();
    file.set_modified(std::time::SystemTime::UNIX_EPOCH)
        .unwrap();
    assert_eq!(watcher.poll(start + secs(30)).unwrap(), Poll::Unchanged);
}

#[test]
fn scan_errors_are_returned() {
    let dir = TempDir::new().unwrap();
    let mut watcher = RootWatcher::new(dir.path().join("missing"), SETTLE);
    assert!(matches!(
        watcher.poll(Instant::now()),
        Err(ScanError::Root { .. })
    ));
}
