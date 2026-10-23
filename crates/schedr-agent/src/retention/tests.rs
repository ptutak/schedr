use std::fs::File;

use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::*;
use crate::journal::JournalRecord;

const DAY: Duration = Duration::from_hours(24);

/// A log dir with files `(name, size, age in days)`, and the time they are judged at.
fn logs(files: &[(&str, usize, u64)]) -> (TempDir, Journal, SystemTime) {
    let dir = TempDir::new().unwrap();
    let log_dir = dir.path().join("logs");
    std::fs::create_dir_all(&log_dir).unwrap();
    let now = SystemTime::now();
    for (name, size, days) in files {
        let path = log_dir.join(name);
        std::fs::write(&path, vec![b'x'; *size]).unwrap();
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(now - DAY * u32::try_from(*days).unwrap())
            .unwrap();
    }
    let journal = Journal::new(dir.path().join("journal"));
    (dir, journal, now)
}

fn left(dir: &TempDir) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir.path().join("logs"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn logs_past_their_age_go_unless_the_journal_has_their_attempt() {
    let (dir, journal, now) = logs(&[
        ("old.log", 10, 40),
        ("running.log", 10, 40),
        ("new.log", 10, 1),
        ("parse-etl.log", 10, 31),
        ("notes.txt", 10, 40),
    ]);
    journal
        .write("running", &JournalRecord::Running(None))
        .unwrap();
    let retention = LogRetention {
        max_age: DAY * 30,
        max_size: None,
    };
    let swept = sweep(&dir.path().join("logs"), &journal, retention, now).unwrap();
    assert_eq!(
        swept,
        Swept {
            files: 2,
            bytes: 20,
            kept: 20
        }
    );
    assert_eq!(left(&dir), ["new.log", "notes.txt", "running.log"]);
}

#[test]
fn over_the_size_limit_the_oldest_go_first() {
    let (dir, journal, now) = logs(&[
        ("a.log", 100, 5),
        ("b.log", 100, 4),
        ("c.log", 100, 3),
        ("d.log", 100, 2),
    ]);
    journal.write("a", &JournalRecord::Running(None)).unwrap();
    let retention = LogRetention {
        max_age: DAY * 30,
        max_size: Some(250),
    };
    let swept = sweep(&dir.path().join("logs"), &journal, retention, now).unwrap();
    // `a` is spared (running) but counts: `b` and `c` go to get from 400 to 200 bytes.
    assert_eq!(
        swept,
        Swept {
            files: 2,
            bytes: 200,
            kept: 200
        }
    );
    assert_eq!(left(&dir), ["a.log", "d.log"]);
}

#[test]
fn a_missing_directory_has_nothing_to_sweep() {
    let dir = TempDir::new().unwrap();
    let journal = Journal::new(dir.path().join("journal"));
    let retention = LogRetention {
        max_age: DAY,
        max_size: Some(0),
    };
    let swept = sweep(
        &dir.path().join("logs"),
        &journal,
        retention,
        SystemTime::now(),
    )
    .unwrap();
    assert_eq!(swept, Swept::default());
}

#[test]
fn a_file_that_cant_be_deleted_is_kept() {
    let (dir, journal, now) = logs(&[("old.log", 10, 40)]);
    let log_dir = dir.path().join("logs");
    // A directory named like a log isn't a file: left alone.
    std::fs::create_dir(log_dir.join("dir.log")).unwrap();
    let retention = LogRetention {
        max_age: DAY,
        max_size: None,
    };
    let swept = sweep(&log_dir, &journal, retention, now).unwrap();
    assert_eq!(swept.files, 1);
    assert_eq!(left(&dir), ["dir.log"]);
}

#[tokio::test]
async fn sweep_logs_sweeps_at_once_and_stops() {
    let (dir, journal, _) = logs(&[("old.log", 10, 40), ("new.log", 10, 0)]);
    let retention = LogRetention {
        max_age: DAY,
        max_size: None,
    };
    let metrics = crate::metrics::AgentMetrics::default();
    sweep_logs(
        dir.path().join("logs"),
        journal,
        retention,
        metrics.clone(),
        tokio::time::sleep(Duration::from_millis(300)),
    )
    .await;
    assert_eq!(left(&dir), ["new.log"]);
    assert_eq!(
        (metrics.log_bytes.get(), metrics.log_swept_bytes.get()),
        (10, 10)
    );
}
