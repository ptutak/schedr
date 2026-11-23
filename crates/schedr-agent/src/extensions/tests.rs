use std::os::unix::fs::PermissionsExt as _;
use std::time::Duration;

use pretty_assertions::assert_eq;
use tempfile::TempDir;

use schedr_model::extension::EventKind;

use super::*;

fn script(dir: &Path, name: &str, body: &str, mode: u32) -> PathBuf {
    let file = dir.join(name);
    std::fs::write(&file, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(mode)).unwrap();
    file
}

fn path_of(dirs: &[&Path]) -> String {
    std::env::join_paths(
        dirs.iter()
            .copied()
            .chain([Path::new("/usr/bin"), Path::new("/bin")]),
    )
    .unwrap()
    .into_string()
    .unwrap()
}

#[test]
fn a_description_is_one_json_line_this_worker_can_use() {
    let good = Description::parse(
        "{\"name\": \"spark\", \"versions\": [\"1.0\", \"1.1\"], \"protocol\": 1}\nmore",
    )
    .unwrap();
    assert_eq!(good.name, "spark");
    assert_eq!(good.versions, ["1.0", "1.1"]);
    for (out, why) in [
        ("not json", "isn't one JSON line"),
        (
            "{\"name\": \"spark\", \"versions\": [], \"protocol\": 1}",
            "no versions",
        ),
        (
            "{\"name\": \"spark\", \"versions\": [\"1\"], \"protocol\": 2}",
            "speaks protocol 2",
        ),
        (
            "{\"name\": \"a b\", \"versions\": [\"1\"], \"protocol\": 1}",
            "a name is",
        ),
        (
            "{\"name\": \"x\", \"versions\": [\"1\"], \"protocol\": 1, \"extra\": 1}",
            "isn't one JSON line",
        ),
    ] {
        let error = Description::parse(out).unwrap_err();
        assert!(error.contains(why), "{out}: {error}");
    }
}

#[test]
fn discovery_finds_each_executable_name_once_in_path_order() {
    let first = TempDir::new().unwrap();
    let second = TempDir::new().unwrap();
    let a = script(first.path(), "schedr-ext-a", "", 0o755);
    script(second.path(), "schedr-ext-a", "", 0o755);
    let b = script(second.path(), "schedr-ext-b", "", 0o755);
    script(second.path(), "schedr-ext-c", "", 0o644);
    script(second.path(), "schedr-ext-", "", 0o755);
    script(second.path(), "other", "", 0o755);
    let found = discoverable(&path_of(&[first.path(), second.path()]));
    assert_eq!(found, [a, b]);
}

#[tokio::test]
async fn a_plugin_is_asked_with_describe_and_one_failing_is_skipped() {
    let dir = TempDir::new().unwrap();
    script(
        dir.path(),
        "schedr-ext-spark",
        r#"[ "$1" = --describe ] && echo '{"name": "spark", "versions": ["1.0"], "protocol": 1}'"#,
        0o755,
    );
    script(dir.path(), "schedr-ext-broken", "exit 3", 0o755);
    script(
        dir.path(),
        "schedr-ext-taken",
        r#"echo '{"name": "taken", "versions": ["9"], "protocol": 1}'"#,
        0o755,
    );
    let path = path_of(&[dir.path()]);
    let configured = Plugins::from([(
        "taken".to_owned(),
        Plugin {
            command: vec!["/opt/taken".to_owned()],
            versions: BTreeSet::from(["1".to_owned()]),
            declarations: schedr_model::extension::Declarations::default(),
            max_series: schedr_model::extension::DEFAULT_MAX_SERIES,
            service: None,
        },
    )]);
    assert_eq!(
        plugins(configured.clone(), false, &path).await,
        (configured.clone(), Vec::new()),
        "no discovery unless asked"
    );
    let (found, events) = plugins(configured, true, &path).await;
    assert_eq!(found.keys().collect::<Vec<_>>(), ["spark", "taken"]);
    // What happened to each, for its extension's page (DN19): the broken one named by its file.
    let mut happened: Vec<(&str, EventKind)> = events
        .iter()
        .map(|e| (e.extension.as_str(), e.kind))
        .collect();
    happened.sort();
    assert_eq!(
        happened,
        [
            ("broken", EventKind::Skipped),
            ("spark", EventKind::Discovered),
            ("taken", EventKind::Skipped)
        ]
    );
    assert!(events.iter().all(|e| e.version.is_none() && e.at_ms > 0));
    assert_eq!(
        found["taken"].command,
        ["/opt/taken"],
        "the configuration wins for its name"
    );
    assert_eq!(
        found["spark"].command,
        [dir.path()
            .join("schedr-ext-spark")
            .to_string_lossy()
            .into_owned()]
    );
    assert_eq!(
        offers(&found),
        [
            pb::ExtensionOffer {
                name: "spark".into(),
                versions: vec!["1.0".into()]
            },
            pb::ExtensionOffer {
                name: "taken".into(),
                versions: vec!["1".into()]
            },
        ]
    );
}

#[tokio::test]
async fn a_plugin_slow_to_describe_itself_is_skipped() {
    tokio::time::pause();
    let dir = TempDir::new().unwrap();
    let slow = script(dir.path(), "schedr-ext-slow", "sleep 30", 0o755);
    let path = path_of(&[dir.path()]);
    let asked = tokio::spawn(async move { describe(&slow, &path).await });
    let error = asked.await.unwrap().unwrap_err();
    assert!(error.contains("took over 5s"), "{error}");
}

#[tokio::test]
async fn describe_reads_no_more_than_its_limit_and_leaves_nothing_running() {
    let dir = TempDir::new().unwrap();
    let path = path_of(&[dir.path()]);
    // Prints past the limit and would go on: refused at the limit, not after buffering it all.
    let loud = script(
        dir.path(),
        "schedr-ext-loud",
        "yes describe | head -c 200000; sleep 30",
        0o755,
    );
    let started = std::time::Instant::now();
    let error = describe(&loud, &path).await.unwrap_err();
    assert!(error.contains("over 64 KiB"), "{error}");
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "{:?}",
        started.elapsed()
    );
    // Answers and leaves a child behind: the child is gone once it answered.
    let pid_file = dir.path().join("child.pid");
    let body = format!(
        "sleep 120 >/dev/null 2>&1 &\necho $! > {}\n\
         echo '{{\"name\": \"orphan\", \"versions\": [\"1\"], \"protocol\": 1}}'",
        pid_file.display()
    );
    let orphan = script(dir.path(), "schedr-ext-orphan", &body, 0o755);
    assert_eq!(describe(&orphan, &path).await.unwrap().name, "orphan");
    let pid: i32 = std::fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let pid = rustix::process::Pid::from_raw(pid).unwrap();
    let mut gone = false;
    for _ in 0..50 {
        if rustix::process::test_kill_process(pid).is_err() {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(gone, "the child it started is killed with its group");
}

#[test]
fn a_discovered_version_takes_the_rule_a_task_does() {
    let error =
        Description::parse("{\"name\": \"spark\", \"versions\": [\"1 0\"], \"protocol\": 1}")
            .unwrap_err();
    assert!(error.contains("version `1 0`"), "{error}");
}
