use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn logged() -> RunnerMessage {
    RunnerMessage::WatcherMessage {
        watcher: 1,
        message: Box::new(RunnerMessage::TaskImported),
    }
}

#[tokio::test]
async fn a_route_full_for_as_long_as_silence_is_allowed_ends_the_host() {
    let (ours, mut theirs) = tokio::net::UnixStream::pair().unwrap();
    let (reader, _writer) = ours.into_split();
    let (route, _messages) = mpsc::channel(1);
    let routes = Routes::default();
    routes.lock().unwrap().insert(1, route);
    let stop = Arc::new(Notify::new());
    let going = Arc::new(StdMutex::new(Going::default()));
    let reading = tokio::spawn(read_host(
        reader,
        routes,
        Arc::clone(&stop),
        Arc::clone(&going),
        (Duration::from_millis(50), 1, Arc::new(MaskSet::new(0))),
        (Arc::default(), WatcherMetrics::default()),
    ));
    for _ in 0..2 {
        write_message(&mut theirs, &logged()).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(10), reading)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), stop.notified())
        .await
        .unwrap();
    assert!(going.lock().unwrap().why.as_ref().unwrap().contains("log"));
}

#[tokio::test]
async fn what_a_host_sent_is_read_after_it_said_who_blocked_it() {
    let (ours, mut theirs) = tokio::net::UnixStream::pair().unwrap();
    let (reader, _writer) = ours.into_split();
    let (route, mut messages) = mpsc::channel(4);
    let routes = Routes::default();
    routes.lock().unwrap().insert(1, route);
    let going = Arc::new(StdMutex::new(Going::default()));
    let reading = tokio::spawn(read_host(
        reader,
        routes,
        Arc::new(Notify::new()),
        Arc::clone(&going),
        (Duration::from_secs(30), 1, Arc::new(MaskSet::new(0))),
        (Arc::default(), WatcherMetrics::default()),
    ));
    let blocked = RunnerMessage::LoopBlocked {
        watcher: Some(1),
        stack: "here".to_owned(),
    };
    write_message(&mut theirs, &blocked).await.unwrap();
    write_message(&mut theirs, &logged()).await.unwrap();
    drop(theirs);
    reading.await.unwrap();
    assert_eq!(messages.recv().await, Some(RunnerMessage::TaskImported));
    assert_eq!(going.lock().unwrap().blamed.as_ref().unwrap().watcher, 1);
}

async fn going_after(said: RunnerMessage) -> Going {
    let (ours, mut theirs) = tokio::net::UnixStream::pair().unwrap();
    let (reader, _writer) = ours.into_split();
    let (route, _messages) = mpsc::channel(4);
    let routes = Routes::default();
    routes.lock().unwrap().insert(1, route);
    let going = Arc::new(StdMutex::new(Going::default()));
    let mask = Arc::new(MaskSet::new(0));
    assert!(mask.add("etl/db", "hunter22-pw"));
    let reading = tokio::spawn(read_host(
        reader,
        routes,
        Arc::new(Notify::new()),
        Arc::clone(&going),
        (Duration::from_secs(30), 1, mask),
        (Arc::default(), WatcherMetrics::default()),
    ));
    write_message(&mut theirs, &said).await.unwrap();
    drop(theirs);
    reading.await.unwrap();
    Arc::try_unwrap(going).unwrap().into_inner().unwrap()
}

#[tokio::test]
async fn blame_for_a_watcher_not_on_the_host_blames_no_one() {
    let blocked = RunnerMessage::LoopBlocked {
        watcher: Some(99),
        stack: "here".to_owned(),
    };
    let going = going_after(blocked).await;
    assert!(going.blamed.is_none());
    assert!(going.why.unwrap().contains("here"));
    let stuck = RunnerMessage::WatcherMessage {
        watcher: 99,
        message: Box::new(RunnerMessage::WatcherStuck),
    };
    assert!(going_after(stuck).await.blamed.is_none());
}

#[tokio::test]
async fn a_stack_is_masked_with_what_every_wait_step_on_the_host_read() {
    for watcher in [Some(1), None] {
        let blocked = RunnerMessage::LoopBlocked {
            watcher,
            stack: "connect(hunter22-pw)".to_owned(),
        };
        let going = going_after(blocked).await;
        let text = going
            .blamed
            .and_then(|blamed| blamed.stack)
            .or(going.why)
            .unwrap();
        assert!(!text.contains("hunter22-pw"), "{text}");
        assert!(text.contains("****"), "{text}");
    }
}

#[test]
fn what_a_host_says_of_itself_is_kept_and_counted_once() {
    let stats = StdMutex::new(HostStats::default());
    let metrics = WatcherMetrics::default();
    HostStats::heard(&stats, &metrics, (10, 2, 3, 500));
    HostStats::heard(&stats, &metrics, (12, 40, 3, 1));
    assert_eq!(metrics.thread_stops.get(), 3, "a total, counted once");
    let now = *stats.lock().unwrap();
    assert_eq!(now.threads, 12);
    assert_eq!(now.tied_threads, 12, "no more than it has");
    // A total going down (it is the host's word) takes nothing back, nor counts again.
    HostStats::heard(&stats, &metrics, (12, 1, 1, 1));
    HostStats::heard(&stats, &metrics, (12, 1, 3, 1));
    assert_eq!(metrics.thread_stops.get(), 3);
}

#[test]
fn a_hosts_code_is_its_folders_names() {
    let code = code_of(&["/copies/etl/abc".to_owned(), "/copies/lib/def".to_owned()]);
    assert_eq!(code, "abc,def");
}

#[test]
fn lost_hosts_are_told_until_their_code_is_old_and_counted_by_it_for_good() {
    let hosts = WatcherHosts::default();
    let steady = "/copies/lib/steady".to_owned();
    let old = vec!["/copies/etl/old".to_owned(), steady.clone()];
    let new = vec!["/copies/etl/new".to_owned(), steady];
    let copies = |etl: &str| {
        Copies::from([
            ("etl".to_owned(), PathBuf::from(etl)),
            ("lib".to_owned(), PathBuf::from("/copies/lib/steady")),
        ])
    };
    hosts.code_changed(&copies("/copies/etl/old"));
    hosts.host_lost(&old);
    hosts.host_lost(&old);
    hosts.host_lost(&new);
    hosts.looked_at(&[], &[]);
    let told: Vec<(String, u32, u64)> = hosts
        .reports()
        .iter()
        .map(|r| (r.code.clone(), r.wait_steps, r.hosts_lost))
        .collect();
    assert_eq!(
        told,
        [
            ("new,steady".to_owned(), 0, 1),
            ("old,steady".to_owned(), 0, 2)
        ]
    );
    // One root new is enough, the other unchanged.
    hosts.code_changed(&copies("/copies/etl/new"));
    hosts.looked_at(&[], &[]);
    assert_eq!(hosts.reports().len(), 1);
    assert_eq!(hosts.reports()[0].code, "new,steady");
    let old_code = crate::metrics::CodeLabels {
        code: "old,steady".to_owned(),
    };
    assert_eq!(hosts.metrics.hosts_lost.get_or_create(&old_code).get(), 2);
}

#[test]
fn an_event_whose_key_holds_a_secret_is_refused() {
    // Grok's review of M4.11f: keys masked alike were stored as one.
    let mask = MaskSet::new(4);
    mask.add("token", "tokenAAA-secret");
    let batch = json!([
        {"key": "acct-1", "data": 1, "at": null},
        {"key": "acct-tokenAAA-secret", "data": 2, "at": null}
    ]);
    assert!(super::secret_key(&mask, "acct-tokenAAA-secret", &json!(1), false).is_some());
    assert!(
        super::secret_key(&mask, "acct-1", &batch, true).is_some(),
        "an item's"
    );
    assert!(
        super::secret_key(&mask, "acct-1", &batch, false).is_none(),
        "data unbatched is only data"
    );
    assert!(super::secret_key(&mask, "acct-1", &json!(1), false).is_none());
    assert!(
        super::secret_key(&MaskSet::new(4), "acct-tokenAAA-secret", &json!(1), false).is_none()
    );
}
