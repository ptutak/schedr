use pretty_assertions::assert_eq;

use super::*;

fn logged() -> RunnerMessage {
    RunnerMessage::WatcherMessage {
        watcher: 1,
        message: Box::new(RunnerMessage::HostAlive),
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
    ));
    let blocked = RunnerMessage::LoopBlocked {
        watcher: Some(1),
        stack: "here".to_owned(),
    };
    write_message(&mut theirs, &blocked).await.unwrap();
    write_message(&mut theirs, &logged()).await.unwrap();
    drop(theirs);
    reading.await.unwrap();
    assert_eq!(messages.recv().await, Some(RunnerMessage::HostAlive));
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
