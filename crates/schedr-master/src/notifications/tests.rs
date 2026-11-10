use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use schedr_config::config::{ChannelConfig, ChannelKind, NotificationsSection};
use schedr_db::Database;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::sender::{Backoff, Sender};
use super::*;
use crate::metrics::MasterMetrics;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn section(url: &str) -> NotificationsSection {
    let ops = ChannelConfig {
        kind: ChannelKind::Webhook,
        url: Some(url.to_owned()),
        headers_env: BTreeMap::from([("Authorization".to_owned(), "OPS_TOKEN".to_owned())]),
        body: Some(json!({"text": "[ops] {dag} {event}", "tasks": "{failed_tasks}"})),
        to: Vec::new(),
    };
    NotificationsSection {
        ui_url: Some("https://schedr.example.com/".to_owned()),
        dag_urls: vec!["https://relay.example.com/".to_owned()],
        channels: BTreeMap::from([("ops".to_owned(), ops)]),
        ..NotificationsSection::default()
    }
}

fn settings(url: &str) -> Notifications {
    let env = BTreeMap::from([("OPS_TOKEN".to_owned(), "Bearer t".to_owned())]);
    Notifications::from_config(&section(url), &env).unwrap()
}

fn dag(notify: &serde_json::Value) -> Dag {
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": "etl", "location": {"root": "r", "path": "etl.py"},
        "trigger": {"kind": "cron", "expression": "0 3 * * *", "timezone": "Europe/Warsaw",
                    "catchup": false, "start": null, "end": null},
        "initial_since": null, "max_active_runs": 1, "tasks": [], "edges": [],
        "notify": notify
    }))
    .unwrap()
}

fn relay(url: &str, body: &serde_json::Value) -> serde_json::Value {
    json!({"channel": {"kind": "webhook", "name": "relay", "url": url, "body": body}})
}

#[test]
fn the_settings_read_the_default_events_and_refuse_an_unknown_one() {
    let read = settings("https://hooks.example.com/x");
    assert_eq!(read.default_events, [NotifyEvent::RunFailed]);
    assert_eq!(read.ui_url.as_deref(), Some("https://schedr.example.com"));
    let mut bad = section("https://hooks.example.com/x");
    bad.default_events = vec!["RUN_FAILS".to_owned()];
    let env = BTreeMap::from([("OPS_TOKEN".to_owned(), "t".to_owned())]);
    let error = Notifications::from_config(&bad, &env).unwrap_err();
    assert!(error.contains("unknown event `RUN_FAILS`"), "{error}");
    let mut ftp = section("ftp://hooks.example.com/x");
    ftp.default_events = vec![];
    let error = Notifications::from_config(&ftp, &env).unwrap_err();
    assert!(
        error.contains("[notifications.channels.ops] url"),
        "{error}"
    );
    let mut email = section("https://hooks.example.com/x");
    email.channels.get_mut("ops").unwrap().to = vec!["a@example.com".to_owned()];
    let error = Notifications::from_config(&email, &env).unwrap_err();
    assert!(error.contains("`to` is an email's"), "{error}");
}

#[test]
fn a_dag_s_channels_are_checked_against_the_server_s() {
    let settings = settings("https://hooks.example.com/x");
    let ok = dag(&json!([
        {"channel": {"kind": "server", "name": "ops"}},
        relay("https://relay.example.com/a", &json!({"value": 1}))
    ]));
    assert_eq!(settings.problems(&ok), Vec::<String>::new());

    let bad = dag(&json!([
        {"channel": {"kind": "server", "name": "opz"}},
        relay("https://elsewhere.example.com/a", &json!({"value": 1})),
        {"channel": {"kind": "webhook", "name": "ops", "url": "https://relay.example.com/b"}}
    ]));
    let problems = settings.problems(&bad);
    assert_eq!(problems.len(), 3, "{problems:#?}");
    assert!(problems[0].contains("no server channel `opz`") && problems[0].contains("`ops`"));
    assert!(problems[1].contains("isn't one DAGs may post to"));
    assert!(problems[2].contains("named as a server channel"));

    // No `dag_urls`: DAGs define no webhooks.
    let mut none = settings;
    none.dag_urls.clear();
    let refused = none.problems(&ok);
    assert_eq!(refused.len(), 1);
    assert!(refused[0].contains("dag_urls: none"), "{refused:?}");
}

#[test]
fn an_event_fills_a_body_value_by_value_and_text_with_its_link() {
    let settings = settings("https://hooks.example.com/x");
    let dag = dag(&json!([
        {"channel": {"kind": "server", "name": "ops"}},
        relay("https://relay.example.com/a", &json!({"object": {
            "rows": {"template": [{"text": "rows: "}, {"ref": {"name": "data", "path": ["rows"]}}]},
            "zone": {"ref": {"name": "zone"}},
            "link": {"ref": {"name": "link"}}
        }}))
    ]));
    let mut event = Event::new(&dag, ts("2026-11-09T10:00:00Z"));
    event.run_id = Some("r1");
    event.state = Some("failed");
    event.failed_tasks = vec![FailedTask {
        task_id: "load".into(),
        map_index: 2,
        error: Some("ValueError: \"bad\" row".into()),
    }];
    // Neither chose events: both get the default ones, RUN_FAILED among them.
    let queued = settings.queued(&event, NotifyEvent::RunFailed, &MasterMetrics::default());
    assert_eq!(queued.len(), 2);
    assert_eq!(
        settings.queued(&event, NotifyEvent::RunSucceeded, &MasterMetrics::default()),
        []
    );
    assert_eq!(queued[0].channel, "ops");
    assert_eq!(queued[0].url, None);
    assert_eq!(
        queued[0].body,
        json!({"text": "[ops] etl RUN_FAILED",
               "tasks": [{"task": "load[2]", "error": "ValueError: \"bad\" row"}]})
    );
    // In an event, `data` is null: a path into it is empty text.
    let relay = queued.iter().find(|q| q.channel == "relay").unwrap();
    assert_eq!(relay.url.as_deref(), Some("https://relay.example.com/a"));
    assert_eq!(
        relay.body,
        json!({"rows": "rows: ", "zone": "Europe/Warsaw",
               "link": "https://schedr.example.com/runs/r1"})
    );
}

#[test]
fn a_task_s_message_needs_its_channel_and_the_data_its_body_uses() {
    let settings = settings("https://hooks.example.com/x");
    let dag = dag(&json!([relay(
        "https://relay.example.com/a",
        &json!({"object": {"rows": {"ref": {"name": "data", "path": ["rows"]}},
                            "text": {"ref": {"name": "text"}}}})
    )]));
    let mut event = Event::new(&dag, ts("2026-11-09T10:00:00Z"));
    event.task = Some("load");
    event.text = Some("loaded".into());
    event.data = json!({"rows": 12});
    let sent = settings.message(&event, "relay").unwrap();
    assert_eq!(sent.event, MESSAGE);
    assert_eq!(sent.body, json!({"rows": 12, "text": "loaded"}));
    // A server channel the DAG doesn't name is found too, with its own body.
    assert_eq!(settings.message(&event, "ops").unwrap().channel, "ops");

    event.data = json!({"count": 12});
    let missing = settings.message(&event, "relay").unwrap_err();
    assert!(missing.contains("`data[\"rows\"]`"), "{missing}");
    let unknown = settings.message(&event, "nope").unwrap_err();
    assert!(unknown.contains("no channel `nope`"), "{unknown}");
}

#[test]
fn without_a_body_the_fixed_json_carries_every_value_and_a_text() {
    let mut settings = settings("https://hooks.example.com/x");
    settings.servers.get_mut("ops").unwrap().body = None;
    let dag = dag(&json!([{"channel": {"kind": "server", "name": "ops"},
                           "events": ["TRIGGER_FAILING"]}]));
    let mut event = Event::new(&dag, ts("2026-11-09T10:00:00Z"));
    event.error = Some("ConnectionError: refused\n  more lines".into());
    let queued = settings.queued(
        &event,
        NotifyEvent::TriggerFailing,
        &MasterMetrics::default(),
    );
    assert_eq!(
        queued[0].body,
        json!({
            "event": "TRIGGER_FAILING", "dag": "etl", "run": null, "planned_for": null,
            "state": null,
            "text": "DAG `etl`: its trigger is failing: ConnectionError: refused \
                     https://schedr.example.com/dags/etl",
            "link": "https://schedr.example.com/dags/etl", "failed_tasks": [], "task": null,
            "attempt": null, "time": "2026-11-09T10:00:00Z", "zone": "Europe/Warsaw"
        })
    );
}

#[test]
fn the_wait_doubles_up_to_its_longest() {
    let backoff = Backoff {
        first: Duration::from_secs(10),
        max: Duration::from_secs(60),
    };
    let waits: Vec<u64> = (1..=5).map(|n| backoff.after(n).as_secs()).collect();
    assert_eq!(waits, [10, 20, 40, 60, 60]);
    assert_eq!(backoff.after(200).as_secs(), 60);
}

/// A webhook receiver answering each request with the next of `statuses` (the last one
/// repeated), recording what it got.
struct Receiver {
    url: String,
    got: Arc<Mutex<Vec<(String, String)>>>,
}

async fn receiver(statuses: Vec<(u16, &'static str)>) -> Receiver {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    let got = Arc::new(Mutex::new(Vec::new()));
    let recorded = got.clone();
    tokio::spawn(async move {
        let mut answered = 0;
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            // Headers, then the body by its length.
            loop {
                let read = socket.read(&mut buffer).await.unwrap_or(0);
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                let text = String::from_utf8_lossy(&request);
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let length = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(str::to_owned)
                        })
                        .and_then(|l| l.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if body.len() >= length {
                        break;
                    }
                }
            }
            let text = String::from_utf8_lossy(&request).into_owned();
            let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
            recorded
                .lock()
                .unwrap()
                .push((head.to_owned(), body.to_owned()));
            let (status, extra) = statuses[answered.min(statuses.len() - 1)];
            answered += 1;
            let answer = format!(
                "HTTP/1.1 {status} X\r\ncontent-length: 0\r\nconnection: close\r\n{extra}\r\n"
            );
            let _ = socket.write_all(answer.as_bytes()).await;
        }
    });
    Receiver { url, got }
}

async fn outbox(settings: Notifications) -> (Database, Sender) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let backoff = Backoff {
        first: Duration::from_secs(10),
        max: Duration::from_secs(3600),
    };
    let sender = Sender::new(
        db.clone(),
        Arc::new(settings),
        MasterMetrics::default(),
        backoff,
    )
    .unwrap();
    (db, sender)
}

fn to_ops(body: serde_json::Value) -> NewNotification {
    NewNotification {
        dag_id: "etl".into(),
        channel: "ops".into(),
        kind: "webhook".into(),
        url: None,
        event: "RUN_FAILED".into(),
        body,
    }
}

#[tokio::test]
async fn a_delivered_notification_is_deleted_with_its_headers_sent() {
    let receiver = receiver(vec![(204, "")]).await;
    let (db, sender) = outbox(settings(&receiver.url)).await;
    let now = ts("2026-11-09T10:00:00Z");
    db.queue_notifications(&[to_ops(json!({"text": "hello \"there\""}))], now)
        .await
        .unwrap();
    let pass = sender.pass(now, || true).await.unwrap();
    assert_eq!(pass.sent, 1);
    assert_eq!(
        db.due_notifications(ts("2030-01-01T00:00:00Z"), 10)
            .await
            .unwrap(),
        []
    );
    let got = receiver.got.lock().unwrap().clone();
    assert_eq!(got.len(), 1);
    let head = got[0].0.to_ascii_lowercase();
    assert!(head.starts_with("post /hook "), "{head}");
    assert!(head.contains("authorization: bearer t"), "{head}");
    assert!(head.contains("content-type: application/json"), "{head}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&got[0].1).unwrap(),
        json!({"text": "hello \"there\""})
    );
}

#[tokio::test]
async fn a_failing_notification_waits_longer_each_time_then_is_given_up_on() {
    // A redirect counts as a failure: it isn't followed (decision 425).
    let receiver = receiver(vec![(503, ""), (302, "location: http://127.0.0.1:1/\r\n")]).await;
    let (db, sender) = outbox(settings(&receiver.url)).await;
    let start = ts("2026-11-09T10:00:00Z");
    db.queue_notifications(&[to_ops(json!({}))], start)
        .await
        .unwrap();

    assert_eq!(sender.pass(start, || true).await.unwrap().retried, 1);
    // Not due again before its wait (10 s), then due.
    assert_eq!(
        sender
            .pass(ts("2026-11-09T10:00:09Z"), || true)
            .await
            .unwrap(),
        sender::Pass::default()
    );
    assert_eq!(
        sender
            .pass(ts("2026-11-09T10:00:10Z"), || true)
            .await
            .unwrap()
            .retried,
        1
    );
    let waiting = db
        .due_notifications(ts("2030-01-01T00:00:00Z"), 10)
        .await
        .unwrap();
    assert_eq!(waiting[0].tries, 2);
    // The second wait is 20 s.
    assert_eq!(
        sender
            .pass(ts("2026-11-09T10:00:29Z"), || true)
            .await
            .unwrap(),
        sender::Pass::default()
    );

    // Past `give_up_after` (24 h): dropped and counted.
    let pass = sender
        .pass(ts("2026-11-10T10:00:00Z"), || true)
        .await
        .unwrap();
    assert_eq!(pass.dropped, 1);
    assert_eq!(
        db.due_notifications(ts("2030-01-01T00:00:00Z"), 10)
            .await
            .unwrap(),
        []
    );
    assert_eq!(receiver.got.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn a_dag_webhook_posts_to_its_own_url_without_the_server_s_headers() {
    let receiver = receiver(vec![(200, "")]).await;
    let mut allowed = settings("https://hooks.example.com/unused");
    allowed.dag_urls.push(receiver.url.clone());
    let (db, sender) = outbox(allowed).await;
    let now = ts("2026-11-09T10:00:00Z");
    let own = NewNotification {
        channel: "relay".into(),
        url: Some(receiver.url.clone()),
        ..to_ops(json!({"a": 1}))
    };
    db.queue_notifications(&[own], now).await.unwrap();
    assert_eq!(sender.pass(now, || true).await.unwrap().sent, 1);
    let head = receiver.got.lock().unwrap()[0].0.to_ascii_lowercase();
    assert!(!head.contains("authorization"), "{head}");
}

#[test]
fn a_dag_webhook_no_longer_allowed_isn_t_queued_a_server_channel_gone_is() {
    let mut settings = settings("https://hooks.example.com/x");
    let dag = dag(&json!([
        {"channel": {"kind": "server", "name": "ops"}},
        relay("https://relay.example.com/a", &json!({"value": 1}))
    ]));
    // Checked when its DAG loaded; the settings changed since.
    settings.servers.clear();
    settings.dag_urls = vec!["https://other.example.com/".to_owned()];
    let metrics = MasterMetrics::default();
    let event = Event::new(&dag, ts("2026-11-09T10:00:00Z"));
    let queued = settings.queued(&event, NotifyEvent::RunFailed, &metrics);
    // `ops` waits for a master with it again, with schedr's JSON.
    assert_eq!(queued.len(), 1);
    assert_eq!(
        (queued[0].channel.as_str(), queued[0].url.as_deref()),
        ("ops", None)
    );
    assert_eq!(queued[0].body["event"], "RUN_FAILED");
    let failed = |channel: &str| {
        metrics
            .notifications_failed
            .get_or_create(&crate::metrics::ChannelLabels {
                channel: channel.to_owned(),
            })
            .get()
    };
    assert_eq!((failed("ops"), failed("relay")), (0, 1));
}

#[test]
fn a_dag_url_must_be_within_an_allowed_one_as_the_client_reads_it() {
    for (url, allowed, within) in [
        ("https://a.example.com/hook", "https://a.example.com", true),
        ("https://a.example.com", "https://a.example.com/", true),
        ("https://A.Example.com/hook", "https://a.example.com/", true),
        (
            "https://a.example.com:443/x",
            "https://a.example.com/",
            true,
        ),
        (
            "https://a.example.com/hooks/x",
            "https://a.example.com/hooks",
            true,
        ),
        (
            "https://a.example.com/hooks",
            "https://a.example.com/hooks",
            true,
        ),
        (
            "https://a.example.com.evil/hook",
            "https://a.example.com",
            false,
        ),
        (
            "https://a.example.com@127.0.0.1/admin",
            "https://a.example.com",
            false,
        ),
        (
            "https://user@a.example.com/x",
            "https://a.example.com/",
            false,
        ),
        (
            "https://a.example.com/hooksy",
            "https://a.example.com/hooks",
            false,
        ),
        ("http://a.example.com/x", "https://a.example.com/", false),
        (
            "https://a.example.com:8443/x",
            "https://a.example.com/",
            false,
        ),
        (
            "https://a.example.com/a/../../x",
            "https://a.example.com/a/",
            false,
        ),
        ("not a url", "https://a.example.com/", false),
    ] {
        assert_eq!(url_within(url, allowed), within, "{url} within {allowed}");
    }
}

#[test]
fn a_body_past_its_size_is_refused_before_filling_the_memory() {
    let settings = Notifications {
        max_body: 1000,
        ..settings("https://hooks.example.com/x")
    };
    let big = json!({"ref": {"name": "data"}});
    let dag = dag(&json!([relay(
        "https://relay.example.com/a",
        &json!({"array": [big.clone(), big.clone(), big]})
    )]));
    let mut event = Event::new(&dag, ts("2026-11-09T10:00:00Z"));
    event.data = json!("x".repeat(400));
    let refused = settings.message(&event, "relay").unwrap_err();
    assert!(refused.contains("more than 1000 bytes"), "{refused}");
    event.data = json!("x".repeat(200));
    assert!(settings.message(&event, "relay").is_ok());
    // The fixed JSON too: `ops` has no body here.
    let mut plain = settings;
    plain.servers.get_mut("ops").unwrap().body = None;
    event.data = json!("x".repeat(2000));
    let refused = plain.message(&event, "ops").unwrap_err();
    assert!(refused.contains("more than 1000 bytes"), "{refused}");
}

#[tokio::test]
async fn a_dag_webhook_queued_before_its_url_was_disallowed_is_dropped() {
    let receiver = receiver(vec![(200, "")]).await;
    let mut now_settings = settings("https://hooks.example.com/unused");
    now_settings.dag_urls = vec!["https://only.example.com/".to_owned()];
    let (db, sender) = outbox(now_settings).await;
    let now = ts("2026-11-09T10:00:00Z");
    let own = NewNotification {
        channel: "relay".into(),
        url: Some(receiver.url.clone()),
        ..to_ops(json!({}))
    };
    db.queue_notifications(&[own], now).await.unwrap();
    assert_eq!(sender.pass(now, || true).await.unwrap().dropped, 1);
    assert_eq!(
        db.due_notifications(ts("2030-01-01T00:00:00Z"), 10)
            .await
            .unwrap(),
        []
    );
    assert_eq!(receiver.got.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn a_server_channel_gone_is_kept_until_give_up_after() {
    let mut gone = settings("https://hooks.example.com/unused");
    gone.servers.clear();
    let (db, sender) = outbox(gone).await;
    let now = ts("2026-11-09T10:00:00Z");
    db.queue_notifications(&[to_ops(json!({}))], now)
        .await
        .unwrap();
    assert_eq!(sender.pass(now, || true).await.unwrap().retried, 1);
    assert_eq!(
        sender
            .pass(ts("2026-11-10T10:00:00Z"), || true)
            .await
            .unwrap()
            .dropped,
        1
    );
}

#[tokio::test]
async fn a_refusal_drops_the_notification_and_too_many_requests_is_retried() {
    let receiver = receiver(vec![(429, ""), (400, "")]).await;
    let (db, sender) = outbox(settings(&receiver.url)).await;
    let now = ts("2026-11-09T10:00:00Z");
    db.queue_notifications(&[to_ops(json!({}))], now)
        .await
        .unwrap();
    assert_eq!(sender.pass(now, || true).await.unwrap().retried, 1);
    let pass = sender
        .pass(ts("2026-11-09T10:00:10Z"), || true)
        .await
        .unwrap();
    assert_eq!(pass.dropped, 1, "a 400 won't fix itself");
    assert_eq!(
        db.due_notifications(ts("2030-01-01T00:00:00Z"), 10)
            .await
            .unwrap(),
        []
    );
}

#[tokio::test]
async fn a_master_that_no_longer_leads_starts_no_delivery() {
    let receiver = receiver(vec![(200, "")]).await;
    let (db, sender) = outbox(settings(&receiver.url)).await;
    let now = ts("2026-11-09T10:00:00Z");
    db.queue_notifications(&[to_ops(json!({})), to_ops(json!({}))], now)
        .await
        .unwrap();
    let asked = std::sync::atomic::AtomicUsize::new(0);
    // Leading for the first delivery only.
    let pass = sender
        .pass(now, || {
            asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0
        })
        .await
        .unwrap();
    assert_eq!(pass.sent, 1);
    assert_eq!(receiver.got.lock().unwrap().len(), 1);
    assert_eq!(db.due_notifications(now, 10).await.unwrap().len(), 1);
}

fn with_email(mut section: NotificationsSection, smtp_port: u16) -> NotificationsSection {
    section.smtp = schedr_config::config::SmtpSection {
        host: Some("127.0.0.1".to_owned()),
        port: smtp_port,
        tls: schedr_config::config::SmtpTls::None,
        from: Some("schedr <schedr@example.com>".to_owned()),
        ..Default::default()
    };
    section.channels.insert(
        "oncall".to_owned(),
        ChannelConfig {
            kind: ChannelKind::Email,
            url: None,
            headers_env: BTreeMap::new(),
            body: None,
            to: vec![
                "oncall@example.com".to_owned(),
                "Team A <a@example.com>".to_owned(),
            ],
        },
    );
    section
}

fn email_settings(smtp_port: u16) -> Notifications {
    let env = BTreeMap::from([("OPS_TOKEN".to_owned(), "Bearer t".to_owned())]);
    Notifications::from_config(
        &with_email(section("https://hooks.example.com/x"), smtp_port),
        &env,
    )
    .unwrap()
}

#[test]
fn an_email_channel_needs_the_smtp_server_recipients_and_only_its_keys() {
    let env = BTreeMap::from([("OPS_TOKEN".to_owned(), "t".to_owned())]);
    let check = |change: &dyn Fn(&mut NotificationsSection)| {
        let mut section = with_email(section("https://hooks.example.com/x"), 25);
        change(&mut section);
        Notifications::from_config(&section, &env).map(|_| ())
    };
    assert_eq!(check(&|_| {}), Ok(()));
    for (change, says) in [
        (
            Box::new(|s: &mut NotificationsSection| s.smtp.host = None)
                as Box<dyn Fn(&mut NotificationsSection)>,
            "sends through [notifications.smtp], which has no `host`",
        ),
        (
            Box::new(|s| s.smtp.from = None),
            "`from`, the sender, is needed",
        ),
        (
            Box::new(|s| s.smtp.from = Some("not an address".into())),
            "from: `not an address`",
        ),
        (
            Box::new(|s| s.channels.get_mut("oncall").unwrap().to.clear()),
            "needs its recipients",
        ),
        (
            Box::new(|s| s.channels.get_mut("oncall").unwrap().to = vec!["nope".into()]),
            "to: `nope` isn't an address",
        ),
        (
            Box::new(|s| s.channels.get_mut("oncall").unwrap().url = Some("https://x/".into())),
            "takes `to` only",
        ),
        (
            Box::new(|s| s.smtp.username = Some("schedr".into())),
            "which isn't set",
        ),
        (
            Box::new(|s| {
                s.smtp.username = Some("schedr".into());
                s.smtp.password = Some("pw".into());
            }),
            "would send the password in plain text",
        ),
    ] {
        let error = check(&*change).unwrap_err();
        assert!(error.contains(says), "{says}: {error}");
    }
}

#[test]
fn an_email_is_plain_text_with_the_summary_as_its_subject() {
    let settings = email_settings(25);
    let dag = dag(&json!([{"channel": {"kind": "server", "name": "oncall"}}]));
    assert_eq!(settings.problems(&dag), Vec::<String>::new());
    let mut event = Event::new(&dag, ts("2026-11-09T10:00:00Z"));
    event.run_id = Some("r1");
    event.planned_for = Some(ts("2026-11-09T02:00:00Z"));
    event.failed_tasks = vec![FailedTask {
        task_id: "load".into(),
        map_index: -1,
        error: Some("ValueError: bad row".into()),
    }];
    let queued = settings.queued(&event, NotifyEvent::RunFailed, &MasterMetrics::default());
    assert_eq!(queued.len(), 1);
    let email = &queued[0];
    assert_eq!(
        (email.kind.as_str(), email.url.as_deref()),
        (KIND_EMAIL, None)
    );
    // The recipients are the channel's when it's sent, not stored (Codex's review of M4.13b).
    assert_eq!(email.body.get("to"), None);
    assert_eq!(
        email.body["subject"],
        "DAG `etl`: run r1 failed (failed: load)"
    );
    let text = email.body["text"].as_str().unwrap();
    for line in [
        "DAG `etl`: run r1 failed (failed: load)\n",
        "Event:        RUN_FAILED\n",
        "Planned for:  2026-11-09T02:00:00Z (2026-11-09 03:00 Europe/Warsaw)\n",
        "Link:         https://schedr.example.com/runs/r1\n",
        "Failed tasks:\n  load: ValueError: bad row\n",
    ] {
        assert!(text.contains(line), "{line:?} in:\n{text}");
    }

    // A task's own message: its text as the subject, its data in the body.
    let mut sent = Event::new(&dag, ts("2026-11-09T10:00:00Z"));
    sent.task = Some("report");
    sent.attempt = Some(1);
    sent.text = Some("report ready".into());
    sent.data = json!({"rows": 12});
    let email = settings.message(&sent, "oncall").unwrap();
    assert_eq!(email.body["subject"], "report ready");
    let text = email.body["text"].as_str().unwrap();
    assert!(
        text.contains("Task:         report (attempt 1)\n"),
        "{text}"
    );
    assert!(text.contains("Data:\n{\n  \"rows\": 12\n}\n"), "{text}");
}

/// A mail server on 127.0.0.1 speaking just enough SMTP: answers `RCPT TO` with `rcpt` (250
/// to take it), keeping each message's DATA.
async fn smtp_server(rcpt: &'static str) -> (u16, Arc<Mutex<Vec<String>>>) {
    use tokio::io::AsyncBufReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let got = Arc::new(Mutex::new(Vec::new()));
    let kept = got.clone();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let kept = kept.clone();
            tokio::spawn(async move {
                let (read, mut write) = socket.into_split();
                let mut lines = tokio::io::BufReader::new(read).lines();
                let _ = write.write_all(b"220 test ready\r\n").await;
                while let Ok(Some(line)) = lines.next_line().await {
                    let upper = line.to_ascii_uppercase();
                    let answer: &str = if upper.starts_with("EHLO") || upper.starts_with("HELO") {
                        "250 test\r\n"
                    } else if upper.starts_with("RCPT") {
                        rcpt
                    } else if upper.starts_with("DATA") {
                        let _ = write.write_all(b"354 go on\r\n").await;
                        let mut data = String::new();
                        while let Ok(Some(line)) = lines.next_line().await {
                            if line == "." {
                                break;
                            }
                            data.push_str(&line);
                            data.push('\n');
                        }
                        kept.lock().unwrap().push(data);
                        "250 queued\r\n"
                    } else if upper.starts_with("QUIT") {
                        let _ = write.write_all(b"221 bye\r\n").await;
                        return;
                    } else {
                        "250 ok\r\n"
                    };
                    let _ = write.write_all(answer.as_bytes()).await;
                }
            });
        }
    });
    (port, got)
}

fn to_oncall(settings: &Notifications) -> NewNotification {
    let dag = dag(&json!([{"channel": {"kind": "server", "name": "oncall"}}]));
    let mut event = Event::new(&dag, ts("2026-11-09T10:00:00Z"));
    event.run_id = Some("r1");
    settings
        .queued(&event, NotifyEvent::RunFailed, &MasterMetrics::default())
        .remove(0)
}

#[tokio::test]
async fn an_email_is_mailed_to_every_recipient_and_deleted() {
    let (port, got) = smtp_server("250 ok\r\n").await;
    let settings = email_settings(port);
    let email = to_oncall(&settings);
    let (db, sender) = outbox(settings).await;
    let now = ts("2026-11-09T10:00:00Z");
    db.queue_notifications(&[email], now).await.unwrap();
    assert_eq!(sender.pass(now, || true).await.unwrap().sent, 1);
    assert_eq!(
        db.due_notifications(ts("2030-01-01T00:00:00Z"), 10)
            .await
            .unwrap(),
        []
    );
    let data = got.lock().unwrap()[0].clone();
    for header in [
        "From: schedr <schedr@example.com>",
        "To: oncall@example.com, \"Team A\" <a@example.com>",
        "Subject: DAG `etl`: run r1 failed",
        "Content-Type: text/plain; charset=utf-8",
    ] {
        assert!(data.contains(header), "{header} in:\n{data}");
    }
    assert!(data.contains("Event:        RUN_FAILED"), "{data}");
}

#[tokio::test]
async fn a_mail_server_s_permanent_refusal_drops_it_a_passing_one_retries() {
    for (rcpt, retried, dropped) in [("550 no such user\r\n", 0, 1), ("451 try later\r\n", 1, 0)] {
        let (port, _) = smtp_server(rcpt).await;
        let settings = email_settings(port);
        let email = to_oncall(&settings);
        let (db, sender) = outbox(settings).await;
        let now = ts("2026-11-09T10:00:00Z");
        db.queue_notifications(&[email], now).await.unwrap();
        let pass = sender.pass(now, || true).await.unwrap();
        assert_eq!((pass.retried, pass.dropped), (retried, dropped), "{rcpt}");
    }
}

fn sender_on(db: &Database, settings: Notifications) -> Sender {
    let backoff = Backoff {
        first: Duration::from_secs(10),
        max: Duration::from_secs(3600),
    };
    Sender::new(
        db.clone(),
        Arc::new(settings),
        MasterMetrics::default(),
        backoff,
    )
    .unwrap()
}

fn email_settings_to(smtp_port: u16, to: Option<&[&str]>) -> Notifications {
    let env = BTreeMap::from([("OPS_TOKEN".to_owned(), "Bearer t".to_owned())]);
    let mut section = with_email(section("https://hooks.example.com/x"), smtp_port);
    match to {
        Some(to) => {
            section.channels.get_mut("oncall").unwrap().to =
                to.iter().map(|&a| a.to_owned()).collect();
        }
        None => {
            section.channels.remove("oncall");
        }
    }
    Notifications::from_config(&section, &env).unwrap()
}

#[tokio::test]
async fn an_email_goes_to_its_channel_s_recipients_when_sent_and_nowhere_once_it_is_gone() {
    let (port, got) = smtp_server("250 ok\r\n").await;
    let email = to_oncall(&email_settings(port));
    let (db, _) = outbox(email_settings(port)).await;
    let now = ts("2026-11-09T10:00:00Z");
    db.queue_notifications(&[email], now).await.unwrap();

    // The channel gone: kept, not mailed to the recipients it had.
    let gone = sender_on(&db, email_settings_to(port, None))
        .pass(now, || true)
        .await
        .unwrap();
    assert_eq!((gone.sent, gone.retried), (0, 1));
    assert!(got.lock().unwrap().is_empty());

    // Back with other recipients: mailed to those only.
    let later = ts("2026-11-09T11:00:00Z");
    let changed = sender_on(&db, email_settings_to(port, Some(&["new@example.com"])));
    assert_eq!(changed.pass(later, || true).await.unwrap().sent, 1);
    let data = got.lock().unwrap()[0].clone();
    assert!(data.contains("To: new@example.com\n"), "{data}");
    assert!(!data.contains("oncall@example.com"), "{data}");
}

#[tokio::test]
async fn a_notification_queued_while_its_channel_was_missing_is_mailed_once_it_is_an_email_one() {
    let (port, got) = smtp_server("250 ok\r\n").await;
    let missing = email_settings_to(port, None);
    let queued = to_oncall(&missing);
    assert_eq!(queued.kind, KIND_WEBHOOK);
    let (db, _) = outbox(missing).await;
    let now = ts("2026-11-09T10:00:00Z");
    db.queue_notifications(&[queued], now).await.unwrap();
    let pass = sender_on(&db, email_settings(port))
        .pass(now, || true)
        .await
        .unwrap();
    assert_eq!(pass.sent, 1);
    let data = got.lock().unwrap()[0].clone();
    assert!(data.contains("Subject: DAG `etl`: run r1 failed"), "{data}");
    assert!(data.contains("\"event\": \"RUN_FAILED\""), "{data}");
}

#[tokio::test]
async fn a_mail_server_that_never_answers_is_given_up_on_within_the_timeout() {
    // Takes the connection, never greets.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let held = Arc::new(Mutex::new(Vec::new()));
    let keep = held.clone();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            keep.lock().unwrap().push(socket);
        }
    });
    let env = BTreeMap::from([("OPS_TOKEN".to_owned(), "Bearer t".to_owned())]);
    let mut section = with_email(section("https://hooks.example.com/x"), port);
    section.timeout = Duration::from_millis(200);
    let settings = Notifications::from_config(&section, &env).unwrap();
    let email = to_oncall(&settings);
    let (db, sender) = outbox(settings).await;
    let now = ts("2026-11-09T10:00:00Z");
    db.queue_notifications(&[email], now).await.unwrap();
    let pass = tokio::time::timeout(Duration::from_secs(30), sender.pass(now, || true))
        .await
        .expect("the pass ends")
        .unwrap();
    assert_eq!((pass.sent, pass.retried), (0, 1));
}
