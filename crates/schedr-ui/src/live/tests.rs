use std::collections::VecDeque;
use std::sync::Mutex;

use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use pretty_assertions::assert_eq;
use rstest::rstest;
use schedr_client::{ApiRequest, ApiResponse, Chunks, Client};
use serde_json::json;
use tokio::sync::mpsc;
use tower::ServiceExt;

use super::*;
use crate::{Deployment, router};

type Feed = mpsc::Sender<Result<Vec<u8>, ClientError>>;

/// A webserver whose event streams the test writes (one per `open`, in order) and whose
/// other answers come from a script (path -> status, body).
/// A path scripted more than once answers in that order, the last answer kept.
#[derive(Default)]
struct Streams {
    streams: Mutex<VecDeque<Chunks>>,
    answers: Mutex<Vec<(String, u16, serde_json::Value)>>,
}

impl Streams {
    /// A webserver with `count` streams to open, and their feeds.
    fn new(count: usize) -> (Self, Vec<Feed>) {
        let mut streams = VecDeque::new();
        let mut feeds = Vec::new();
        for _ in 0..count {
            let (feed, chunks) = Chunks::channel();
            streams.push_back(chunks);
            feeds.push(feed);
        }
        let streams = Self {
            streams: Mutex::new(streams),
            answers: Mutex::default(),
        };
        (streams, feeds)
    }
}

impl Transport for Streams {
    async fn send(&self, request: ApiRequest) -> Result<ApiResponse, ClientError> {
        tokio::task::yield_now().await; // a real transport waits for the network
        let mut answers = self.answers.lock().unwrap();
        let matching: Vec<usize> = (0..answers.len())
            .filter(|&i| answers[i].0 == request.path)
            .collect();
        let (status, body) = match matching.as_slice() {
            [] => (404, json!({"error": "not found"})),
            [only] => (answers[*only].1, answers[*only].2.clone()),
            [first, ..] => {
                let (_, status, body) = answers.remove(*first);
                (status, body)
            }
        };
        Ok(ApiResponse { status, body })
    }

    async fn open(&self, _path: String) -> Result<Chunks, ClientError> {
        tokio::task::yield_now().await;
        self.streams
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| ClientError::Unexpected("no more streams".to_owned()))
    }
}

fn app(streams: Streams) -> axum::Router {
    router(Ui {
        deployments: vec![Deployment {
            name: "etl".to_owned(),
            client: Client::new(streams, Duration::ZERO, Duration::ZERO),
            proof: "none",
        }],
        docs_dir: None,
        timings: UiTimings::default(),
        auth: schedr_api::auth::Auth::default(),
        config: serde_json::json!({}),
        process_id: String::new(),
        host: String::new(),
    })
}

async fn send(feed: &Feed, name: &str, data: serde_json::Value) {
    let text = format!("event: {name}\ndata: {data}\n\n");
    feed.send(Ok(text.into_bytes())).await.unwrap();
}

/// The SSE stream of `GET path`.
async fn open(app: &axum::Router, path: &str) -> Body {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    response.into_body()
}

/// The next event's name and data (comments skipped), within 10 s.
async fn next_event(body: &mut Body, pending: &mut String) -> (String, String) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(end) = pending.find("\n\n") {
                let block: String = pending.drain(..end + 2).collect();
                let field = |name: &str| {
                    let values: Vec<&str> = block
                        .lines()
                        .filter_map(|l| l.strip_prefix(name))
                        .map(str::trim_start)
                        .collect();
                    values.join("\n")
                };
                if block.contains("event:") {
                    return (field("event:"), field("data:"));
                }
                continue;
            }
            let frame = body.frame().await.expect("the stream ends").unwrap();
            if let Ok(data) = frame.into_data() {
                pending.push_str(std::str::from_utf8(&data).unwrap());
            }
        }
    })
    .await
    .expect("an event within 10 s")
}

fn run(run_id: &str, dag_id: &str) -> Change {
    Change::Run {
        run_id: run_id.to_owned(),
        dag_id: dag_id.to_owned(),
    }
}

#[rstest]
#[case("run", json!({"run_id": "r1", "dag_id": "sync"}), Some(run("r1", "sync")))]
#[case("dag", json!({"dag_id": "sync"}), Some(Change::Dag { dag_id: "sync".to_owned() }))]
#[case("workers", json!({}), Some(Change::Workers))]
#[case("refresh", json!({}), Some(Change::Refresh))]
#[case("run", json!({"run_id": "r1"}), None)]
#[case("later", json!({}), None)]
fn a_webservers_events_are_read_as_changes(
    #[case] name: &str,
    #[case] data: serde_json::Value,
    #[case] change: Option<Change>,
) {
    let event = ServerEvent {
        name: name.to_owned(),
        data: data.to_string(),
    };
    assert_eq!(Change::of(&event), change);
}

/// The next event, a `change`'s data as JSON (`hello`'s as its number).
async fn next_json(body: &mut Body, pending: &mut String) -> (String, serde_json::Value) {
    let (name, data) = next_event(body, pending).await;
    (name, serde_json::from_str(&data).unwrap())
}

fn change(kind: &str, run: Option<&str>, dag: Option<&str>) -> serde_json::Value {
    json!({"deployment": "etl", "kind": kind, "run": run, "dag": dag})
}

/// A `change` event's data without when it was relayed (checked to be there).
fn without_at(mut data: serde_json::Value) -> serde_json::Value {
    assert!(data["at"].as_i64().is_some(), "{data}");
    data.as_object_mut().unwrap().remove("at");
    data
}

#[tokio::test]
async fn a_browsers_stream_says_hello_then_has_every_change_and_a_lost_stream_is_a_refresh() {
    let (streams, feeds) = Streams::new(2);
    let app = app(streams);
    let before = jiff::Timestamp::now().as_millisecond();
    let mut body = open(&app, "/events").await;
    let mut pending = String::new();
    let (name, now) = next_json(&mut body, &mut pending).await;
    assert_eq!(name, "hello");
    assert!(now.as_i64().unwrap() >= before, "{now}");
    // Every change, each page deciding which concern it (decision 175).
    send(&feeds[0], "run", json!({"run_id": "r2", "dag_id": "sync"})).await;
    send(&feeds[0], "dag", json!({"dag_id": "sync"})).await;
    send(&feeds[0], "workers", json!({})).await;
    for expected in [
        change("run", Some("r2"), Some("sync")),
        change("dag", None, Some("sync")),
        change("workers", None, None),
    ] {
        let (name, data) = next_json(&mut body, &mut pending).await;
        assert_eq!((name.as_str(), without_at(data)), ("change", expected));
    }
    // The webserver's stream ends: the relay opens the next one, and says anything may have
    // changed meanwhile.
    drop(feeds.into_iter().next());
    let (name, data) = next_json(&mut body, &mut pending).await;
    assert_eq!(
        (name.as_str(), without_at(data)),
        ("change", change("refresh", None, None))
    );
}

#[tokio::test]
async fn the_same_change_is_sent_at_most_once_a_second_and_the_last_never_dropped() {
    let (streams, feeds) = Streams::new(1);
    let app = app(streams);
    let mut body = open(&app, "/events").await;
    let mut pending = String::new();
    next_event(&mut body, &mut pending).await;
    let started = tokio::time::Instant::now();
    let r1 = json!({"run_id": "r1", "dag_id": "sync"});
    send(&feeds[0], "run", r1.clone()).await;
    let (_, first) = next_json(&mut body, &mut pending).await;
    assert_eq!(without_at(first), change("run", Some("r1"), Some("sync")));
    // Two more within the second: held, then one sent when the second is up; another
    // change meanwhile isn't held.
    send(&feeds[0], "run", r1.clone()).await;
    send(&feeds[0], "run", r1).await;
    send(&feeds[0], "run", json!({"run_id": "r2", "dag_id": "sync"})).await;
    let (_, other) = next_json(&mut body, &mut pending).await;
    assert_eq!(without_at(other), change("run", Some("r2"), Some("sync")));
    assert!(started.elapsed() < COALESCE, "{:?}", started.elapsed());
    let (_, again) = next_json(&mut body, &mut pending).await;
    assert_eq!(without_at(again), change("run", Some("r1"), Some("sync")));
    assert!(started.elapsed() >= COALESCE, "{:?}", started.elapsed());
    // Nothing else is owed.
    let third = tokio::time::timeout(COALESCE * 2, next_event(&mut body, &mut pending)).await;
    assert!(third.is_err(), "{third:?}");
}

#[tokio::test]
async fn a_closed_browsers_follower_ends_without_waiting_for_a_change() {
    let (changes, _) = broadcast::channel(8);
    let (sender, receiver) = mpsc::channel(4);
    tokio::spawn(forward(
        (changes.subscribe(), false),
        sender,
        "etl".to_owned(),
        UiTimings::default(),
    ));
    tokio::task::yield_now().await;
    assert_eq!(changes.receiver_count(), 1);
    drop(receiver);
    tokio::time::timeout(Duration::from_secs(5), async {
        while changes.receiver_count() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the follower ends");
}

/// `GET path`'s status and body (with `headers`).
async fn get_text(
    app: &axum::Router,
    path: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, String) {
    let mut request = Request::builder().uri(path);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

/// A tail's answer split: the log's HTML, and where the next part is read from (none: the end)
/// with its delay.
fn tail_parts(body: &str) -> (String, Option<(String, String)>) {
    let Some(at) = body.find("<span class=\"log-tail\"") else {
        return (body.to_owned(), None);
    };
    let reader = &body[at..];
    assert!(reader.ends_with("</span>"), "{reader}");
    let attr = |name: &str| {
        let start = reader.find(&format!("{name}=\"")).unwrap() + name.len() + 2;
        let end = start + reader[start..].find('"').unwrap();
        reader[start..end].replace("&amp;", "&")
    };
    (
        body[..at].to_owned(),
        Some((attr("hx-get"), attr("hx-trigger"))),
    )
}

fn log_path(from: u64) -> String {
    format!("/api/v1/runs/r1/tasks/load/attempts/2/log?from={from}&limit=1048576")
}

fn log(from: u64, next: u64, running: bool, content: &str) -> serde_json::Value {
    json!({"attempt_id": "a1", "try_number": 2, "worker_id": "w1", "from": from,
           "next": next, "size": next, "running": running, "content": content})
}

#[tokio::test]
async fn a_tail_asked_from_past_the_logs_end_ends() {
    // Codex: the API puts `next` at the end, before `from`; the tail panicked.
    let (mut streams, _) = Streams::new(0);
    *streams.answers.get_mut().unwrap() = vec![(log_path(100), 200, log(5, 5, false, ""))];
    let app = app(streams);
    let (status, body) = get_text(&app, "/d/etl/runs/r1/tasks/load/tail?try=2&from=100", &[]).await;
    assert_eq!((status, body.as_str()), (StatusCode::OK, ""));
}

#[tokio::test]
async fn a_running_log_is_polled_part_by_part_until_it_ends() {
    let (mut streams, _) = Streams::new(0);
    *streams.answers.get_mut().unwrap() = vec![
        (
            log_path(10),
            200,
            log(10, 30, true, "\u{1b}[31mstep 1 <ok>\n"),
        ),
        (log_path(30), 200, log(30, 42, false, "all done\n")),
    ];
    let app = app(streams);
    let (status, body) = get_text(&app, "/d/etl/runs/r1/tasks/load/tail?try=2&from=10", &[]).await;
    assert_eq!(status, StatusCode::OK);
    let (html, next) = tail_parts(&body);
    assert_eq!(html, "<span class=\"fg-1\">step 1 &lt;ok&gt;\n</span>");
    // Read again in a second, from where it ends, still red.
    let (url, trigger) = next.expect("more to read");
    assert_eq!(url, "/d/etl/runs/r1/tasks/load/tail?try=2&from=30&style=31");
    assert_eq!(trigger, "load delay:1000ms, retry delay:1s");
    let (_, body) = get_text(&app, &url.replace("style=31", "style=31%3B1"), &[]).await;
    assert_eq!(
        tail_parts(&body),
        (
            "<span class=\"fg-1 bold\">all done\n</span>".to_owned(),
            None
        )
    );
    for bad in [
        "/d/etl/runs/r1/tasks/load/tail?from=1",
        "/d/nowhere/runs/r1/tasks/load/tail?try=1&from=0",
    ] {
        let (status, _) = get_text(&app, bad, &[]).await;
        assert!(status.is_client_error(), "{bad}");
    }
}

#[tokio::test]
async fn a_tail_keeps_the_proxys_prefix_the_instance_and_a_cut_escape_for_the_next_read() {
    let (mut streams, _) = Streams::new(0);
    *streams.answers.get_mut().unwrap() = vec![(
        "/api/v1/runs/r1/tasks/load/attempts/2/log?from=0&limit=1048576&map_index=3".to_owned(),
        200,
        log(0, 9, true, "line\n\u{1b}[3"),
    )];
    let app = app(streams);
    let (_, body) = get_text(
        &app,
        "/d/etl/runs/r1/tasks/load/tail?try=2&from=0&map_index=3",
        &[("x-forwarded-prefix", "/schedr/")],
    )
    .await;
    let (html, next) = tail_parts(&body);
    assert_eq!(html, "line\n");
    assert_eq!(
        next.unwrap().0,
        "/schedr/d/etl/runs/r1/tasks/load/tail?try=2&from=6&map_index=3"
    );
}

#[tokio::test]
async fn the_tail_rides_out_a_worker_out_of_reach_and_says_why_it_stops() {
    let (streams, _) = Streams::new(0);
    *streams.answers.lock().unwrap() = vec![
        (
            log_path(0),
            503,
            json!({"error": "worker `w1` can't be reached"}),
        ),
        (log_path(0), 200, log(0, 6, true, "later\n")),
        (
            log_path(6),
            404,
            json!({"error": "the log of try 2 is gone"}),
        ),
        (
            log_path(6),
            503,
            json!({"error": "worker `w1` can't be reached"}),
        ),
    ];
    let app = app(streams);
    let path = "/d/etl/runs/r1/tasks/load/tail?try=2&from=0";
    // Out of reach: nothing shown, read again, remembering since when.
    let (_, body) = get_text(&app, path, &[]).await;
    let (html, next) = tail_parts(&body);
    let url = next.unwrap().0;
    assert_eq!(html, "");
    assert!(url.contains("from=0&out="), "{url}");
    let (_, body) = get_text(&app, &url, &[]).await;
    let (html, next) = tail_parts(&body);
    assert_eq!(html, "later\n");
    let url = next.unwrap().0;
    assert!(!url.contains("out="), "{url}");
    // Gone: said, and nothing more read.
    let (_, body) = get_text(&app, &url, &[]).await;
    let (html, next) = tail_parts(&body);
    assert!(next.is_none(), "{body}");
    assert!(
        html.contains("can't be followed any more: the log of try 2 is gone"),
        "{html}"
    );
    // Out of reach for longer than it waits: said too.
    let long_ago = jiff::Timestamp::now().as_millisecond() - 31_000;
    let (_, body) = get_text(&app, &format!("{url}&out={long_ago}"), &[]).await;
    let (html, next) = tail_parts(&body);
    assert!(next.is_none(), "{body}");
    assert!(
        html.contains("can't be followed any more: worker `w1` can&#39;t be reached"),
        "{html}"
    );
}

#[tokio::test]
async fn the_stream_asks_proxies_not_to_buffer_it() {
    let (streams, _) = Streams::new(1);
    let app = app(streams);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.headers()["x-accel-buffering"], "no");
}

#[tokio::test]
async fn a_change_made_since_the_page_was_rendered_is_sent_when_its_stream_opens() {
    // B144: a stream opened (or reopened) after a change its page didn't see starts with a
    // refresh; one rendered after the last change doesn't.
    let (streams, feeds) = Streams::new(1);
    let app = app(streams);
    let millis = || jiff::Timestamp::now().as_millisecond();
    let refresh = change("refresh", None, None);
    // Before the relay ran, nothing is known: refreshed.
    let rendered = millis() - 1;
    let mut first = open(&app, &format!("/events?since={rendered}")).await;
    let mut pending = String::new();
    next_event(&mut first, &mut pending).await;
    let (_, data) = next_json(&mut first, &mut pending).await;
    assert_eq!(without_at(data), refresh);
    tokio::time::sleep(Duration::from_millis(5)).await;
    let before = millis();
    tokio::time::sleep(Duration::from_millis(5)).await;
    send(&feeds[0], "run", json!({"run_id": "r1", "dag_id": "sync"})).await;
    // Once `first` has the change, the relay recorded it.
    next_event(&mut first, &mut pending).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let after = millis();
    let mut missed = open(&app, &format!("/events?since={before}")).await;
    let mut pending = String::new();
    next_event(&mut missed, &mut pending).await;
    let (_, data) = next_json(&mut missed, &mut pending).await;
    assert_eq!(without_at(data), refresh);
    let mut current = open(&app, &format!("/events?since={after}")).await;
    let mut pending = String::new();
    assert_eq!(next_event(&mut current, &mut pending).await.0, "hello");
    let none = tokio::time::timeout(COALESCE, next_event(&mut current, &mut pending)).await;
    assert!(none.is_err(), "{none:?}");
}
