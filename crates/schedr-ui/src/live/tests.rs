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
        }],
        docs_dir: None,
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
#[case::run_page("deployment=etl&run=r1", &[true, false, false, false, false, true])]
#[case::dag_page("deployment=etl&dag=sync", &[true, true, false, true, false, true])]
#[case::dag_list("kinds=dag", &[false, false, false, true, false, true])]
#[case::runs_list("kinds=run", &[true, true, true, false, false, true])]
#[case::a_dags_runs("kinds=run&dag=sync", &[true, true, false, false, false, true])]
#[case::workers("kinds=workers", &[false, false, false, false, true, true])]
#[case::everything("", &[true, true, true, true, true, true])]
fn a_page_follows_what_it_shows(#[case] query: &str, #[case] wanted: &[bool]) {
    let follow: Follow = serde_urlencoded_query(query);
    let changes = [
        run("r1", "sync"),
        run("r2", "sync"),
        run("r3", "other"),
        Change::Dag {
            dag_id: "sync".to_owned(),
        },
        Change::Workers,
        Change::Refresh,
    ];
    let got: Vec<bool> = changes.iter().map(|c| follow.wants(c)).collect();
    assert_eq!(got, wanted);
}

/// `query` read as axum's `Query` reads it.
fn serde_urlencoded_query(query: &str) -> Follow {
    let uri: axum::http::Uri = format!("/events?{query}").parse().unwrap();
    Query::<Follow>::try_from_uri(&uri).unwrap().0
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

#[tokio::test]
async fn a_pages_changes_are_relayed_and_a_lost_stream_is_a_refresh() {
    let (streams, feeds) = Streams::new(2);
    let app = app(streams);
    let mut body = open(&app, "/events?deployment=etl&run=r1").await;
    let mut pending = String::new();
    // Another run and a DAG: not this page's.
    send(&feeds[0], "run", json!({"run_id": "r2", "dag_id": "sync"})).await;
    send(&feeds[0], "dag", json!({"dag_id": "sync"})).await;
    send(&feeds[0], "run", json!({"run_id": "r1", "dag_id": "sync"})).await;
    assert_eq!(
        next_event(&mut body, &mut pending).await,
        ("change".to_owned(), "etl".to_owned())
    );
    // The webserver's stream ends: the relay opens the next one, and says anything may have
    // changed meanwhile.
    drop(feeds.into_iter().next());
    assert_eq!(
        next_event(&mut body, &mut pending).await,
        ("change".to_owned(), "etl".to_owned())
    );
}

#[tokio::test]
async fn a_running_log_is_tailed_until_it_ends() {
    let (mut streams, _) = Streams::new(0);
    let log = |from: u64, next: u64, running: bool, content: &str| {
        json!({"attempt_id": "a1", "try_number": 2, "worker_id": "w1", "from": from,
               "next": next, "size": next, "running": running, "content": content})
    };
    let path =
        |from: u64| format!("/api/v1/runs/r1/tasks/load/attempts/2/log?from={from}&limit=65536");
    *streams.answers.get_mut().unwrap() = vec![
        (
            path(10),
            200,
            log(10, 30, true, "\u{1b}[31mstep 1\u{1b}[0m <ok>\n"),
        ),
        (path(30), 200, log(30, 42, false, "all done\n")),
    ];
    let app = app(streams);
    let mut body = open(&app, "/d/etl/runs/r1/tasks/load/tail?try=2&from=10").await;
    let mut pending = String::new();
    assert_eq!(
        next_event(&mut body, &mut pending).await,
        (
            "log".to_owned(),
            "<span class=\"fg-1\">step 1</span> &lt;ok&gt;\n".to_owned()
        )
    );
    assert_eq!(
        next_event(&mut body, &mut pending).await,
        ("log".to_owned(), "all done\n".to_owned())
    );
    assert_eq!(
        next_event(&mut body, &mut pending).await,
        ("end".to_owned(), String::new())
    );
    for bad in [
        "/d/etl/runs/r1/tasks/load/tail?from=1",
        "/d/nowhere/runs/r1/tasks/load/tail?try=1&from=0",
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(bad).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert!(response.status().is_client_error(), "{bad}");
    }
}

#[tokio::test]
async fn a_pages_changes_are_coalesced_and_the_last_is_never_dropped() {
    let (streams, feeds) = Streams::new(1);
    let app = app(streams);
    let mut body = open(&app, "/events?deployment=etl&run=r1").await;
    let mut pending = String::new();
    let started = tokio::time::Instant::now();
    send(&feeds[0], "run", json!({"run_id": "r1", "dag_id": "sync"})).await;
    let first = next_event(&mut body, &mut pending).await;
    // Two more within the second: held, then one sent when the second is up.
    send(&feeds[0], "run", json!({"run_id": "r1", "dag_id": "sync"})).await;
    send(&feeds[0], "run", json!({"run_id": "r1", "dag_id": "sync"})).await;
    let second = next_event(&mut body, &mut pending).await;
    assert_eq!(first, ("change".to_owned(), "etl".to_owned()));
    assert_eq!(second, first);
    assert!(started.elapsed() >= COALESCE, "{:?}", started.elapsed());
    // Nothing else is owed.
    let third = tokio::time::timeout(COALESCE * 2, next_event(&mut body, &mut pending)).await;
    assert!(third.is_err(), "{third:?}");
}

#[tokio::test]
async fn a_closed_pages_follower_ends_without_waiting_for_a_change_it_wants() {
    let (changes, _) = broadcast::channel(8);
    let (sender, receiver) = mpsc::channel(4);
    let follow = Follow {
        run: Some("r1".to_owned()),
        ..Follow::default()
    };
    tokio::spawn(forward(
        changes.subscribe(),
        sender,
        follow,
        "etl".to_owned(),
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

#[tokio::test]
async fn the_tail_rides_out_a_worker_out_of_reach_and_says_why_it_stops() {
    let (streams, _) = Streams::new(0);
    let log = |from: u64, next: u64, running: bool, content: &str| {
        json!({"attempt_id": "a1", "try_number": 2, "worker_id": "w1", "from": from,
               "next": next, "size": next, "running": running, "content": content})
    };
    let path =
        |from: u64| format!("/api/v1/runs/r1/tasks/load/attempts/2/log?from={from}&limit=65536");
    *streams.answers.lock().unwrap() = vec![
        (
            path(0),
            503,
            json!({"error": "worker `w1` can't be reached"}),
        ),
        (path(0), 200, log(0, 6, true, "later\n")),
        (path(6), 404, json!({"error": "the log of try 2 is gone"})),
    ];
    let app = app(streams);
    let mut body = open(&app, "/d/etl/runs/r1/tasks/load/tail?try=2&from=0").await;
    let mut pending = String::new();
    assert_eq!(
        next_event(&mut body, &mut pending).await,
        ("log".to_owned(), "later\n".to_owned())
    );
    let (name, data) = next_event(&mut body, &mut pending).await;
    assert_eq!(name, "log");
    assert!(
        data.contains("can't be followed any more: the log of try 2 is gone"),
        "{data}"
    );
    assert_eq!(
        next_event(&mut body, &mut pending).await,
        ("end".to_owned(), String::new())
    );
}

#[tokio::test]
async fn streams_ask_proxies_not_to_buffer_them() {
    let (streams, _) = Streams::new(1);
    let app = app(streams);
    for path in [
        "/events?kinds=dag",
        "/d/etl/runs/r1/tasks/load/tail?try=1&from=0",
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.headers()["x-accel-buffering"], "no", "{path}");
    }
}
