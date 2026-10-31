use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tower::ServiceExt;

use schedr_model::logs::{LOG_SIZE_HEADER, RUNNING_HEADER, WORKER_ID_HEADER};

use super::*;

const ATTEMPT: &str = "0199f0c2-7b3e-7d10-8000-00000000a001";

fn endpoint() -> (TempDir, LogEndpoint) {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("logs")).unwrap();
    let endpoint = LogEndpoint {
        worker_id: "w1".to_owned(),
        log_dir: dir.path().join("logs"),
        journal: Journal::new(dir.path().join("journal")),
    };
    (dir, endpoint)
}

/// `GET uri`: status, the three headers and the body.
async fn get(endpoint: &LogEndpoint, uri: &str) -> (u16, [String; 3], Vec<u8>) {
    let response = router(endpoint.clone())
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .map(|v| v.to_str().unwrap().to_owned())
            .unwrap_or_default()
    };
    let headers = [
        header(WORKER_ID_HEADER),
        header(LOG_SIZE_HEADER),
        header(RUNNING_HEADER),
    ];
    let status = response.status().as_u16();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, headers, body.to_vec())
}

fn error(body: &[u8]) -> String {
    serde_json::from_slice::<LogError>(body).unwrap().error
}

#[tokio::test]
async fn a_log_is_read_in_parts_from_an_offset() {
    let (_dir, endpoint) = endpoint();
    std::fs::write(
        endpoint.log_dir.join(format!("{ATTEMPT}.log")),
        "line 1\nline 2\n",
    )
    .unwrap();
    let (status, headers, body) = get(&endpoint, &format!("/logs/{ATTEMPT}")).await;
    assert_eq!(status, 200);
    assert_eq!(headers, ["w1", "14", "false"].map(str::to_owned));
    assert_eq!(body, b"line 1\nline 2\n");

    let (_, headers, body) = get(&endpoint, &format!("/logs/{ATTEMPT}?from=7&limit=4")).await;
    assert_eq!(headers[1], "14");
    assert_eq!(body, b"line");
    // Less than one character is never asked for: a part always moves the reader on.
    let (_, _, body) = get(&endpoint, &format!("/logs/{ATTEMPT}?from=7&limit=1")).await;
    assert_eq!(body, b"line");
    // Past the end: nothing, and the size to wait on.
    let (status, _, body) = get(&endpoint, &format!("/logs/{ATTEMPT}?from=99")).await;
    assert_eq!((status, body.len()), (200, 0));
}

#[tokio::test]
async fn a_running_attempt_says_so() {
    let (_dir, endpoint) = endpoint();
    std::fs::write(endpoint.log_dir.join(format!("{ATTEMPT}.log")), "working").unwrap();
    endpoint
        .journal
        .write(ATTEMPT, &JournalRecord::Running(None))
        .unwrap();
    let (_, headers, _) = get(&endpoint, &format!("/logs/{ATTEMPT}")).await;
    assert_eq!(headers[2], "true");
}

#[tokio::test]
async fn a_missing_log_or_a_bad_request_is_a_json_error_naming_the_worker() {
    let (_dir, endpoint) = endpoint();
    let (status, headers, body) = get(&endpoint, &format!("/logs/{ATTEMPT}")).await;
    assert_eq!((status, headers[0].as_str()), (404, "w1"));
    assert_eq!(
        error(&body),
        format!("worker `w1` has no log of attempt `{ATTEMPT}` (it may have been deleted)")
    );
    let (status, _, body) = get(&endpoint, "/logs/..%2Fjournal").await;
    assert_eq!(status, 400);
    assert_eq!(error(&body), "`../journal` isn't an attempt ID");
    let (status, _, body) = get(&endpoint, &format!("/logs/{ATTEMPT}?from=start")).await;
    assert_eq!(status, 400);
    assert_eq!(
        error(&body),
        "the query takes `from` and `limit`, as numbers of bytes"
    );
}

#[tokio::test]
async fn a_log_that_cant_be_read_is_a_server_error() {
    let (_dir, endpoint) = endpoint();
    std::fs::create_dir(endpoint.log_dir.join(format!("{ATTEMPT}.log"))).unwrap();
    let (status, _, body) = get(&endpoint, &format!("/logs/{ATTEMPT}")).await;
    assert_eq!(status, 500);
    assert!(error(&body).starts_with("worker `w1` can't read the log: "));
}

#[tokio::test]
async fn a_link_or_a_fifo_named_like_a_log_is_never_served() {
    // B62: a task or a local user plants `<uuid>.log`; the endpoint serves logs, not what the
    // worker can read, and doesn't hang on a FIFO no one writes.
    let (dir, endpoint) = endpoint();
    let secret = dir.path().join("secret");
    std::fs::write(&secret, "a private key").unwrap();
    std::os::unix::fs::symlink(&secret, endpoint.log_dir.join(format!("{ATTEMPT}.log"))).unwrap();
    let (status, _, body) = get(&endpoint, &format!("/logs/{ATTEMPT}")).await;
    assert_eq!(status, 500);
    assert!(!String::from_utf8_lossy(&body).contains("a private key"));
    assert!(error(&body).starts_with("worker `w1` can't read the log: "));

    let fifo = "0199f0c2-7b3e-7d10-8000-00000000a002";
    rustix::fs::mkfifoat(
        rustix::fs::CWD,
        endpoint.log_dir.join(format!("{fifo}.log")),
        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
    )
    .unwrap();
    let answer = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        get(&endpoint, &format!("/logs/{fifo}")),
    )
    .await
    .expect("a FIFO hangs the endpoint");
    assert_eq!(answer.0, 500);
}

#[test]
fn attempt_ids_are_uuids() {
    assert!(is_attempt_id(ATTEMPT));
    assert!(is_attempt_id("0199F0C2-7B3E-7D10-8000-00000000A001"));
    assert!(!is_attempt_id("0199f0c2-7b3e-7d10-8000-00000000a00"));
    assert!(!is_attempt_id("0199f0c2a7b3e-7d10-8000-00000000a001"));
    assert!(!is_attempt_id("parse-etl.log"));
    assert!(!is_attempt_id("../../../../../etc/passwd/xxxxxxxxxxxx"));
}
