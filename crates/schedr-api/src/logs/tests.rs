use pretty_assertions::assert_eq;

use super::*;

#[test]
fn text_of_keeps_a_cut_character_for_the_next_part() {
    let bytes = "zaż".as_bytes();
    assert_eq!(text_of(bytes, false), ("zaż".to_owned(), 4));
    assert_eq!(text_of(&bytes[..3], true), ("za".to_owned(), 2));
    // At the end of a finished log the cut stays, replaced.
    assert_eq!(text_of(&bytes[..3], false), ("za\u{fffd}".to_owned(), 3));
    // Invalid bytes inside are replaced, never held back...
    assert_eq!(text_of(b"a\xffb", true), ("a\u{fffd}b".to_owned(), 3));
    // ... and don't stop a cut character at the end from being held back.
    let mut mixed = b"a\xffb".to_vec();
    mixed.extend_from_slice(&bytes[..3]);
    assert_eq!(text_of(&mixed, true), ("a\u{fffd}bza".to_owned(), 5));
    // A 4-byte character cut after 3, and a stray continuation byte at the end.
    let emoji = "😀".as_bytes();
    assert_eq!(text_of(&emoji[..3], true), (String::new(), 0));
    assert_eq!(text_of(b"a\x80", true), ("a\u{fffd}".to_owned(), 2));
}

#[tokio::test]
async fn a_tls_mode_mismatch_is_refused_before_connecting() {
    let plain = HttpLogSource::new(None);
    let error = plain
        .fetch("https://w1:8081", "a1", 0, 10)
        .await
        .unwrap_err();
    assert_eq!(
        error,
        LogFetchError::Unreachable(
            "it serves logs over https but [tls] mode is `mesh` here: give every role the same \
             [tls] mode"
                .to_owned()
        )
    );
}

#[tokio::test]
async fn an_unreachable_worker_is_reported_with_the_reason() {
    let plain = HttpLogSource::new(None);
    // Port 9 (discard) on loopback: refused at once.
    let LogFetchError::Unreachable(reason) = plain
        .fetch("http://127.0.0.1:9", "a1", 0, 10)
        .await
        .unwrap_err()
    else {
        panic!("expected unreachable");
    };
    assert!(reason.contains("error sending request"), "{reason}");
    // The client is built once and reused.
    assert!(plain.client().is_ok());
}

#[tokio::test]
async fn a_worker_redirecting_isnt_followed() {
    // B133: the log client followed a redirect, also to plain HTTP in app TLS mode.
    let reached = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let elsewhere = {
        let reached = std::sync::Arc::clone(&reached);
        axum::Router::new().route(
            "/{*path}",
            axum::routing::get(move || {
                reached.store(true, std::sync::atomic::Ordering::SeqCst);
                async { "a log from elsewhere" }
            }),
        )
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let other = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, elsewhere).await.unwrap() });
    let worker = axum::Router::new().route(
        "/{*path}",
        axum::routing::get(move || async move {
            axum::response::Redirect::temporary(&format!("http://{other}/log"))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, worker).await.unwrap() });
    let plain = HttpLogSource::new(None);
    let fetched = plain.fetch(&format!("http://{address}"), "a1", 0, 10).await;
    assert!(fetched.is_err(), "{fetched:?}");
    assert!(!reached.load(std::sync::atomic::Ordering::SeqCst));
}
