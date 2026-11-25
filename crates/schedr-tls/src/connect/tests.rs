use super::*;

#[tokio::test]
async fn a_url_that_doesnt_read_is_an_error() {
    let error = channel("not a url", None).await.unwrap_err();
    assert_ne!(error, "");
}

#[derive(Debug, thiserror::Error)]
#[error("transport error")]
struct Outer(#[source] std::io::Error);

#[test]
fn error_text_follows_the_causes() {
    let inner = std::io::Error::other("unknown issuer");
    let outer = std::io::Error::new(std::io::ErrorKind::InvalidData, inner);
    assert_eq!(error_text(&outer), "unknown issuer");
    assert_eq!(
        error_text(&Outer(std::io::Error::other("unknown issuer"))),
        "transport error: unknown issuer"
    );
}

/// An address that answers nothing leaves the next one its time (agy's review of B296).
#[tokio::test]
async fn a_silent_address_leaves_the_next_its_time() {
    let silent = tokio::net::TcpSocket::new_v4().unwrap();
    silent.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let silent = silent.listen(1).unwrap();
    let silent_at = silent.local_addr().unwrap();
    let mut full = Vec::new();
    while let Ok(stream) =
        tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(silent_at)).await
    {
        full.push(stream.unwrap());
        assert!(full.len() < 64, "the listener's queue doesn't fill");
    }
    let live = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live_at = live.local_addr().unwrap();
    let late = || std::io::Error::other("late");
    let until = Instant::now() + Duration::from_secs(2);
    let stream = first_answering(&[silent_at, live_at], until, late)
        .await
        .unwrap();
    assert_eq!(stream.peer_addr().unwrap(), live_at);
    let until = Instant::now() + Duration::from_millis(300);
    let error = first_answering(&[silent_at], until, late)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "late");
}

#[tokio::test]
async fn an_https_url_without_tls_is_refused() {
    let error = channel("https://127.0.0.1:1", None).await.unwrap_err();
    assert!(error.contains("needs TLS"), "{error}");
}
