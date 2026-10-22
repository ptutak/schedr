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
