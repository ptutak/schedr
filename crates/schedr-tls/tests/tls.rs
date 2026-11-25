//! mTLS over real connections on loopback: the gRPC channel to a TLS server, the HTTPS
//! endpoint with its fixed worker name, refusals (another CA, no client certificate, a wrong
//! name) and rotation.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use rustls::pki_types::ServerName;
use rustls::pki_types::pem::PemObject;
use schedr_tls::testing::TestCa;
use schedr_tls::{Protocol, Reload, Tls, TlsFiles, channel, incoming};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;

fn tls(ca: &TestCa, dir: &TempDir, prefix: &str, names: &[&str]) -> Tls {
    Tls::load(
        ca.write(dir.path(), prefix, names),
        "schedr-worker",
        "schedr-ui",
    )
    .unwrap()
}

/// A gRPC server with no services on a free port, over `tls`: its address.
async fn grpc_server(tls: Option<Tls>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let incoming = incoming(
        listener,
        tls,
        Protocol::Grpc,
        schedr_tls::incoming::ACCEPT_RETRY,
    )
    .unwrap();
    let address = incoming.local_addr();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_routes(tonic::service::Routes::default())
            .serve_with_incoming(incoming),
    );
    address
}

/// An HTTP endpoint answering `ok` on a free port, over `tls`: its address.
async fn http_server(tls: Option<Tls>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let incoming = incoming(
        listener,
        tls,
        Protocol::Http,
        schedr_tls::incoming::ACCEPT_RETRY,
    )
    .unwrap();
    let address = incoming.local_addr();
    tokio::spawn(async move {
        axum::serve(incoming, Router::new().route("/", get(|| async { "ok" })))
            .await
            .unwrap();
    });
    address
}

/// `GET /` over TLS with `config`, connecting to `address` as `name`: the response.
async fn get_over_tls(
    config: Arc<rustls::ClientConfig>,
    address: std::net::SocketAddr,
    name: &str,
) -> std::io::Result<String> {
    let tcp = TcpStream::connect(address).await?;
    let name = ServerName::try_from(name.to_owned()).unwrap();
    let mut stream = TlsConnector::from(config).connect(name, tcp).await?;
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: worker\r\nConnection: close\r\n\r\n")
        .await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    Ok(response)
}

#[tokio::test]
async fn a_worker_reaches_the_master_over_mtls() {
    let dir = TempDir::new().unwrap();
    let ca = TestCa::new("schedr CA");
    let master = tls(&ca, &dir, "master", &["127.0.0.1"]);
    let worker = tls(&ca, &dir, "worker", &["schedr-worker"]);
    let address = grpc_server(Some(master)).await;
    channel(&format!("https://{address}"), Some(&worker))
        .await
        .unwrap();
}

#[tokio::test]
async fn plain_grpc_needs_no_certificates() {
    let address = grpc_server(None).await;
    channel(&format!("http://{address}"), None).await.unwrap();
}

#[tokio::test]
async fn a_master_certificate_from_another_ca_or_for_another_name_is_refused() {
    let dir = TempDir::new().unwrap();
    let ca = TestCa::new("schedr CA");
    let worker = tls(&ca, &dir, "worker", &["schedr-worker"]);

    let other = TestCa::new("someone else");
    let address = grpc_server(Some(tls(&other, &dir, "foreign", &["127.0.0.1"]))).await;
    let error = channel(&format!("https://{address}"), Some(&worker))
        .await
        .unwrap_err();
    assert!(error.contains("UnknownIssuer"), "{error}");

    let address = grpc_server(Some(tls(&ca, &dir, "elsewhere", &["schedr-master"]))).await;
    let error = channel(&format!("https://{address}"), Some(&worker))
        .await
        .unwrap_err();
    assert!(error.contains("certificate not valid for name"), "{error}");
}

#[tokio::test]
async fn a_worker_from_another_ca_is_refused_by_the_master() {
    let dir = TempDir::new().unwrap();
    let ca = TestCa::new("schedr CA");
    let address = grpc_server(Some(tls(&ca, &dir, "master", &["127.0.0.1"]))).await;
    // It trusts the master's CA, but its own certificate comes from another one.
    let foreign = TestCa::new("someone else").write(dir.path(), "foreign", &["schedr-worker"]);
    std::fs::write(&foreign.ca, ca.pem()).unwrap();
    let worker = Tls::load(foreign, "schedr-worker", "schedr-ui").unwrap();
    // TLS 1.3: the client finishes its side of the handshake, and the master's refusal (an
    // alert) arrives with the first read.
    let tcp = TcpStream::connect(address).await.unwrap();
    let name = ServerName::try_from("127.0.0.1").unwrap();
    let mut stream = TlsConnector::from(worker.grpc_client())
        .connect(name, tcp)
        .await
        .unwrap();
    stream
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .unwrap();
    let mut buffer = [0; 64];
    let error = stream.read(&mut buffer).await.unwrap_err();
    assert!(error.to_string().contains("UnknownCA"), "{error}");
}

/// Decision 520: the master's gRPC port takes only a certificate naming `[tls] worker_name`:
/// another role's, though the CA signed it (the webserver's, `schedr ui`'s), can't connect as a
/// worker. A per-worker certificate names both (`schedr-worker` and `<id>.schedr-worker`).
#[tokio::test]
async fn only_a_workers_certificate_reaches_the_masters_grpc_port() {
    let dir = TempDir::new().unwrap();
    let ca = TestCa::new("schedr CA");
    let address = grpc_server(Some(tls(&ca, &dir, "master", &["127.0.0.1"]))).await;
    let per_pod = tls(&ca, &dir, "pod", &["schedr-worker", "w-7f9c.schedr-worker"]);
    channel(&format!("https://{address}"), Some(&per_pod))
        .await
        .unwrap();
    for (prefix, names) in [
        ("webserver", &["schedr-webserver"][..]),
        ("ui", &["schedr-ui"][..]),
    ] {
        let other = tls(&ca, &dir, prefix, names);
        // TLS 1.3: the refusal arrives with the first read, as for another CA's.
        let tcp = TcpStream::connect(address).await.unwrap();
        let name = ServerName::try_from("127.0.0.1").unwrap();
        let mut stream = TlsConnector::from(other.grpc_client())
            .connect(name, tcp)
            .await
            .unwrap();
        stream
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        let mut buffer = [0; 64];
        let error = stream.read(&mut buffer).await.unwrap_err();
        assert!(
            error.to_string().contains("BadCertificate"),
            "{prefix}: {error}"
        );
    }
}

#[tokio::test]
async fn the_log_endpoint_is_checked_against_the_worker_name_whatever_the_address() {
    let dir = TempDir::new().unwrap();
    let ca = TestCa::new("schedr CA");
    let worker = tls(&ca, &dir, "worker", &["schedr-worker"]);
    let webserver = tls(&ca, &dir, "webserver", &["schedr-webserver"]);
    let address = http_server(Some(worker)).await;

    // Connected by IP, verified as `schedr-worker`.
    let response = get_over_tls(webserver.worker_client(), address, "10.42.1.7")
        .await
        .unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("ok"), "{response}");

    // A worker certificate without the name is refused.
    let unnamed = http_server(Some(tls(&ca, &dir, "unnamed", &["10.42.1.7"]))).await;
    let error = get_over_tls(webserver.worker_client(), unnamed, "10.42.1.7")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not valid for name \"schedr-worker\""),
        "{error}"
    );
}

#[tokio::test]
async fn the_log_endpoint_refuses_a_client_without_a_certificate() {
    let dir = TempDir::new().unwrap();
    let ca = TestCa::new("schedr CA");
    let address = http_server(Some(tls(&ca, &dir, "worker", &["schedr-worker"]))).await;
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from_pem_slice(ca.pem().as_bytes()).unwrap())
        .unwrap();
    let anonymous = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    // TLS 1.3 refuses after the handshake: the request gets no answer.
    let answer = get_over_tls(Arc::new(anonymous), address, "schedr-worker").await;
    assert!(
        answer.as_ref().is_err() || answer.as_ref().is_ok_and(String::is_empty),
        "{answer:?}"
    );
}

#[tokio::test]
async fn plain_http_works_without_tls() {
    let address = http_server(None).await;
    let mut tcp = TcpStream::connect(address).await.unwrap();
    tcp.write_all(b"GET / HTTP/1.1\r\nHost: w\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    tcp.read_to_string(&mut response).await.unwrap();
    assert!(response.ends_with("ok"), "{response}");
}

#[tokio::test]
async fn rotated_certificates_are_used_by_new_connections() {
    let dir = TempDir::new().unwrap();
    let old = TestCa::new("old CA");
    let new = TestCa::new("new CA");
    // The master starts on the old CA's certificate and trusts both CAs' clients.
    let files = old.write(dir.path(), "master", &["127.0.0.1"]);
    std::fs::write(&files.ca, format!("{}{}", old.pem(), new.pem())).unwrap();
    let master = Tls::load(files.clone(), "schedr-worker", "schedr-ui").unwrap();
    let address = grpc_server(Some(master.clone())).await;
    // The worker knows only the new CA.
    let worker = tls(&new, &dir, "worker", &["schedr-worker"]);
    let url = format!("https://{address}");
    assert!(channel(&url, Some(&worker)).await.is_err());

    let issued = new.issue(&["127.0.0.1"]);
    std::fs::write(&files.cert, issued.cert).unwrap();
    std::fs::write(&files.key, issued.key).unwrap();
    assert!(matches!(master.reload(), Reload::Reloaded));
    channel(&url, Some(&worker)).await.unwrap();
}

#[tokio::test]
async fn a_silent_client_holds_up_no_one() {
    let dir = TempDir::new().unwrap();
    let ca = TestCa::new("schedr CA");
    let master = tls(&ca, &dir, "master", &["127.0.0.1"]);
    let worker = tls(&ca, &dir, "worker", &["schedr-worker"]);
    let address = grpc_server(Some(master)).await;
    // Connected, never says anything.
    let _silent = TcpStream::connect(address).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        channel(&format!("https://{address}"), Some(&worker)),
    )
    .await
    .unwrap()
    .unwrap();
}

#[test]
fn tls_files_are_plain_paths() {
    let files = TlsFiles {
        ca: "/etc/schedr/tls/ca.crt".into(),
        cert: "/etc/schedr/tls/tls.crt".into(),
        key: "/etc/schedr/tls/tls.key".into(),
    };
    assert_eq!(files.clone(), files);
}

#[tokio::test]
async fn a_master_that_never_answers_the_handshake_times_out() {
    let dir = TempDir::new().unwrap();
    let ca = TestCa::new("schedr CA");
    let worker = tls(&ca, &dir, "worker", &["schedr-worker"]);
    // Accepts connections, never speaks TLS.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((tcp, _)) = listener.accept().await {
            held.push(tcp);
        }
    });
    let url = format!("https://{address}");
    let error = channel(&url, Some(&worker)).await.unwrap_err();
    assert!(
        error.contains("didn't complete the TLS handshake"),
        "{error}"
    );
}

/// An HTTP endpoint answering `ok` on a free port, over `tls`'s UI port configuration.
async fn ui_port(tls: Tls) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let incoming = incoming(
        listener,
        Some(tls),
        Protocol::Ui,
        schedr_tls::incoming::ACCEPT_RETRY,
    )
    .unwrap();
    let address = incoming.local_addr();
    tokio::spawn(async move {
        axum::serve(incoming, Router::new().route("/", get(|| async { "ok" })))
            .await
            .unwrap();
    });
    address
}

#[tokio::test]
async fn a_webservers_ui_port_takes_only_the_uis_certificate() {
    let dir = TempDir::new().unwrap();
    let ca = TestCa::new("schedr CA");
    let webserver = tls(&ca, &dir, "webserver", &["127.0.0.1"]);
    let address = ui_port(webserver).await;
    // The UI's certificate names `schedr-ui`: through, the webserver checked by its address.
    let ui = tls(&ca, &dir, "ui", &["schedr-ui"]);
    let response = get_over_tls(ui.api_client(), address, "127.0.0.1")
        .await
        .unwrap();
    assert!(response.ends_with("ok"), "{response}");
    // A worker's certificate, from the same CA: refused (it may not pass names on).
    let worker = tls(&ca, &dir, "worker", &["schedr-worker"]);
    let refused = get_over_tls(worker.api_client(), address, "127.0.0.1").await;
    let error = refused.unwrap_err().to_string();
    assert!(
        error.contains("NotValidForName") || error.contains("BadCertificate"),
        "{error}"
    );
}

/// Decision 520: a peer's certificate names a worker by `<id>.<worker_name>`, exactly; a
/// wildcard names no one, nor another worker's name, nor a plain connection.
#[test]
fn a_certificate_names_its_own_worker_and_no_other() {
    let ca = TestCa::new("schedr CA");
    let peer = |names: &[&str]| schedr_tls::Peer {
        remote_addr: None,
        certificate: Some(
            rustls::pki_types::CertificateDer::from_pem_slice(ca.issue(names).cert.as_bytes())
                .unwrap()
                .into_owned(),
        ),
    };
    let pod = peer(&["schedr-worker", "w-7f9c.schedr-worker"]);
    assert_eq!(pod.names_worker("w-7f9c", "schedr-worker"), Ok(()));
    // DNS names ignore case and worker IDs don't: an uppercase ID is no certificate's.
    assert!(
        pod.names_worker("W-7F9C", "schedr-worker")
            .unwrap_err()
            .contains("lowercase")
    );
    let other = pod.names_worker("w-other", "schedr-worker").unwrap_err();
    assert!(
        other.contains("doesn't name `w-other.schedr-worker`"),
        "{other}"
    );
    // The shared certificate names the role only.
    assert!(
        peer(&["schedr-worker"])
            .names_worker("w-7f9c", "schedr-worker")
            .is_err()
    );
    // A wildcard would pass the usual name check for any ID.
    let wildcard = peer(&["schedr-worker", "*.schedr-worker"]);
    assert!(wildcard.names_worker("w-7f9c", "schedr-worker").is_err());
    assert!(
        pod.names_worker("w_7f9c", "schedr-worker")
            .unwrap_err()
            .contains("DNS label")
    );
    let plain = schedr_tls::Peer {
        remote_addr: None,
        certificate: None,
    };
    assert!(
        plain
            .names_worker("w-7f9c", "schedr-worker")
            .unwrap_err()
            .contains("no certificate")
    );
}

/// agy's review of M5.2i14: a wildcard of another role's (`*.schedr.local`) doesn't pass for a
/// worker name under it (`worker.schedr.local`), on the master's gRPC port.
#[tokio::test]
async fn a_wildcard_certificate_doesnt_pass_for_the_worker_name() {
    let dir = TempDir::new().unwrap();
    let ca = TestCa::new("schedr CA");
    let master = Tls::load(
        ca.write(dir.path(), "master", &["127.0.0.1"]),
        "worker.schedr.local",
        "schedr-ui",
    )
    .unwrap();
    let address = grpc_server(Some(master)).await;
    let load = |prefix: &str, names: &[&str]| {
        Tls::load(
            ca.write(dir.path(), prefix, names),
            "worker.schedr.local",
            "schedr-ui",
        )
        .unwrap()
    };
    let named = load("named", &["worker.schedr.local"]);
    channel(&format!("https://{address}"), Some(&named))
        .await
        .unwrap();
    let wildcard = load("wildcard", &["*.schedr.local"]);
    let tcp = TcpStream::connect(address).await.unwrap();
    let name = ServerName::try_from("127.0.0.1").unwrap();
    let mut stream = TlsConnector::from(wildcard.grpc_client())
        .connect(name, tcp)
        .await
        .unwrap();
    stream
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .unwrap();
    let mut buffer = [0; 64];
    let error = stream.read(&mut buffer).await.unwrap_err();
    assert!(error.to_string().contains("BadCertificate"), "{error}");
}

/// Fills the queue of the listener at `address`: until a connection isn't answered, not after a
/// guess.
async fn filled(address: std::net::SocketAddr) -> Vec<tokio::net::TcpStream> {
    let mut full = Vec::new();
    while let Ok(stream) = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        tokio::net::TcpStream::connect(address),
    )
    .await
    {
        full.push(stream.unwrap());
        assert!(full.len() < 64, "the listener's queue doesn't fill");
    }
    full
}

/// A listener that drops every new connection's SYN: its queue full, nothing accepted.
async fn unanswering() -> (tokio::net::TcpListener, Vec<tokio::net::TcpStream>) {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let address = listener.local_addr().unwrap();
    (listener, filled(address).await)
}

/// B296: a connection nothing answers ends within its limit, plain or TLS, so the agent tries
/// again; it waited for the kernel's retries, and the worker was counted lost.
#[rstest::rstest]
#[tokio::test]
async fn a_connection_nobody_answers_ends_within_its_limit(#[values(false, true)] with_tls: bool) {
    let dir = TempDir::new().unwrap();
    let ca = TestCa::new("schedr CA");
    let worker = with_tls.then(|| tls(&ca, &dir, "worker", &["schedr-worker"]));
    let (listener, _full) = unanswering().await;
    let scheme = if with_tls { "https" } else { "http" };
    let url = format!("{scheme}://{}", listener.local_addr().unwrap());
    let limit = std::time::Duration::from_millis(300);
    let tried = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        schedr_tls::channel_within(&url, worker.as_ref(), limit),
    )
    .await
    .expect("the connection was given up at its limit");
    let error = tried.unwrap_err();
    assert!(error.contains("300ms"), "{error}");
}

/// Its later connections are limited as its first (agy's review of B296): a plain channel's
/// weren't.
#[tokio::test]
async fn a_plain_channels_next_connection_has_the_limit_too() {
    use tower::{Service as _, ServiceExt as _};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let limit = std::time::Duration::from_millis(300);
    let mut channel = schedr_tls::channel_within(&format!("http://{address}"), None, limit)
        .await
        .unwrap();
    // Its connection closed, the address then answers nothing.
    drop(listener.accept().await.unwrap());
    drop(listener);
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_reuseaddr(true).unwrap();
    socket.bind(address).unwrap();
    let _listener = socket.listen(1).unwrap();
    let _full = filled(address).await;
    let limited = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            // The first error may be the closed connection's.
            let request = tonic::codegen::http::Request::new(tonic::body::Body::empty());
            let error = match channel.ready().await {
                Ok(ready) => match ready.call(request).await {
                    Ok(_) => continue,
                    Err(error) => error,
                },
                Err(error) => error,
            };
            let mut cause: Option<&dyn std::error::Error> = Some(&error);
            while let Some(why) = cause {
                if why.to_string().contains("300ms") {
                    return;
                }
                cause = why.source();
            }
        }
    });
    limited
        .await
        .expect("the next connection was given up at its limit");
}
