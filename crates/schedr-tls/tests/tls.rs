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
    Tls::load(ca.write(dir.path(), prefix, names), "schedr-worker").unwrap()
}

/// A gRPC server with no services on a free port, over `tls`: its address.
async fn grpc_server(tls: Option<Tls>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let incoming = incoming(listener, tls, Protocol::Grpc).unwrap();
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
    let incoming = incoming(listener, tls, Protocol::Http).unwrap();
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
    let worker = Tls::load(foreign, "schedr-worker").unwrap();
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
    let master = Tls::load(files.clone(), "schedr-worker").unwrap();
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
