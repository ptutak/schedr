//! Against a fake Vault: the logic of tokens, renewal and answers. The real Vault is in
//! `tests/vault.rs`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex as StdMutex;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode as Status};
use axum::routing::{get, post};
use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

/// What the fake Vault knows and saw.
#[derive(Debug, Default)]
struct Fake {
    /// Tokens it accepts.
    valid: BTreeSet<String>,
    logins: u32,
    renewals: u32,
    lease: u64,
    namespaces: Vec<String>,
    secrets: BTreeMap<String, Value>,
    /// How long a login and a renewal take.
    login_delay: Duration,
    renew_delay: Duration,
    /// Logins answer 503 (Vault sealed).
    sealed: bool,
}

type Shared = Arc<StdMutex<Fake>>;

fn token(headers: &HeaderMap) -> String {
    headers
        .get("X-Vault-Token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

fn denied() -> (Status, Json<Value>) {
    (
        Status::FORBIDDEN,
        Json(json!({"errors": ["permission denied"]})),
    )
}

async fn fake_vault(fake: Shared) -> String {
    let app = axum::Router::new()
        .route(
            "/v1/auth/token/lookup-self",
            get(|State(f): State<Shared>, h: HeaderMap| async move {
                let f = f.lock().unwrap();
                if !f.valid.contains(&token(&h)) {
                    return denied();
                }
                (
                    Status::OK,
                    Json(json!({"data": {"ttl": f.lease, "renewable": true}})),
                )
            }),
        )
        .merge(login())
        .route(
            "/v1/auth/token/renew-self",
            post(|State(f): State<Shared>, h: HeaderMap| async move {
                let delay = f.lock().unwrap().renew_delay;
                tokio::time::sleep(delay).await;
                let mut f = f.lock().unwrap();
                if !f.valid.contains(&token(&h)) {
                    return denied();
                }
                f.renewals += 1;
                let lease = f.lease;
                (
                    Status::OK,
                    Json(
                        json!({"auth": {"client_token": token(&h), "lease_duration": lease,
                        "renewable": true}}),
                    ),
                )
            }),
        )
        .route(
            "/v1/secret/data/{*path}",
            get(
                |State(f): State<Shared>, h: HeaderMap, Path(path): Path<String>| async move {
                    let mut f = f.lock().unwrap();
                    if let Some(ns) = h.get("X-Vault-Namespace") {
                        f.namespaces.push(ns.to_str().unwrap().to_owned());
                    }
                    if !f.valid.contains(&token(&h)) {
                        return denied();
                    }
                    match f.secrets.get(&path) {
                        Some(Value::Null) => (
                            Status::NOT_FOUND,
                            Json(json!({"data": {"data": null, "metadata": {}}})),
                        ),
                        Some(data) => (Status::OK, Json(json!({"data": {"data": data}}))),
                        None if path == "v1" => (Status::OK, Json(json!({"data": {"k": "v"}}))),
                        None if path == "down" => (
                            Status::SERVICE_UNAVAILABLE,
                            Json(json!({"errors": ["Vault is sealed"]})),
                        ),
                        None => (Status::NOT_FOUND, Json(json!({"errors": []}))),
                    }
                },
            ),
        )
        .with_state(fake)
        .merge(odd_answers());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{address}")
}

/// Vault's Kubernetes login, refusing every role and JWT but the test's.
fn login() -> axum::Router<Shared> {
    axum::Router::new().route(
        "/v1/auth/kubernetes/login",
        post(
            |State(f): State<Shared>, Json(body): Json<Value>| async move {
                let delay = f.lock().unwrap().login_delay;
                tokio::time::sleep(delay).await;
                let mut f = f.lock().unwrap();
                if f.sealed {
                    f.logins += 1;
                    return (
                        Status::SERVICE_UNAVAILABLE,
                        Json(json!({"errors": ["Vault is sealed"]})),
                    );
                }
                if body["role"] != "schedr-worker" || body["jwt"] != "the-jwt" {
                    return (
                        Status::BAD_REQUEST,
                        Json(json!({"errors": ["invalid role"]})),
                    );
                }
                f.logins += 1;
                let token = format!("k8s-{}", f.logins);
                f.valid.insert(token.clone());
                let lease = f.lease;
                (
                    Status::OK,
                    Json(
                        json!({"auth": {"client_token": token, "lease_duration": lease,
                        "renewable": true}}),
                    ),
                )
            },
        ),
    )
}

/// Answers that aren't a secret.
fn odd_answers() -> axum::Router {
    axum::Router::new()
        // A proxy's page, not JSON.
        .route(
            "/v1/secret/data/html",
            get(|| async { (Status::OK, "<html>login</html>") }),
        )
        // A proxy's 404 page: no word from Vault on the secret (Codex, M4.6e).
        .route(
            "/v1/secret/data/html404",
            get(|| async { (Status::NOT_FOUND, "<html>not here</html>") }),
        )
        // Elsewhere: followed, the read would succeed (and carry the token there).
        .route(
            "/v1/secret/data/moved",
            get(|| async {
                (
                    Status::TEMPORARY_REDIRECT,
                    [("location", "/v1/secret/data/etl/db")],
                )
            }),
        )
}

fn settings(address: String, auth: VaultAuth) -> VaultSettings {
    VaultSettings {
        address,
        mount: "secret".to_owned(),
        namespace: None,
        ca_cert: None,
        auth,
        request_timeout: std::time::Duration::from_secs(30),
        connect_timeout: std::time::Duration::from_secs(10),
        renewal: VaultRenewal::default(),
    }
}

fn fake(lease: u64) -> Shared {
    Arc::new(StdMutex::new(Fake {
        lease,
        secrets: BTreeMap::from([
            (
                "etl/db".to_owned(),
                json!({"password": "pw", "port": 5432, "tls": true, "hosts": ["a", "b"]}),
            ),
            ("gone".to_owned(), Value::Null),
            ("a b/c%d".to_owned(), json!({"k": "v"})),
        ]),
        ..Fake::default()
    }))
}

fn kubernetes(dir: &tempfile::TempDir) -> VaultAuth {
    let jwt_file = dir.path().join("token");
    std::fs::write(&jwt_file, "the-jwt\n").unwrap();
    VaultAuth::Kubernetes {
        role: "schedr-worker".to_owned(),
        mount: "kubernetes".to_owned(),
        jwt_file,
    }
}

#[tokio::test]
async fn a_token_reads_kv_v2_values_as_text() {
    let fake = fake(0);
    fake.lock().unwrap().valid.insert("root".to_owned());
    let address = fake_vault(fake.clone()).await;
    let mut settings = settings(address, VaultAuth::Token("root".to_owned()));
    settings.namespace = Some("team-x".to_owned());
    let vault = VaultStorage::new(settings).unwrap();
    let data = vault.read("etl/db").await.unwrap();
    assert_eq!(
        data,
        BTreeMap::from([
            ("hosts".to_owned(), r#"["a","b"]"#.to_owned()),
            ("password".to_owned(), "pw".to_owned()),
            ("port".to_owned(), "5432".to_owned()),
            ("tls".to_owned(), "true".to_owned()),
        ])
    );
    // A path's parts are encoded, not split or read as a query.
    assert_eq!(vault.read("a b/c%d").await.unwrap()["k"], "v");
    assert_eq!(fake.lock().unwrap().namespaces, ["team-x", "team-x"]);
}

#[tokio::test]
async fn each_answer_has_its_error_kind() {
    let fake = fake(0);
    fake.lock().unwrap().valid.insert("root".to_owned());
    let address = fake_vault(fake.clone()).await;
    let vault = VaultStorage::new(settings(address, VaultAuth::Token("root".to_owned()))).unwrap();
    let kind = |r: Result<SecretData, SecretError>| r.unwrap_err().kind();
    assert_eq!(kind(vault.read("nope").await), "secret_not_found");
    assert_eq!(kind(vault.read("gone").await), "secret_not_found");
    assert_eq!(kind(vault.read("down").await), "secret_unavailable");
    assert_eq!(kind(vault.read("etl/../sys").await), "secret_not_found");
    // An answer that isn't a secret is no answer, not "no such secret".
    assert_eq!(kind(vault.read("html").await), "secret_unavailable");
    assert_eq!(kind(vault.read("html404").await), "secret_unavailable");
    assert_eq!(kind(vault.read("v1").await), "secret_unavailable");
    // A redirect isn't followed: it would carry the token elsewhere.
    assert_eq!(kind(vault.read("moved").await), "secret_unavailable");

    let refused = VaultStorage::new(settings(
        fake_vault(fake.clone()).await,
        VaultAuth::Token("x".into()),
    ))
    .unwrap();
    let error = refused.read("etl/db").await.unwrap_err();
    assert_eq!(error.kind(), "secret_denied");
    assert!(error.to_string().contains("permission denied"), "{error}");
    assert!(!format!("{refused:?}").contains("\"x\""), "{refused:?}");

    // Nothing listening: unavailable, retried.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let down = VaultStorage::new(settings(address, VaultAuth::Token("root".into()))).unwrap();
    assert_eq!(kind(down.read("etl/db").await), "secret_unavailable");
}

#[tokio::test]
async fn kubernetes_auth_logs_in_once_and_again_when_its_token_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let fake = fake(3600);
    let vault =
        VaultStorage::new(settings(fake_vault(fake.clone()).await, kubernetes(&dir))).unwrap();
    vault.read("etl/db").await.unwrap();
    vault.read("etl/db").await.unwrap();
    assert_eq!(fake.lock().unwrap().logins, 1);
    // Revoked: the next read logs in again and succeeds.
    fake.lock().unwrap().valid.clear();
    assert_eq!(vault.read("etl/db").await.unwrap()["password"], "pw");
    assert_eq!(fake.lock().unwrap().logins, 2);
    assert!(!format!("{vault:?}").contains("k8s-2"));

    // A role Vault refuses: denied, not retried.
    let mut auth = kubernetes(&dir);
    if let VaultAuth::Kubernetes { role, .. } = &mut auth {
        "other".clone_into(role);
    }
    let refused = VaultStorage::new(settings(fake_vault(fake).await, auth)).unwrap();
    let error = refused.read("etl/db").await.unwrap_err();
    assert_eq!(error.kind(), "secret_denied");
    assert!(error.to_string().contains("invalid role"), "{error}");
}

#[tokio::test]
async fn the_token_is_renewed_before_it_expires() {
    let dir = tempfile::tempdir().unwrap();
    // A 3 s lease: renewed after 2 s, so a read at 4 s still uses the first login.
    let fake = fake(3);
    let vault =
        VaultStorage::new(settings(fake_vault(fake.clone()).await, kubernetes(&dir))).unwrap();
    vault.read("etl/db").await.unwrap();
    tokio::time::sleep(Duration::from_millis(4200)).await;
    vault.read("etl/db").await.unwrap();
    let fake = fake.lock().unwrap();
    assert!(fake.renewals >= 1, "{fake:?}");
    assert_eq!(fake.logins, 1);
}

#[tokio::test]
async fn a_slow_renewal_holds_up_no_read() {
    let dir = tempfile::tempdir().unwrap();
    // A 3 s lease, renewed after 2 s; the renewal takes 3 s.
    let fake = fake(3);
    fake.lock().unwrap().renew_delay = Duration::from_secs(3);
    let vault =
        VaultStorage::new(settings(fake_vault(fake.clone()).await, kubernetes(&dir))).unwrap();
    vault.read("etl/db").await.unwrap();
    tokio::time::sleep(Duration::from_millis(2300)).await;
    let started = Instant::now();
    vault.read("etl/db").await.unwrap();
    // While the renewal waits, the token still works (fake: some 2.3 s into its 3 s).
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(
        fake.lock().unwrap().renewals,
        0,
        "the renewal is still in flight"
    );
}

#[tokio::test]
async fn reads_waiting_on_a_failed_login_share_its_failure() {
    let dir = tempfile::tempdir().unwrap();
    let fake = fake(3600);
    {
        let mut f = fake.lock().unwrap();
        f.sealed = true;
        f.login_delay = Duration::from_millis(500);
    }
    let vault =
        VaultStorage::new(settings(fake_vault(fake.clone()).await, kubernetes(&dir))).unwrap();
    let started = Instant::now();
    let reads: Vec<_> = (0..5)
        .map(|_| {
            let vault = vault.clone();
            tokio::spawn(async move { vault.read("etl/db").await })
        })
        .collect();
    for read in reads {
        let read = read.await.unwrap();
        assert_eq!(read.unwrap_err().kind(), "secret_unavailable");
    }
    // One login for all five, not five in a row.
    assert_eq!(fake.lock().unwrap().logins, 1);
    assert!(
        started.elapsed() < Duration::from_millis(1500),
        "{:?}",
        started.elapsed()
    );
    // A later read tries again.
    fake.lock().unwrap().sealed = false;
    fake.lock().unwrap().login_delay = Duration::ZERO;
    assert_eq!(vault.read("etl/db").await.unwrap()["password"], "pw");
}

#[tokio::test]
async fn a_wrong_auth_mount_is_denied_with_vaults_words() {
    let dir = tempfile::tempdir().unwrap();
    let mut auth = kubernetes(&dir);
    if let VaultAuth::Kubernetes { mount, .. } = &mut auth {
        "nope".clone_into(mount);
    }
    let vault = VaultStorage::new(settings(fake_vault(fake(3600)).await, auth)).unwrap();
    let error = vault.read("etl/db").await.unwrap_err();
    assert_eq!(error.kind(), "secret_denied", "{error}");
    assert!(
        error.to_string().contains("the Kubernetes login (404"),
        "{error}"
    );
}

#[test]
fn a_path_is_encoded_part_by_part() {
    assert_eq!(encode_path("etl/db"), "etl/db");
    assert_eq!(encode_path("a b/c%d?e#"), "a%20b/c%25d%3Fe%23");
    assert_eq!(encode_path("zażółć"), "za%C5%BC%C3%B3%C5%82%C4%87");
}
