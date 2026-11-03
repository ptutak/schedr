//! The Vault storage against a real Vault in dev mode (`SCHEDR_TEST_VAULT_URL`, its root token
//! `SCHEDR_TEST_VAULT_TOKEN`; `make test-integration` starts one): KV v2 with a token, and
//! Kubernetes auth with a fake Kubernetes API that Vault calls for its `TokenReview`. Vault
//! runs on the host's network, so it reaches the fake API on 127.0.0.1.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use axum::Json;
use axum::routing::post;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use schedr_agent::secrets::SecretStorage;
use schedr_agent::vault::{VaultAuth, VaultSettings, VaultStorage};
use serde_json::{Value, json};

fn vault() -> (String, String) {
    let url = std::env::var("SCHEDR_TEST_VAULT_URL")
        .expect("SCHEDR_TEST_VAULT_URL: a Vault in dev mode (`make test-integration` starts one)");
    let token = std::env::var("SCHEDR_TEST_VAULT_TOKEN").unwrap_or_else(|_| "root".to_owned());
    (url, token)
}

/// A name of this test's own (paths, mounts, policies), as tests share the Vault.
fn unique(what: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{what}-{}-{nanos}", std::process::id())
}

/// Calls Vault's API as root.
async fn admin(method: reqwest::Method, path: &str, body: &Value) -> Value {
    let (url, token) = vault();
    let response = reqwest::Client::new()
        .request(method, format!("{url}/v1/{path}"))
        .header("X-Vault-Token", token)
        .json(body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "{path}: {status} {text}");
    serde_json::from_str(&text).unwrap_or(Value::Null)
}

fn settings(auth: VaultAuth) -> VaultSettings {
    VaultSettings {
        address: vault().0,
        mount: "secret".to_owned(),
        namespace: None,
        ca_cert: None,
        auth,
    }
}

#[tokio::test]
async fn a_token_reads_kv_v2_secrets() {
    let path = unique("etl");
    admin(
        reqwest::Method::POST,
        &format!("secret/data/{path}/db"),
        &json!({"data": {"password": "hunter22-pw", "port": 5432}}),
    )
    .await;
    let vault = VaultStorage::new(settings(VaultAuth::Token(vault().1))).unwrap();
    let data = vault.read(&format!("{path}/db")).await.unwrap();
    assert_eq!(data["password"], "hunter22-pw");
    assert_eq!(data["port"], "5432");
    let missing = vault.read(&format!("{path}/nope")).await.unwrap_err();
    assert_eq!(missing.kind(), "secret_not_found");
    // Deleted: its latest version has no data.
    admin(
        reqwest::Method::DELETE,
        &format!("secret/data/{path}/db"),
        &json!({}),
    )
    .await;
    let deleted = vault.read(&format!("{path}/db")).await.unwrap_err();
    assert_eq!(deleted.kind(), "secret_not_found", "{deleted}");

    let refused = VaultStorage::new(settings(VaultAuth::Token("hvs.nope".to_owned()))).unwrap();
    let error = refused.read(&format!("{path}/db")).await.unwrap_err();
    assert_eq!(error.kind(), "secret_denied", "{error}");
}

/// A fake Kubernetes API answering every `TokenReview` for `namespace/name`.
async fn fake_kubernetes(reviews: Arc<AtomicU32>) -> String {
    let app = axum::Router::new().route(
        "/apis/authentication.k8s.io/v1/tokenreviews",
        post(move |Json(_review): Json<Value>| {
            let reviews = reviews.clone();
            async move {
                reviews.fetch_add(1, Ordering::SeqCst);
                (
                    axum::http::StatusCode::CREATED,
                    Json(json!({
                        "apiVersion": "authentication.k8s.io/v1",
                        "kind": "TokenReview",
                        "status": {"authenticated": true, "user": {
                            "username": "system:serviceaccount:schedr:worker",
                            "uid": "u1",
                            "groups": ["system:serviceaccounts"],
                        }},
                    })),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{address}")
}

/// A service-account token's shape (Vault reads its claims; the fake API vouches for it).
fn service_account_jwt() -> String {
    let part = |value: Value| URL_SAFE_NO_PAD.encode(value.to_string());
    format!(
        "{}.{}.c2ln",
        part(json!({"alg": "RS256", "typ": "JWT"})),
        part(json!({
            "iss": "kubernetes/serviceaccount",
            "sub": "system:serviceaccount:schedr:worker",
            "kubernetes.io/serviceaccount/namespace": "schedr",
            "kubernetes.io/serviceaccount/service-account.name": "worker",
            "kubernetes.io/serviceaccount/service-account.uid": "u1",
        }))
    )
}

#[tokio::test]
async fn kubernetes_auth_logs_in_and_reads_what_its_role_allows() {
    let reviews = Arc::new(AtomicU32::new(0));
    let host = fake_kubernetes(reviews.clone()).await;
    let (mount, policy, path) = (unique("k8s"), unique("policy"), unique("team"));
    admin(
        reqwest::Method::POST,
        &format!("sys/auth/{mount}"),
        &json!({"type": "kubernetes"}),
    )
    .await;
    admin(
        reqwest::Method::POST,
        &format!("auth/{mount}/config"),
        &json!({"kubernetes_host": host, "disable_local_ca_jwt": true}),
    )
    .await;
    admin(
        reqwest::Method::PUT,
        &format!("sys/policies/acl/{policy}"),
        &json!({"policy": format!(r#"path "secret/data/{path}/*" {{ capabilities = ["read"] }}"#)}),
    )
    .await;
    admin(
        reqwest::Method::POST,
        &format!("auth/{mount}/role/schedr-worker"),
        &json!({
            "bound_service_account_names": ["worker"],
            "bound_service_account_namespaces": ["schedr"],
            "token_policies": [policy],
            "token_ttl": "60s",
        }),
    )
    .await;
    admin(
        reqwest::Method::POST,
        &format!("secret/data/{path}/db"),
        &json!({"data": {"password": "hunter22-pw"}}),
    )
    .await;
    admin(
        reqwest::Method::POST,
        &format!("secret/data/{path}-other/db"),
        &json!({"data": {"password": "theirs"}}),
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let jwt_file = dir.path().join("token");
    std::fs::write(&jwt_file, service_account_jwt()).unwrap();
    let vault = VaultStorage::new(settings(VaultAuth::Kubernetes {
        role: "schedr-worker".to_owned(),
        mount,
        jwt_file,
    }))
    .unwrap();
    let data = vault.read(&format!("{path}/db")).await.unwrap();
    assert_eq!(data["password"], "hunter22-pw");
    vault.read(&format!("{path}/db")).await.unwrap();
    assert_eq!(
        reviews.load(Ordering::SeqCst),
        1,
        "one login for both reads"
    );
    // Outside the role's policy: denied (after one login again, in case the token was the
    // problem).
    let error = vault.read(&format!("{path}-other/db")).await.unwrap_err();
    assert_eq!(error.kind(), "secret_denied", "{error}");
    assert!(!error.to_string().contains("hvs."), "{error}");
}
