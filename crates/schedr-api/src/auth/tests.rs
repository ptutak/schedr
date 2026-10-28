use axum::body::Body;
use axum::http::{HeaderValue, Request};
use http_body_util::BodyExt;
use pretty_assertions::assert_eq;
use rstest::rstest;
use schedr_db::Database;
use tower::ServiceExt;

use super::*;
use crate::{ApiState, router};

/// A users file of `anna:secret` and `ben:other`, hashed at bcrypt's lowest cost.
fn users_text() -> String {
    let hash = |password| bcrypt::hash(password, 4).unwrap();
    format!(
        "# who may sign in\n\nanna:{}\nben:{}\n",
        hash("secret"),
        hash("other")
    )
}

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in pairs {
        map.insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    map
}

fn basic(user: &str, password: &str) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    format!("Basic {encoded}")
}

fn auth(mode: AuthMode, identity_header: Option<&str>, token: Option<&str>) -> Auth {
    let dir = tempfile::tempdir().unwrap();
    let users_file = dir.path().join("users");
    std::fs::write(&users_file, users_text()).unwrap();
    let section = AuthSection {
        mode,
        identity_header: identity_header.map(str::to_owned),
        users_file: Some(users_file),
        ..AuthSection::default()
    };
    Auth::from_config(
        &section,
        token.map(|t| Arc::new(Followed::fixed(t.to_owned()))),
    )
    .unwrap()
}

#[test]
fn users_are_read_and_checked() {
    let users = Users::parse(&users_text()).unwrap();
    assert_eq!(users.len(), 2);
    assert!(users.verify("anna", "secret"));
    assert!(!users.verify("anna", "other"));
    assert!(!users.verify("nobody", "secret"));
    for (text, why) in [
        ("anna", "isn't `name:bcrypt-hash`"),
        (":$2b$04$x", "the name isn't usable"),
        ("anna:plain", "hash isn't bcrypt's"),
        ("anna:$2b$04$x\nanna:$2b$04$y", "is there twice"),
    ] {
        let error = Users::parse(text).unwrap_err();
        assert!(error.contains(why), "{text:?}: {error}");
    }
}

#[test]
fn a_check_that_passed_is_remembered_and_a_failed_one_is_not() {
    let users = Users::parse(&users_text()).unwrap();
    let timed = |name: &str, password: &str| {
        let started = std::time::Instant::now();
        let valid = users.verify(name, password);
        (valid, started.elapsed())
    };
    let (valid, first) = timed("anna", "secret");
    assert!(valid);
    let (valid, again) = timed("anna", "secret");
    assert!(valid);
    // bcrypt's cost 4 is about a millisecond; remembered, a lookup.
    assert!(again * 10 < first, "{again:?} vs {first:?}");
    // The same name with another password isn't taken from what was remembered.
    assert!(!users.verify("anna", "secreT"));
    assert!(!users.verify("anna", "secreT"));
}

#[rstest]
#[case::none_anonymous(AuthMode::None, None, &[], Ok("anonymous"))]
#[case::none_named(AuthMode::None, Some("x-forwarded-user"), &[("x-forwarded-user", "anna")], Ok("anna"))]
#[case::proxy(AuthMode::ProxyHeader, None, &[("x-forwarded-user", "anna")], Ok("anna"))]
#[case::proxy_own_header(AuthMode::ProxyHeader, Some("x-auth-user"), &[("x-auth-user", "ben")], Ok("ben"))]
#[case::proxy_missing(AuthMode::ProxyHeader, None, &[], Err(Refused::NoProxyHeader("x-forwarded-user".to_owned())))]
#[case::proxy_empty(AuthMode::ProxyHeader, None, &[("x-forwarded-user", " ")], Err(Refused::NoProxyHeader("x-forwarded-user".to_owned())))]
#[case::basic(AuthMode::Basic, None, &[("authorization", "Basic YW5uYTpzZWNyZXQ=")], Ok("anna"))]
#[case::basic_wrong(AuthMode::Basic, None, &[("authorization", "Basic YW5uYTpub3Bl")], Err(Refused::Credentials))]
#[case::basic_unknown(AuthMode::Basic, None, &[("authorization", "Basic ZXZlOnNlY3JldA==")], Err(Refused::Credentials))]
#[case::basic_none(AuthMode::Basic, None, &[], Err(Refused::Credentials))]
#[case::basic_bearer(AuthMode::Basic, None, &[("authorization", "Bearer abc")], Err(Refused::Credentials))]
// Anyone else's claim to pass a name on is ignored.
#[case::spoofed_user(AuthMode::None, None, &[("x-schedr-user", "admin")], Ok("anonymous"))]
#[case::spoofed_user_basic(AuthMode::Basic, None, &[("x-schedr-user", "admin")], Err(Refused::Credentials))]
#[case::wrong_token(AuthMode::Basic, None, &[("x-schedr-ui-token", "guess"), ("x-schedr-user", "admin")], Err(Refused::Credentials))]
// The UI's token: its name taken, whatever the mode.
#[case::token(AuthMode::Basic, None, &[("x-schedr-ui-token", "t0ken"), ("x-schedr-user", "anna")], Ok("anna"))]
#[case::token_nobody(AuthMode::ProxyHeader, None, &[("x-schedr-ui-token", "t0ken")], Ok("anonymous"))]
fn the_user_is_who_the_mode_says(
    #[case] mode: AuthMode,
    #[case] identity_header: Option<&str>,
    #[case] pairs: &[(&str, &str)],
    #[case] expected: Result<&str, Refused>,
) {
    let got = auth(mode, identity_header, Some("t0ken")).user(&headers(pairs), false);
    assert_eq!(got, expected.map(str::to_owned));
}

#[test]
fn a_proven_ui_passes_the_name_on_and_an_empty_token_proves_nothing() {
    let auth = auth(AuthMode::Basic, None, Some(""));
    let named = headers(&[("x-schedr-user", "anna")]);
    assert_eq!(auth.user(&named, true), Ok("anna".to_owned()));
    let with_empty = headers(&[("x-schedr-ui-token", ""), ("x-schedr-user", "anna")]);
    assert_eq!(auth.user(&with_empty, false), Err(Refused::Credentials));
    // A control character in a name (a forged log line) isn't taken.
    let forged = headers(&[("x-schedr-user", "anna\tadmin")]);
    assert_eq!(auth.user(&forged, true), Ok("anonymous".to_owned()));
}

#[test]
fn basic_needs_its_users() {
    let section = AuthSection {
        mode: AuthMode::Basic,
        ..AuthSection::default()
    };
    let error = Auth::from_config(&section, None).unwrap_err();
    assert!(error.contains("users_file"), "{error}");
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("users");
    std::fs::write(&empty, "# nobody yet\n").unwrap();
    let section = AuthSection {
        mode: AuthMode::Basic,
        users_file: Some(empty),
        ..AuthSection::default()
    };
    assert!(
        Auth::from_config(&section, None)
            .unwrap_err()
            .contains("no users")
    );
}

async fn call(
    app: &axum::Router,
    path: &str,
    pairs: &[(&str, &str)],
) -> (StatusCode, HeaderMap, String) {
    let mut request = Request::builder().uri(path);
    for (name, value) in pairs {
        request = request.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    let bytes = body.collect().await.unwrap().to_bytes();
    (
        parts.status,
        parts.headers,
        String::from_utf8(bytes.to_vec()).unwrap(),
    )
}

#[tokio::test]
async fn the_api_asks_for_credentials_but_its_description_stays_open() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let app = router(ApiState::new(db, None).with_auth(auth(AuthMode::Basic, None, None)));
    let (status, headers, body) = call(&app, "/api/v1/dags", &[]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        headers[header::WWW_AUTHENTICATE],
        "Basic realm=\"schedr\", charset=\"UTF-8\""
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["error"],
        "not signed in: user name and password needed"
    );
    let (status, _, _) = call(
        &app,
        "/api/v1/dags",
        &[("authorization", &basic("anna", "secret"))],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    for open in ["/api/v1/openapi.json", "/api/v1/docs/"] {
        let (status, _, _) = call(&app, open, &[]).await;
        assert_ne!(status, StatusCode::UNAUTHORIZED, "{open}");
    }
}

#[tokio::test]
async fn a_command_records_the_user_the_request_was_settled_as() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let app =
        router(ApiState::new(db.clone(), None).with_auth(auth(AuthMode::ProxyHeader, None, None)));
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/workers/w1/drain")
        .header("x-forwarded-user", "anna")
        .header("x-schedr-user", "admin")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let command: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(command["requested_by"], "anna");
    // Proven in-process (as standalone's own UI calls), its name is taken.
    let mut request = Request::builder()
        .method("POST")
        .uri("/api/v1/workers/w1/resume")
        .header("x-schedr-user", "ben")
        .body(Body::empty())
        .unwrap();
    request.extensions_mut().insert(ProvenUi);
    let response = app.oneshot(request).await.unwrap();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let command: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(command["requested_by"], "ben");
}

/// Makes `followed` look at its file again on the next read (as [`RECHECK`] passing would).
fn due<T>(followed: &Followed<T>) {
    let file = followed.file.as_ref().unwrap();
    file.looked.lock().unwrap().0 = Instant::now().checked_sub(RECHECK).unwrap();
}

#[test]
fn the_users_follow_their_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("users");
    std::fs::write(&path, users_text()).unwrap();
    let users = Users::read(&path).unwrap();
    assert!(users.verify("anna", "secret"));
    // Anna's password changed and Ben removed: the old password (remembered) and Ben refused.
    let hash = bcrypt::hash("changed", 4).unwrap();
    std::fs::write(&path, format!("# changed\nanna:{hash}\n")).unwrap();
    assert!(
        users.verify("anna", "secret"),
        "looked at only every RECHECK"
    );
    due(&users.loaded);
    assert!(!users.verify("anna", "secret"));
    assert!(users.verify("anna", "changed"));
    assert!(!users.verify("ben", "other"));
    assert_eq!(users.len(), 1);
    // A file that doesn't read (half-written, gone) leaves them as they were.
    std::fs::write(&path, "anna\n").unwrap();
    due(&users.loaded);
    assert!(users.verify("anna", "changed"));
    std::fs::remove_file(&path).unwrap();
    due(&users.loaded);
    assert!(users.verify("anna", "changed"));
}

#[test]
fn an_unknown_name_costs_what_the_files_hashes_cost() {
    // Verified for an unknown name, the dummy is at the file's highest cost, so a known name
    // isn't told apart by its time.
    let cheap = Users::parse(&users_text()).unwrap();
    assert_eq!(cost(&cheap.loaded.current().0.dummy), Some(4));
    let dearer = format!("{}carl:{}\n", users_text(), bcrypt::hash("x", 5).unwrap());
    let dearer = Users::parse(&dearer).unwrap();
    assert_eq!(cost(&dearer.loaded.current().0.dummy), Some(5));
    assert!(
        Users::parse("anna:$2b$99$x").is_err(),
        "a cost bcrypt doesn't take"
    );
}

#[test]
fn a_header_sent_twice_names_no_one() {
    // A client's header, then the proxy's appended after it: neither is taken.
    let twice = |name: &'static str, first: &str, second: &str| {
        let mut map = HeaderMap::new();
        map.append(name, HeaderValue::from_str(first).unwrap());
        map.append(name, HeaderValue::from_str(second).unwrap());
        map
    };
    let proxy = auth(AuthMode::ProxyHeader, None, Some("t0ken"));
    assert_eq!(
        proxy.user(&twice("x-forwarded-user", "forged", "anna"), false),
        Err(Refused::NoProxyHeader("x-forwarded-user".to_owned()))
    );
    let none = auth(AuthMode::None, Some("x-forwarded-user"), None);
    assert_eq!(
        none.user(&twice("x-forwarded-user", "forged", "anna"), false),
        Ok("anonymous".to_owned())
    );
    let basic_twice = twice(
        "authorization",
        &basic("anna", "secret"),
        &basic("ben", "other"),
    );
    assert_eq!(
        auth(AuthMode::Basic, None, None).user(&basic_twice, false),
        Err(Refused::Credentials)
    );
    let mut named_twice = twice("x-schedr-user", "admin", "anna");
    named_twice.insert("x-schedr-ui-token", HeaderValue::from_static("t0ken"));
    assert_eq!(proxy.user(&named_twice, false), Ok("anonymous".to_owned()));
}

#[test]
fn the_token_is_equal_only_to_itself() {
    let token = b"0123456789abcdef";
    assert!(constant_time_eq(token, token));
    for given in [
        &b""[..],
        b"0123456789abcde",
        b"0123456789abcdef0",
        b"0123456789abcdeF",
        b"\x00123456789abcdef",
    ] {
        assert!(!constant_time_eq(given, token), "{given:?}");
    }
}

#[test]
fn the_rules_without_the_uis_token_take_none() {
    let auth = auth(AuthMode::Basic, None, Some("t0ken"));
    let with_token = headers(&[("x-schedr-ui-token", "t0ken")]);
    assert_eq!(auth.user(&with_token, false), Ok("anonymous".to_owned()));
    assert_eq!(
        auth.without_ui_token().user(&with_token, false),
        Err(Refused::Credentials)
    );
}

#[rstest]
#[case::another_site(&[("sec-fetch-site", "cross-site")], StatusCode::FORBIDDEN)]
#[case::same_site_other_origin(&[("sec-fetch-site", "same-site")], StatusCode::FORBIDDEN)]
#[case::origin_elsewhere(&[("origin", "http://evil.example"), ("host", "schedr:8080")], StatusCode::FORBIDDEN)]
#[case::same_origin(&[("sec-fetch-site", "same-origin")], StatusCode::ACCEPTED)]
#[case::origin_is_the_host(&[("origin", "http://schedr:8080"), ("host", "schedr:8080")], StatusCode::ACCEPTED)]
// No browser says anything: the CLI, `schedr ui`.
#[case::not_a_browser(&[], StatusCode::ACCEPTED)]
#[tokio::test]
async fn the_api_takes_no_write_from_another_sites_page(
    #[case] pairs: &[(&str, &str)],
    #[case] expected: StatusCode,
) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let app = router(ApiState::new(db, None).with_auth(auth(AuthMode::Basic, None, None)));
    // The browser attaches the credentials it remembers to any site's form.
    let mut request = Request::builder()
        .method("POST")
        .uri("/api/v1/workers/w1/drain")
        .header("authorization", basic("anna", "secret"));
    for (name, value) in pairs {
        request = request.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), expected);
    // Reading stays open to a page elsewhere's links (it can't read the answer).
    let (status, _, _) = call(
        &app,
        "/api/v1/workers",
        &[
            ("authorization", &basic("anna", "secret")),
            ("sec-fetch-site", "cross-site"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[test]
fn a_users_file_is_read_again_once_it_can_be_read() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("users");
    std::fs::write(&path, users_text()).unwrap();
    let users = Users::read(&path).unwrap();
    // Anna's password changed, but the one read after it fails.
    let hash = bcrypt::hash("changed", 4).unwrap();
    std::fs::write(&path, format!("anna:{hash}\n")).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read(&path).is_ok() {
        return; // root reads it anyway: nothing to fail
    }
    due(&users.loaded);
    assert!(users.verify("anna", "secret"), "kept as it was");
    // Readable again, unchanged otherwise: read at the next look.
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    due(&users.loaded);
    assert!(users.verify("anna", "changed"));
    assert!(!users.verify("anna", "secret"));
}

#[test]
fn the_uis_token_follows_its_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    std::fs::write(&path, "0123456789abcdef\n").unwrap();
    let token = read_ui_token(&path).unwrap();
    let auth = Auth::from_config(&AuthSection::default(), Some(Arc::clone(&token))).unwrap();
    let named = |token: &str| headers(&[("x-schedr-ui-token", token), ("x-schedr-user", "anna")]);
    assert_eq!(
        auth.user(&named("0123456789abcdef"), false),
        Ok("anna".to_owned())
    );
    // A new token, as the Secret's volume brings it: the old one stops proving anything.
    std::fs::write(&path, "fedcba9876543210-new\n").unwrap();
    due(&token);
    assert_eq!(
        auth.user(&named("fedcba9876543210-new"), false),
        Ok("anna".to_owned())
    );
    assert_eq!(
        auth.user(&named("0123456789abcdef"), false),
        Ok("anonymous".to_owned())
    );
    // One too short is refused, the token kept.
    std::fs::write(&path, "short\n").unwrap();
    due(&token);
    assert_eq!(token.current().0.as_str(), "fedcba9876543210-new");
    assert!(
        read_ui_token(&path)
            .unwrap_err()
            .contains("shorter than 16")
    );
}
