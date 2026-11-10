use pretty_assertions::assert_eq;
use rstest::rstest;
use serde_json::json;

use super::*;

#[rstest]
#[case::password(
    "postgres://schedr:hunter2@db:5432/schedr",
    "postgres://schedr:****@db:5432/schedr"
)]
#[case::no_password("postgres://schedr@db/schedr", "postgres://schedr@db/schedr")]
#[case::no_user("http://webserver:8080/", "http://webserver:8080/")]
#[case::query(
    "postgres://db/s?sslmode=require&password=hunter2&sslpassword=x&Token=y",
    "postgres://db/s?sslmode=require&password=****&sslpassword=****&Token=****"
)]
#[case::key_parameter(
    "https://h/x?key=abc&api_key=d#top",
    "https://h/x?key=****&api_key=****#top"
)]
#[case::an_at_in_the_password("postgres://u:p@ss@db/s", "postgres://u:****@db/s")]
#[case::text_around(
    "connect to postgres://u:p@db/s now",
    "connect to postgres://u:****@db/s now"
)]
#[case::two("http://a:b@x http://c:d@y", "http://a:****@x http://c:****@y")]
#[case::plain("default", "default")]
// Codex's review of M4.4b: each of these kept its secret.
#[case::an_encoded_query_name(
    "postgres://db/s?%70assword=hunter2&sslmode=require",
    "postgres://db/s?%70assword=****&sslmode=require"
)]
#[case::no_scheme("schedr:hunter2@db:5432/s", "schedr:****@db:5432/s")]
#[case::a_network_path("//schedr:hunter2@db/s", "//schedr:****@db/s")]
#[case::a_dsn(
    "host=db user=schedr password=hunter2 dbname=s",
    "host=db user=schedr password=**** dbname=s"
)]
#[case::a_quoted_dsn_value("password='hunter 2' host=db", "password='****' host=db")]
#[case::two_with_a_comma(
    "postgres://a:hunter2@h1,postgres://b:hunter2@h2/s",
    "postgres://a:****@h1,postgres://b:****@h2/s"
)]
// Without schemes, two are one user info to the last `@` (a password may hold `,`).
#[case::two_without_schemes("a:hunter2@h1,b:hunter2@h2", "a:****@h2")]
#[case::a_slash_in_the_password("postgres://u:hun/ter2@db/s", "postgres://u:****@db/s")]
#[case::ipv6("postgres://u:hunter2@[::1]:5432/s", "postgres://u:****@[::1]:5432/s")]
#[case::a_message("token: hunter2, retrying", "token: **** retrying")]
#[case::json_text(r#"{"api_key":"hunter2"}"#, r#"{"api_key":"****"}"#)]
#[case::an_email("mail ops@example.com", "mail ops@example.com")]
// Codex's review of the Deployments batch: characters a password may hold (reqwest and SQLx
// accept them) ended the part, and a value's escaped quote ended the value.
// B202: a raw `"` in a password, sqlx taking the URL.
#[case::a_quote_in_the_password(
    r#"postgres://user:head"secret@db/schedr"#,
    "postgres://user:****@db/schedr"
)]
#[case::a_tab_in_a_query_name(
    "https://h/?pass\tword=hunter2&x=1",
    "https://h/?pass\tword=****&x=1"
)]
#[case::tab_separated_names("key\tpassword=hunter2", "key\tpassword=****")]
#[case::tab_separated_plain("name\tvalue=shown", "name\tvalue=shown")]
#[case::an_at_then_a_quote(
    r#"postgres://user:head@mid"secret@db/schedr"#,
    "postgres://user:****@db/schedr"
)]
#[case::a_quoted_url(
    r#"url = "postgres://u:hunter2@db/s""#,
    r#"url = "postgres://u:****@db/s""#
)]
#[case::a_quoted_url_with_no_password(
    r#"see "http://h:8080/x" and a@b"#,
    r#"see "http://h:8080/x" and a@b"#
)]
#[case::an_apostrophe("http://u:abc'hunter2@127.0.0.1:0", "http://u:****@127.0.0.1:0")]
#[case::parentheses("postgres://u:abc(hunter2)@db/s", "postgres://u:****@db/s")]
#[case::quoted_with_parentheses("('postgres://u:abc(hunter2)@db/s')", "('postgres://u:****@db/s')")]
#[case::an_at_and_a_colon("postgres://u:p@hunter2:z@db/s", "postgres://u:****@db/s")]
#[case::query_parentheses("https://h/?token=abc(hunter2)&x=1", "https://h/?token=****&x=1")]
#[case::an_escaped_quote(
    r"host=db password='abc\'hunter2' dbname=s",
    "host=db password='****' dbname=s"
)]
fn urls_keep_all_but_their_secrets(#[case] text: &str, #[case] shown: &str) {
    let mut value = json!({"url": text});
    mask(&mut value);
    assert_eq!(value, json!({"url": shown}));
}

#[test]
fn a_secret_named_setting_is_masked_whole_and_a_path_to_one_shown() {
    let mut value = json!({
        "password": "hunter2", "api_token": "t", "client_secret": ["a", "b"], "token_file":
        "/run/secrets/token", "tls": {"key": "/etc/tls/key.pem"}, "identity_header": "x-forwarded-user",
        "empty_password": "", "queue": "default"
    });
    mask(&mut value);
    assert_eq!(
        value,
        json!({
            "password": "****", "api_token": "****", "client_secret": "****",
            "token_file": "/run/secrets/token", "tls": {"key": "/etc/tls/key.pem"},
            "identity_header": "x-forwarded-user", "empty_password": "", "queue": "default"
        })
    );
}

#[test]
fn a_secret_held_as_an_object_a_list_or_a_number_is_masked_whole() {
    // Codex's review of M4.4b: an altered worker's `{"PASSWORD": {"value": …}}`, `api_key`
    // and a numeric `token` were stored and shown as they came.
    let mut value = json!({
        "PASSWORD": {"value": "hunter2"}, "api_key": "hunter2", "token": 123_456,
        "apiKey": ["hunter2"], "Client-Secret": true, "keys": {"signingKey": "hunter2"},
        "slots": 4, "unset_token": null
    });
    mask(&mut value);
    assert_eq!(
        value,
        json!({
            "PASSWORD": "****", "api_key": "****", "token": "****", "apiKey": "****",
            "Client-Secret": "****", "keys": {"signingKey": "****"}, "slots": 4,
            "unset_token": null
        })
    );
}

/// Every leaf of `value` (strings, numbers, booleans and nulls alike) replaced with `with`.
fn fill(value: &mut Value, with: &str) {
    match value {
        Value::Array(items) => {
            if items.is_empty() {
                items.push(Value::String(with.to_owned()));
            }
            for item in items {
                fill(item, with);
            }
        }
        Value::Object(fields) => {
            for field in fields.values_mut() {
                fill(field, with);
            }
        }
        leaf => *leaf = Value::String(with.to_owned()),
    }
}

#[test]
fn no_configuration_field_shows_a_secret() {
    // The user's requirement (2026-10-31): every field, including any added later, filled
    // with a URL carrying secrets, shows none of them once masked.
    let mut value = serde_json::to_value(crate::Config::default()).unwrap();
    let secret = "postgres://schedr:hunter2@db:5432/s?password=hunter2&token=hunter2";
    fill(&mut value, secret);
    let filled = value.to_string();
    assert!(filled.matches("hunter2").count() > 30, "{filled}");
    mask(&mut value);
    let shown = value.to_string();
    assert!(!shown.contains("hunter2"), "{shown}");
    assert!(
        shown.contains("postgres://schedr:****@db:5432/s"),
        "{shown}"
    );
}

#[test]
fn a_configuration_masks_its_database_url() {
    let mut config = crate::Config::default();
    config.database.url = Some("postgres://schedr:hunter2@db/schedr".to_owned());
    let shown = masked(&config);
    assert_eq!(shown["database"]["url"], "postgres://schedr:****@db/schedr");
}

#[test]
fn only_tls_keys_holding_a_path_are_shown() {
    // Codex's review of the Deployments batch: any `key` holding a `/` was taken for a path,
    // a Base64 secret too.
    let mut value = json!({
        "connection": {"key": "abc/hunter2AB"}, "key": "/etc/key.pem",
        "tls": {"key": "/etc/tls/key.pem"}, "TLS": {"key": "hunter2"}
    });
    mask(&mut value);
    assert_eq!(
        value,
        json!({
            "connection": {"key": "****"}, "key": "****",
            "tls": {"key": "/etc/tls/key.pem"}, "TLS": {"key": "****"}
        })
    );
}

#[test]
fn masking_takes_time_in_proportion_to_the_text() {
    // Codex's review: every `@` scanned back to the start (16,000 `@`s: 7 s in a debug
    // build); every `=` after a secret's name scanned on to the end.
    let started = std::time::Instant::now();
    for text in [
        "@".repeat(200_000),
        "token=".repeat(50_000),
        "a:".repeat(100_000) + "@x",
    ] {
        let _ = mask_text(&text);
    }
    let took = started.elapsed();
    assert!(took < std::time::Duration::from_secs(2), "{took:?}");
}

#[test]
fn a_tasks_data_is_masked_by_its_texts_and_by_secret_words_in_names() {
    let mut value = serde_json::json!({
        "password": "hunter2",
        "api_key": "abc",
        "apiKey": "abc",
        "API-KEY": "abc",
        "APIToken": "abc",
        "APISecret": "abc",
        "AWSAccessKeyId": "abc",
        "DBPassword": "abc",
        "HTTPPassword": "abc",
        "clientSecret": "abc",
        "access_token": 7,
        "credentials": {"user": "u"},
        "partition_key": "2026-11-01",
        "sort_key": "id",
        "cache_key": "k",
        "keyboard": "qwerty",
        "tokenizer": "bpe",
        "password_hint": "",
        "rows": [{"url": "postgres://u:hunter2@db/s", "note": "password=hunter2"}]
    });
    super::mask_data(&mut value);
    assert_eq!(
        value,
        serde_json::json!({
            "password": "****",
            "api_key": "****",
            "apiKey": "****",
            "API-KEY": "****",
            "APIToken": "****",
            "APISecret": "****",
            "AWSAccessKeyId": "****",
            "DBPassword": "****",
            "HTTPPassword": "****",
            "clientSecret": "****",
            "access_token": "****",
            "credentials": "****",
            "partition_key": "2026-11-01",
            "sort_key": "id",
            "cache_key": "k",
            "keyboard": "qwerty",
            "tokenizer": "bpe",
            "password_hint": "",
            "rows": [{"url": "postgres://u:****@db/s", "note": "password=****"}]
        })
    );
}

#[test]
fn a_text_is_masked_after_what_precedes_it() {
    // A quoted value going on over lines: its rest is hidden in a text read after its start.
    assert_eq!(
        super::mask_text_after("password='hunter2\n", "stillsecret'\nok\n"),
        "****'\nok\n"
    );
    assert_eq!(super::mask_text_after("x = 1\n", "ok\n"), "ok\n");
}

#[test]
fn a_key_is_masked_as_a_text_too() {
    // Codex: a task's output keyed by a connection URL kept the password in the key.
    let mut value = serde_json::json!({
        "postgres://u:hunter2@db/s": {"rows": 3},
        "postgres://u:other@db/s": {"rows": 4},
        "password=hunter3": 1,
        "plain": 2
    });
    super::mask_data(&mut value);
    assert_eq!(
        value,
        serde_json::json!({
            "postgres://u:****@db/s": {"rows": 3},
            "postgres://u:****@db/s (2)": {"rows": 4},
            // Its name has the word `password`: its value is hidden too.
            "password=****": "****",
            "plain": 2
        })
    );
    let mut config = serde_json::json!({"hosts": {"postgres://u:hunter2@db/s": "a"}});
    super::mask(&mut config);
    assert_eq!(
        config,
        serde_json::json!({"hosts": {"postgres://u:****@db/s": "a"}})
    );
}

#[test]
fn a_server_webhook_shows_only_its_origin_and_no_body() {
    let mut config = crate::Config::default();
    config.notifications.channels.insert(
        "ops".to_owned(),
        crate::config::ChannelConfig {
            kind: crate::config::ChannelKind::Webhook,
            url: Some("https://hooks.slack.com/services/T123/B456/SECRETcapability".to_owned()),
            headers_env: std::collections::BTreeMap::new(),
            body: Some(serde_json::json!({"api_key": "k", "text": "{text}"})),
            to: Vec::new(),
        },
    );
    config.notifications.channels.insert(
        "relay".to_owned(),
        crate::config::ChannelConfig {
            url: Some("https://user:pw@relay.example.com:8443/x?y=1".to_owned()),
            body: None,
            ..config.notifications.channels["ops"].clone()
        },
    );
    config.notifications.smtp.password = Some("smtp-SECRET".to_owned());
    let shown = masked(&config);
    assert_eq!(shown["notifications"]["smtp"]["password"], "****");
    let channels = &shown["notifications"]["channels"];
    assert_eq!(channels["ops"]["url"], "https://hooks.slack.com/****");
    assert_eq!(channels["ops"]["body"], "****");
    assert_eq!(
        channels["relay"]["url"],
        "https://relay.example.com:8443/****"
    );
    assert!(!shown.to_string().contains("SECRET"), "{shown}");
}

/// B202's review: many quotes before an `@` are masked in linear time, not quadratic.
#[test]
fn many_quotes_are_masked_in_linear_time() {
    let text = |n: usize| format!("u:{}@h", "\"".repeat(n));
    let started = std::time::Instant::now();
    let _ = mask_text(&text(4000));
    let small = started.elapsed();
    let started = std::time::Instant::now();
    let _ = mask_text(&text(64_000));
    let large = started.elapsed();
    // Sixteen times the text: linear is about 16 times the time, quadratic 256.
    assert!(
        large < small * 64 + std::time::Duration::from_millis(50),
        "{small:?} → {large:?}"
    );
}
