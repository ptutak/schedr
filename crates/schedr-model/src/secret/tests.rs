use pretty_assertions::assert_eq;
use rstest::rstest;
use serde_json::json;

use super::*;

fn name(storage: Option<&str>, path: &str, key: Option<&str>) -> SecretName {
    SecretName {
        storage: storage.map(str::to_owned),
        path: path.to_owned(),
        key: key.map(str::to_owned),
    }
}

#[rstest]
#[case(name(None, "etl/db", Some("password")), "secrets['etl/db#password']")]
#[case(name(Some("other"), "etl/db", None), "secrets.other['etl/db']")]
#[case(
    name(Some("vault-2"), "a", Some("k")),
    "secrets.storage('vault-2')['a#k']"
)]
#[case(name(Some("2nd"), "a", Some("k")), "secrets.storage('2nd')['a#k']")]
#[case(
    name(Some("storage"), "a", Some("k")),
    "secrets.storage('storage')['a#k']"
)]
#[case(name(Some("_a"), "a", Some("k")), "secrets.storage('_a')['a#k']")]
#[case(name(Some("class"), "a", Some("k")), "secrets.storage('class')['a#k']")]
#[case(name(None, "it's", Some("k")), "secrets[\"it's#k\"]")]
fn a_name_is_shown_as_written(#[case] secret: SecretName, #[case] shown: &str) {
    assert_eq!(secret.to_string(), shown);
}

#[rstest]
#[case(name(None, "etl/db", Some("password")), json!({"path": "etl/db", "key": "password"}))]
#[case(name(Some("v2"), "etl", None), json!({"storage": "v2", "path": "etl"}))]
fn a_name_round_trips(#[case] secret: SecretName, #[case] stored: serde_json::Value) {
    assert_eq!(serde_json::to_value(&secret).unwrap(), stored);
    assert_eq!(
        serde_json::from_value::<SecretName>(stored).unwrap(),
        secret
    );
}

#[rstest]
#[case(name(None, "", Some("k")), "the path is empty")]
#[case(name(None, "/etl", Some("k")), "no empty part")]
#[case(name(None, "etl//db", Some("k")), "no empty part")]
#[case(name(None, "etl/", None), "a prefix is `SecretPrefix(...)`")]
#[case(name(None, "etl#x", Some("k")), "no `#`")]
#[case(name(None, "etl", Some("")), "the key after `#` is empty")]
#[case(name(None, "etl", Some("a#b")), "a key contains neither")]
#[case(name(Some("a b"), "etl", None), "only letters, digits")]
#[case(name(Some(""), "etl", None), "the storage's name is empty")]
#[case(name(Some("default"), "etl", None), "`default` names")]
fn a_bad_name_is_refused(#[case] secret: SecretName, #[case] reason: &str) {
    let error = secret.check().unwrap_err();
    assert!(error.contains(reason), "{error}");
    assert!(error.starts_with("`secrets"), "{error}");
}

#[test]
fn a_good_name_passes() {
    assert_eq!(
        name(Some("vault-2"), "a/b_c.d/e", Some("x.y")).check(),
        Ok(())
    );
}

#[test]
fn a_declaration_round_trips() {
    let declared = vec![
        SecretDecl::Secret(name(None, "etl/api", Some("token"))),
        SecretDecl::Prefix {
            storage: Some("other".into()),
            prefix: "tenants/".into(),
        },
    ];
    let stored = json!([
        {"kind": "secret", "path": "etl/api", "key": "token"},
        {"kind": "prefix", "storage": "other", "prefix": "tenants/"},
    ]);
    assert_eq!(serde_json::to_value(&declared).unwrap(), stored);
    assert_eq!(
        serde_json::from_value::<Vec<SecretDecl>>(stored).unwrap(),
        declared
    );
}

#[rstest]
#[case("tenants", "a prefix ends with `/`")]
#[case("/", "the prefix is empty")]
#[case("a//", "no empty part")]
fn a_bad_prefix_is_refused(#[case] prefix: &str, #[case] reason: &str) {
    let declared = SecretDecl::Prefix {
        storage: None,
        prefix: prefix.into(),
    };
    let error = declared.check().unwrap_err();
    assert!(error.contains(reason), "{error}");
    assert!(error.starts_with("`SecretPrefix("), "{error}");
}

#[test]
fn what_a_declaration_covers() {
    let one = SecretDecl::Secret(name(None, "etl/db", Some("password")));
    let whole = SecretDecl::Secret(name(None, "etl/db", None));
    let prefix = SecretDecl::Prefix {
        storage: None,
        prefix: "etl/".into(),
    };
    let other = SecretDecl::Prefix {
        storage: Some("other".into()),
        prefix: "etl/".into(),
    };
    let password = name(None, "etl/db", Some("password"));
    let user = name(None, "etl/db", Some("user"));
    let deep = name(None, "etl/x/y", Some("k"));
    let sibling = name(None, "etl_old/db", Some("k"));
    assert!(one.covers(&password) && !one.covers(&user));
    assert!(
        whole.covers(&password) && whole.covers(&user) && whole.covers(&name(None, "etl/db", None))
    );
    assert!(!one.covers(&name(None, "etl/db", None)));
    assert!(prefix.covers(&deep) && !prefix.covers(&sibling));
    assert!(!other.covers(&deep));
    assert!(other.covers(&name(Some("other"), "etl/a", None)));
}
