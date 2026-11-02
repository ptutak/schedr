use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn name(storage: Option<&str>, path: &str, key: Option<&str>) -> SecretName {
    SecretName {
        storage: storage.map(str::to_owned),
        path: path.to_owned(),
        key: key.map(str::to_owned),
    }
}

fn memory() -> MemoryStorage {
    MemoryStorage::new([
        (
            name(None, "etl/db", Some("password")),
            "hunter22".to_owned(),
        ),
        (name(None, "etl/db", Some("user")), "etl".to_owned()),
        (
            name(None, "tenants/a", Some("token")),
            "token-of-a".to_owned(),
        ),
        (
            name(None, "variables/bucket", Some("value")),
            "raw-bucket".to_owned(),
        ),
    ])
}

/// Counts its reads, and fails with `error` when set.
#[derive(Debug, Default)]
struct Counting {
    reads: AtomicUsize,
    error: Option<SecretError>,
}

impl SecretStorage for Counting {
    fn read<'a>(&'a self, _path: &'a str) -> ReadFuture<'a> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            match &self.error {
                Some(error) => Err(error.clone()),
                None => Ok(BTreeMap::from([("k".to_owned(), "value-1".to_owned())])),
            }
        })
    }
}

fn stores(storage: Storage) -> Arc<SecretStores> {
    Arc::new(
        SecretStores::new(
            BTreeMap::from([("main".to_owned(), storage)]),
            Some("main".to_owned()),
            5,
        )
        .unwrap(),
    )
}

#[test]
fn a_secret_spec_is_parsed() {
    assert_eq!(
        MemoryStorage::parse("etl/db#password=a=b").unwrap(),
        (name(None, "etl/db", Some("password")), "a=b".to_owned())
    );
    for bad in ["etl/db#password", "etl/db=x", "etl//db#k=x", "etl#=x"] {
        assert!(MemoryStorage::parse(bad).is_err(), "{bad}");
    }
}

#[tokio::test]
async fn keys_and_whole_secrets_are_fetched() {
    let stores = stores(Storage::new(Arc::new(memory())));
    let password = stores.fetch(&name(None, "etl/db", Some("password"))).await;
    assert_eq!(password, Ok(json!("hunter22")));
    let whole = stores.fetch(&name(None, "etl/db", None)).await;
    assert_eq!(whole, Ok(json!({"password": "hunter22", "user": "etl"})));
    let missing = stores.fetch(&name(None, "etl/db", Some("port"))).await;
    assert_eq!(missing.unwrap_err().kind(), "secret_not_found");
    let error = stores
        .fetch(&name(None, "nope", Some("k")))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "`secrets['nope#k']`: no such secret");
    let other = stores
        .fetch(&name(Some("vault"), "etl/db", Some("user")))
        .await;
    let other = other.unwrap_err().to_string();
    assert!(
        other.contains("no secret storage `vault` on this worker"),
        "{other}"
    );
}

#[tokio::test]
async fn a_worker_without_a_default_storage_says_so() {
    let stores = SecretStores::none();
    let error = stores.fetch(&name(None, "a", Some("k"))).await.unwrap_err();
    assert!(
        error.to_string().contains("no default secret storage"),
        "{error}"
    );
    assert!(SecretStores::new(BTreeMap::new(), Some("x".into()), 5).is_err());
}

#[tokio::test]
async fn fetched_secrets_are_cached_for_the_ttl() {
    let counting = Arc::new(Counting::default());
    let mut storage = Storage::new(counting.clone());
    storage.cache_ttl = Duration::from_millis(200);
    let stores = stores(storage);
    let k = name(None, "a", Some("k"));
    stores.fetch(&k).await.unwrap();
    stores.fetch(&name(None, "a", None)).await.unwrap();
    assert_eq!(counting.reads.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(250)).await;
    stores.fetch(&k).await.unwrap();
    assert_eq!(counting.reads.load(Ordering::SeqCst), 2);

    let uncached = Arc::new(Counting::default());
    let mut storage = Storage::new(uncached.clone());
    storage.cache_ttl = Duration::ZERO;
    let stores = self::stores(storage);
    stores.fetch(&k).await.unwrap();
    stores.fetch(&k).await.unwrap();
    assert_eq!(uncached.reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_storage_error_keeps_its_kind_and_names_the_secret() {
    for (error, kind) in [
        (
            SecretError::Unavailable("connection refused".into()),
            "secret_unavailable",
        ),
        (
            SecretError::Denied("permission denied".into()),
            "secret_denied",
        ),
        (
            SecretError::NotFound("no such path".into()),
            "secret_not_found",
        ),
    ] {
        let stores = stores(Storage::new(Arc::new(Counting {
            error: Some(error),
            ..Counting::default()
        })));
        let failed = stores.fetch(&name(None, "a", Some("k"))).await.unwrap_err();
        assert_eq!(failed.kind(), kind);
        assert!(
            failed.to_string().starts_with("`secrets['a#k']`: "),
            "{failed}"
        );
    }
}

#[tokio::test]
async fn an_attempt_reads_only_what_it_declares() {
    let mut storage = Storage::new(Arc::new(memory()));
    storage.unmasked_prefixes = vec!["variables/".into()];
    let stores = stores(storage);
    let declared = vec![
        SecretDecl::Secret(name(None, "etl/db", None)),
        SecretDecl::Prefix {
            storage: None,
            prefix: "tenants/".into(),
        },
        SecretDecl::Secret(name(None, "variables/bucket", Some("value"))),
    ];
    let needed = [
        name(None, "etl/db", None),
        name(None, "variables/bucket", Some("value")),
    ];
    let secrets = AttemptSecrets::fetch(stores, &needed, declared)
        .await
        .unwrap();
    // A key of the whole secret, from what was fetched; a prefix's secret, fetched now.
    let user = secrets.read(&name(None, "etl/db", Some("user"))).await;
    assert_eq!(user, Ok(json!("etl")));
    let token = secrets.read(&name(None, "tenants/a", Some("token"))).await;
    assert_eq!(token, Ok(json!("token-of-a")));
    assert_eq!(secrets.failed_kind(), None);
    let undeclared = secrets.read(&name(None, "other/x", Some("k"))).await;
    assert_eq!(undeclared.unwrap_err().kind(), "secret_denied");
    assert_eq!(secrets.failed_kind(), Some("secret_denied"));
    let missing = secrets.read(&name(None, "tenants/b", Some("token"))).await;
    assert_eq!(missing.unwrap_err().kind(), "secret_not_found");
    assert_eq!(secrets.failed_kind(), Some("secret_not_found"));
    // Masked: the fetched values (the prefix's too), not the unmasked prefix's, not "etl"
    // (too short, noted for the warning).
    let masked = secrets.mask.mask_text("hunter22 token-of-a raw-bucket etl");
    assert_eq!(masked, "**** **** raw-bucket etl");
    assert_eq!(
        *secrets.too_short.lock().unwrap(),
        [name(None, "etl/db", None)]
    );
}

#[tokio::test]
async fn a_test_run_answers_for_every_storage() {
    let stores = SecretStores::for_test(memory());
    assert_eq!(stores.names(), Vec::<String>::new());
    let named = stores
        .fetch(&name(Some("vault"), "etl/db", Some("password")))
        .await;
    assert_eq!(named, Ok(json!("hunter22")));
}

/// Answers once, then is down.
#[derive(Debug, Default)]
struct OnceThenDown {
    reads: AtomicUsize,
}

impl SecretStorage for OnceThenDown {
    fn read<'a>(&'a self, _path: &'a str) -> ReadFuture<'a> {
        Box::pin(async move {
            if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(BTreeMap::from([("k".to_owned(), "value-of-k".to_owned())]))
            } else {
                Err(SecretError::Unavailable("storage went down".into()))
            }
        })
    }
}

#[tokio::test]
async fn a_key_of_a_fetched_whole_secret_is_read_whichever_declaration_covers_it() {
    // A prefix declared before the whole secret (review of M4.6b): no second fetch.
    let storage = Arc::new(OnceThenDown::default());
    let mut uncached = Storage::new(storage.clone());
    uncached.cache_ttl = Duration::ZERO;
    let declared = vec![
        SecretDecl::Prefix {
            storage: None,
            prefix: "tenants/".into(),
        },
        SecretDecl::Secret(name(None, "tenants/a", None)),
    ];
    let needed = [name(None, "tenants/a", None)];
    let secrets = AttemptSecrets::fetch(stores(uncached), &needed, declared)
        .await
        .unwrap();
    let read = secrets.read(&name(None, "tenants/a", Some("k"))).await;
    assert_eq!(read, Ok(json!("value-of-k")));
    assert_eq!(storage.reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_short_value_is_noted_once() {
    let short = MemoryStorage::new([
        (name(None, "a", Some("x")), "1".to_owned()),
        (name(None, "a", Some("y")), "2".to_owned()),
    ]);
    let declared = vec![SecretDecl::Secret(name(None, "a", None))];
    let needed = [name(None, "a", None)];
    let secrets = AttemptSecrets::fetch(stores(Storage::new(Arc::new(short))), &needed, declared)
        .await
        .unwrap();
    assert_eq!(*secrets.too_short.lock().unwrap(), [name(None, "a", None)]);
}
