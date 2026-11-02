use pretty_assertions::assert_eq;

use super::*;

fn key() -> SecretKey {
    SecretKey::generate().unwrap()
}

fn values(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

async fn migrated() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db
}

/// The row's `data` as stored.
async fn raw_data(db: &Database, path: &str) -> Result<String, DbError> {
    let (raw,): (String,) = on_pool!(db, |pool, backend| {
        sqlx::query_as(sql(
            backend,
            "SELECT data{text} FROM secrets WHERE path = $1",
        ))
        .bind(path)
        .fetch_one(pool)
        .await?
    });
    Ok(raw)
}

const NOW: &str = "2026-11-02T20:00:00Z";

#[test]
fn a_value_decrypts_only_for_its_own_path_and_key() {
    let key = key();
    let sealed = key.encrypt("etl/db", "password", "hunter22").unwrap();
    assert!(
        sealed.starts_with("v1:") && !sealed.contains("hunter22"),
        "{sealed}"
    );
    assert_eq!(
        key.decrypt("etl/db", "password", &sealed).unwrap(),
        "hunter22"
    );
    // Moved to another key or path, or read under another key: refused.
    for (path, name) in [("etl/db", "user"), ("etl/other", "password")] {
        let error = key.decrypt(path, name, &sealed).unwrap_err();
        assert!(error.to_string().contains("can't be decrypted"), "{error}");
    }
    assert!(self::key().decrypt("etl/db", "password", &sealed).is_err());
    // A fresh nonce each time.
    assert_ne!(
        sealed,
        key.encrypt("etl/db", "password", "hunter22").unwrap()
    );
    for bad in ["", "v1:", "v1:!!", "v2:AAAA", &sealed[..sealed.len() - 4]] {
        assert!(key.decrypt("etl/db", "password", bad).is_err(), "{bad}");
    }
}

#[test]
fn a_key_is_read_from_base64_and_never_shown() {
    let key = key();
    let again = SecretKey::from_base64(&key.to_base64()).unwrap();
    let sealed = key.encrypt("a", "k", "v").unwrap();
    assert_eq!(again.decrypt("a", "k", &sealed).unwrap(), "v");
    assert_eq!(format!("{key:?}"), "SecretKey(****)");
    let short = SecretKey::from_base64("c2hvcnQ=").unwrap_err().to_string();
    assert!(short.contains("must be 32 bytes, got 5"), "{short}");
    let garbage = SecretKey::from_base64("not base64!")
        .unwrap_err()
        .to_string();
    assert!(
        garbage.contains("isn't base64") && !garbage.contains("not base64!"),
        "{garbage}"
    );
    let unset = SecretKey::from_env(&BTreeMap::new())
        .unwrap_err()
        .to_string();
    assert!(unset.contains("`SCHEDR_SECRET_KEY` isn't set"), "{unset}");
    let env = BTreeMap::from([(SECRET_KEY_ENV.to_owned(), format!(" {} ", key.to_base64()))]);
    assert!(SecretKey::from_env(&env).is_ok());
}

#[tokio::test]
async fn secrets_are_set_merged_listed_read_and_deleted() {
    let db = migrated().await;
    let key = key();
    let now: Timestamp = NOW.parse().unwrap();
    db.set_secret(
        &key,
        "etl/db",
        &values(&[("user", "etl"), ("password", "hunter22")]),
        now,
    )
    .await
    .unwrap();
    // Setting a key keeps the others.
    db.set_secret(&key, "etl/db", &values(&[("password", "changed-1")]), now)
        .await
        .unwrap();
    db.set_secret(&key, "tenants/a", &values(&[("token", "t-a")]), now)
        .await
        .unwrap();
    assert_eq!(
        db.secret(&key, "etl/db").await.unwrap(),
        Some(values(&[("password", "changed-1"), ("user", "etl")]))
    );
    assert_eq!(db.secret(&key, "nope").await.unwrap(), None);
    let listed = db.secret_entries("").await.unwrap();
    let names: Vec<(&str, Vec<&str>)> = listed
        .iter()
        .map(|e| (e.path.as_str(), e.keys.iter().map(String::as_str).collect()))
        .collect();
    assert_eq!(
        names,
        [
            ("etl/db", vec!["password", "user"]),
            ("tenants/a", vec!["token"])
        ]
    );
    assert_eq!(db.secret_entries("tenants/").await.unwrap().len(), 1);
    // No value in the table.
    let raw = raw_data(&db, "etl/db").await.unwrap();
    assert!(
        !raw.contains("changed-1") && !raw.contains("etl\""),
        "{raw}"
    );
    // Under another key: refused, naming the value's place, not the value.
    let error = db
        .secret(&self::key(), "etl/db")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("`etl/db#password` can't be decrypted"),
        "{error}"
    );
    assert!(db.delete_secret("etl/db").await.unwrap());
    assert!(!db.delete_secret("etl/db").await.unwrap());
    assert_eq!(db.secret(&key, "etl/db").await.unwrap(), None);
}
