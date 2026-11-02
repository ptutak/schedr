use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use pretty_assertions::assert_eq;

use super::*;
use crate::app::load_config;

const KEY: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

struct Setup {
    _dir: tempfile::TempDir,
    root: PathBuf,
    config: Config,
    env: BTreeMap<String, String>,
}

async fn setup() -> Setup {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let url = format!("sqlite://{}", root.join("schedr.db").display());
    Database::connect(&url)
        .await
        .unwrap()
        .migrate()
        .await
        .unwrap();
    let env: BTreeMap<String, String> = [
        ("SCHEDR_CONFIG__DATABASE__URL", url.as_str()),
        ("SCHEDR_SECRET_KEY", KEY),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();
    let config = load_config(None, &env, |_: &Path| {
        Err(io::Error::from(io::ErrorKind::NotFound))
    })
    .unwrap();
    Setup {
        _dir: dir,
        root,
        config,
        env,
    }
}

async fn secrets(setup: &Setup, args: &[&str], stdin: &str) -> Result<String, AppError> {
    use clap::Parser as _;
    let cli = crate::cli::Cli::try_parse_from(
        std::iter::once("schedr")
            .chain(std::iter::once("secrets"))
            .chain(args.iter().copied()),
    )
    .unwrap();
    let crate::cli::Command::Secrets { command } = cli.command else {
        panic!("not secrets");
    };
    let mut out = Vec::new();
    run(
        &command,
        &setup.config,
        &setup.env,
        &mut stdin.as_bytes(),
        &mut out,
    )
    .await?;
    Ok(String::from_utf8(out).unwrap())
}

#[tokio::test]
async fn set_list_get_and_delete() {
    let s = setup().await;
    let file = s.root.join("cert.pem");
    fs::write(&file, "-----BEGIN-----\nabc\n").unwrap();
    let out = secrets(
        &s,
        &[
            "set",
            "etl/db",
            "user=etl",
            &format!("cert=@{}", file.display()),
            "password=-",
        ],
        "hunter22\n",
    )
    .await
    .unwrap();
    assert_eq!(out, "set `etl/db`: cert, password, user\n");
    // Merged: the other keys are kept.
    secrets(&s, &["set", "etl/db", "user=other"], "")
        .await
        .unwrap();
    secrets(&s, &["set", "web/api", "token=t0k3n"], "")
        .await
        .unwrap();

    let list = secrets(&s, &["list", "etl/"], "").await.unwrap();
    assert!(list.starts_with("etl/db\tcert,password,user\t"), "{list}");
    assert_eq!(list.lines().count(), 1);
    assert!(!list.contains("hunter22"));
    assert_eq!(
        secrets(&s, &["get", "etl/db#password"], "").await.unwrap(),
        "hunter22\n"
    );
    assert_eq!(
        secrets(&s, &["get", "etl/db#cert"], "").await.unwrap(),
        "-----BEGIN-----\nabc\n\n"
    );
    assert_eq!(
        secrets(&s, &["get", "etl/db#user"], "").await.unwrap(),
        "other\n"
    );
    let whole: BTreeMap<String, String> =
        serde_json::from_str(&secrets(&s, &["get", "web/api"], "").await.unwrap()).unwrap();
    assert_eq!(
        whole,
        BTreeMap::from([("token".to_owned(), "t0k3n".to_owned())])
    );

    let err = secrets(&s, &["get", "etl/db#nope"], "").await.unwrap_err();
    assert_eq!(err.to_string(), "the secret `etl/db` has no key `nope`");
    assert_eq!(
        secrets(&s, &["delete", "etl/db"], "").await.unwrap(),
        "deleted `etl/db`\n"
    );
    let err = secrets(&s, &["get", "etl/db#user"], "").await.unwrap_err();
    assert_eq!(err.to_string(), "no secret `etl/db`");
    let err = secrets(&s, &["delete", "etl/db"], "").await.unwrap_err();
    assert_eq!(err.to_string(), "no secret `etl/db`");
}

#[tokio::test]
async fn bad_arguments_are_refused_without_showing_a_value() {
    let s = setup().await;
    for (args, message) in [
        (
            &["set", "etl/db", "hunter22"][..],
            "each value is KEY=VALUE, KEY=@FILE or KEY=-",
        ),
        (&["set", "etl/db", "a=1", "a=2"][..], "`a` is given twice"),
        (
            &["set", "etl/db", "a=-", "b=-"][..],
            "only one value can be read from standard input (`KEY=-`)",
        ),
        (
            &["set", "etl/../x", "a=1"][..],
            "`etl/../x`: a path has no `.` or `..` part",
        ),
    ] {
        let err = secrets(&s, args, "x").await.unwrap_err();
        assert_eq!(err.to_string(), message, "{args:?}");
    }
    let err = secrets(&s, &["set", "/etl", "a=1"], "").await.unwrap_err();
    assert!(err.to_string().contains("no empty part"), "{err}");
}

#[tokio::test]
async fn without_the_key_values_cant_be_set_or_read_but_are_listed() {
    let mut s = setup().await;
    secrets(&s, &["set", "etl/db", "password=hunter22"], "")
        .await
        .unwrap();
    s.env.remove("SCHEDR_SECRET_KEY");
    let err = secrets(&s, &["get", "etl/db#password"], "")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("`SCHEDR_SECRET_KEY` isn't set"),
        "{err}"
    );
    assert!(secrets(&s, &["set", "x", "a=1"], "").await.is_err());
    assert!(
        secrets(&s, &["list"], "")
            .await
            .unwrap()
            .starts_with("etl/db\tpassword\t")
    );
    // Another key: refused, never garbage.
    s.env.insert(
        "SCHEDR_SECRET_KEY".to_owned(),
        SecretKey::generate().unwrap().to_base64(),
    );
    let err = secrets(&s, &["get", "etl/db#password"], "")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("can't be decrypted"), "{err}");
}

#[tokio::test]
async fn export_writes_one_private_file_per_path_for_vault() {
    let s = setup().await;
    secrets(&s, &["set", "etl/db", "user=etl", "password=hunter22"], "")
        .await
        .unwrap();
    secrets(&s, &["set", "top", "a=1"], "").await.unwrap();
    let to = s.root.join("export");
    let out = secrets(&s, &["export", "--to", to.to_str().unwrap()], "")
        .await
        .unwrap();
    assert!(out.starts_with("exported 2 secret(s) to "), "{out}");
    assert!(!out.contains("hunter22"));
    let file = to.join("etl/db.json");
    let data: BTreeMap<String, String> =
        serde_json::from_str(&fs::read_to_string(&file).unwrap()).unwrap();
    assert_eq!(
        data,
        BTreeMap::from([
            ("password".to_owned(), "hunter22".to_owned()),
            ("user".to_owned(), "etl".to_owned()),
        ])
    );
    assert_eq!(
        fs::metadata(&file).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(to.join("etl")).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert!(to.join("top.json").is_file());
    // Not into a directory that has something in it.
    let err = secrets(&s, &["export", "--to", to.to_str().unwrap()], "")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("isn't empty"), "{err}");
}

#[tokio::test]
async fn new_key_prints_a_usable_key() {
    let s = setup().await;
    let out = secrets(&s, &["new-key"], "").await.unwrap();
    let key = SecretKey::from_base64(out.trim()).unwrap();
    assert_ne!(key.to_base64(), KEY);
}

#[tokio::test]
async fn an_unmigrated_database_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("new.db").display());
    Database::connect(&url).await.unwrap();
    let mut s = setup().await;
    s.config.database.url = Some(url);
    let err = secrets(&s, &["list"], "").await.unwrap_err();
    assert!(err.to_string().contains("run `schedr db migrate`"), "{err}");
}

#[tokio::test]
async fn export_refuses_paths_it_cant_write_side_by_side_before_writing_any() {
    // Codex's finding in M4.6c's review: `a` is the file `a.json`, `a.json/b` needs it as a
    // directory.
    let s = setup().await;
    secrets(&s, &["set", "a", "k=v"], "").await.unwrap();
    secrets(&s, &["set", "a.json/b", "k=v"], "").await.unwrap();
    let to = s.root.join("export");
    let err = secrets(&s, &["export", "--to", to.to_str().unwrap()], "")
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .starts_with("`a` and `a.json/b` can't both be exported"),
        "{err}"
    );
    assert!(!to.exists());
}

#[tokio::test]
async fn control_characters_and_a_linked_directory_are_refused() {
    // Grok's findings in M4.6c's security review.
    let s = setup().await;
    let err = secrets(&s, &["set", "ok\nINJECTED", "k=v"], "")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no control characters"), "{err}");
    let err = secrets(&s, &["set", "ok", "k\tx=v"], "").await.unwrap_err();
    assert!(err.to_string().contains("no control characters"), "{err}");
    secrets(&s, &["set", "ok", "k=v"], "").await.unwrap();
    let target = s.root.join("target");
    fs::create_dir(&target).unwrap();
    let link = s.root.join("link");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    let err = secrets(&s, &["export", "--to", link.to_str().unwrap()], "")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("symbolic link"), "{err}");
    assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
}

#[test]
fn a_failed_write_removes_what_export_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let data = BTreeMap::from([("k".to_owned(), "v".to_owned())]);
    let mut written = Vec::new();
    write_secret(&dir.path().join("a.json"), &data, &mut written).unwrap();
    // The same file again: it exists, so the write fails.
    assert!(write_secret(&dir.path().join("a.json"), &data, &mut written).is_err());
    assert_eq!(written, [dir.path().join("a.json")]);
}
