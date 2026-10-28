use std::time::Duration;

use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::*;
use crate::testing::TestCa;

fn setup() -> (TempDir, TestCa, Tls) {
    let dir = TempDir::new().unwrap();
    let ca = TestCa::new("schedr test CA");
    let files = ca.write(dir.path(), "role", &["schedr-master"]);
    let tls = Tls::load(files, "schedr-worker", "schedr-ui").unwrap();
    (dir, ca, tls)
}

fn rewrite(dir: &TempDir, ca: &TestCa) {
    let issued = ca.issue(&["schedr-master"]);
    std::fs::write(dir.path().join("role.crt"), issued.cert).unwrap();
    std::fs::write(dir.path().join("role.key"), issued.key).unwrap();
}

#[test]
fn loads_and_hands_out_every_configuration() {
    let (_dir, _ca, tls) = setup();
    assert_eq!(tls.generation(), 0);
    assert_eq!(tls.grpc_server().alpn_protocols, [b"h2".to_vec()]);
    assert_eq!(tls.http_server().alpn_protocols, [b"http/1.1".to_vec()]);
    assert_eq!(tls.grpc_client().alpn_protocols, [b"h2".to_vec()]);
    assert_eq!(tls.worker_client().alpn_protocols, [b"http/1.1".to_vec()]);
}

#[test]
fn load_fails_on_a_missing_file() {
    let dir = TempDir::new().unwrap();
    let files = TlsFiles {
        ca: dir.path().join("ca.crt"),
        cert: dir.path().join("tls.crt"),
        key: dir.path().join("tls.key"),
    };
    let error = Tls::load(files, "w", "schedr-ui").unwrap_err().to_string();
    assert!(error.starts_with("can't read [tls] ca"), "{error}");
}

#[test]
fn reload_uses_changed_files_and_keeps_the_old_ones_when_they_break() {
    let (dir, ca, tls) = setup();
    assert!(matches!(tls.reload(), Reload::Unchanged));

    rewrite(&dir, &ca);
    assert!(matches!(tls.reload(), Reload::Reloaded));
    assert_eq!(tls.generation(), 1);
    let in_use = tls.grpc_server();

    std::fs::write(dir.path().join("role.key"), "half written").unwrap();
    let Reload::Failed(error) = tls.reload() else {
        panic!("a broken key must fail");
    };
    assert!(
        error.to_string().contains("holds no usable private key"),
        "{error}"
    );
    // Reported once; the material in use stays.
    assert!(matches!(tls.reload(), Reload::Unchanged));
    assert_eq!(tls.generation(), 1);
    assert!(Arc::ptr_eq(&in_use, &tls.grpc_server()));

    rewrite(&dir, &ca);
    assert!(matches!(tls.reload(), Reload::Reloaded));
    assert_eq!(tls.generation(), 2);
}

#[test]
fn a_file_that_disappears_is_reported_once() {
    let (dir, _ca, tls) = setup();
    std::fs::remove_file(dir.path().join("role.crt")).unwrap();
    assert!(matches!(
        tls.reload(),
        Reload::Failed(TlsError::Read { .. })
    ));
    assert!(matches!(tls.reload(), Reload::Unchanged));
}

#[tokio::test]
async fn watch_reloads_until_stopped() {
    let (dir, ca, tls) = setup();
    rewrite(&dir, &ca);
    tls.clone()
        .watch(
            Duration::from_millis(10),
            tokio::time::sleep(Duration::from_millis(200)),
        )
        .await;
    assert_eq!(tls.generation(), 1);
    // A broken change is logged, and the watch goes on.
    std::fs::write(dir.path().join("role.crt"), "broken").unwrap();
    tls.clone()
        .watch(
            Duration::from_millis(10),
            tokio::time::sleep(Duration::from_millis(100)),
        )
        .await;
    assert_eq!(tls.generation(), 1);
}
