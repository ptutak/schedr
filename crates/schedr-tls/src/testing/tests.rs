use tempfile::TempDir;

use super::*;
use crate::material::build;

#[test]
fn a_ca_signs_certificates_usable_by_servers_and_clients() {
    let ca = TestCa::new("schedr test CA");
    assert!(ca.pem().starts_with("-----BEGIN CERTIFICATE-----"));
    let issued = ca.issue(&["schedr-worker", "127.0.0.1"]);
    assert!(issued.cert.contains("BEGIN CERTIFICATE"));
    assert!(issued.key.contains("PRIVATE KEY"));
}

#[test]
fn write_puts_the_files_where_tls_files_say() {
    let dir = TempDir::new().unwrap();
    let ca = TestCa::new("schedr test CA");
    let files = ca.write(dir.path(), "master", &["127.0.0.1"]);
    assert_eq!(files.ca, dir.path().join("master-ca.crt"));
    assert_eq!(files.cert, dir.path().join("master.crt"));
    assert_eq!(files.key, dir.path().join("master.key"));
    assert!(build(&files.read().unwrap(), &files, "schedr-worker").is_ok());
}
