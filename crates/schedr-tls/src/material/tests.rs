use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::*;
use crate::testing::TestCa;

fn files(dir: &TempDir) -> TlsFiles {
    TestCa::new("schedr test CA").write(dir.path(), "role", &["schedr-master", "127.0.0.1"])
}

#[test]
fn builds_every_configuration_with_its_alpn() {
    let dir = TempDir::new().unwrap();
    let files = files(&dir);
    let configs = build(&files.read().unwrap(), &files, "schedr-worker").unwrap();
    assert_eq!(configs.grpc_server.alpn_protocols, [ALPN_H2.to_vec()]);
    assert_eq!(configs.http_server.alpn_protocols, [ALPN_HTTP1.to_vec()]);
    assert_eq!(configs.grpc_client.alpn_protocols, [ALPN_H2.to_vec()]);
    assert_eq!(configs.worker_client.alpn_protocols, [ALPN_HTTP1.to_vec()]);
}

#[test]
fn reads_the_three_files_and_names_the_one_missing() {
    let dir = TempDir::new().unwrap();
    let files = files(&dir);
    let contents = files.read().unwrap();
    assert!(String::from_utf8_lossy(&contents.ca).contains("BEGIN CERTIFICATE"));
    assert!(String::from_utf8_lossy(&contents.key).contains("PRIVATE KEY"));
    std::fs::remove_file(&files.key).unwrap();
    let error = files.read().unwrap_err().to_string();
    assert!(
        error.starts_with(&format!("can't read [tls] key `{}`: ", files.key.display())),
        "{error}"
    );
}

#[test]
fn a_file_without_a_certificate_or_key_is_named() {
    let dir = TempDir::new().unwrap();
    let files = files(&dir);
    let good = files.read().unwrap();

    let no_ca = Contents {
        ca: b"not pem".to_vec(),
        ..good.clone()
    };
    assert_eq!(
        build(&no_ca, &files, "w").unwrap_err().to_string(),
        format!("[tls] ca `{}` holds no certificate", files.ca.display())
    );
    let no_cert = Contents {
        cert: good.key.clone(),
        ..good.clone()
    };
    assert_eq!(
        build(&no_cert, &files, "w").unwrap_err().to_string(),
        format!("[tls] cert `{}` holds no certificate", files.cert.display())
    );
    let no_key = Contents {
        key: good.cert.clone(),
        ..good
    };
    let error = build(&no_key, &files, "w").unwrap_err().to_string();
    assert!(
        error.starts_with(&format!(
            "[tls] key `{}` holds no usable private key",
            files.key.display()
        )),
        "{error}"
    );
}

#[test]
fn a_key_of_another_certificate_is_refused() {
    let dir = TempDir::new().unwrap();
    let files = files(&dir);
    let other = TestCa::new("other").issue(&["other"]);
    let contents = Contents {
        key: other.key.into_bytes(),
        ..files.read().unwrap()
    };
    let error = build(&contents, &files, "w").unwrap_err().to_string();
    assert!(
        error.starts_with(&format!(
            "[tls] cert `{}` and key `{}` can't be used together: ",
            files.cert.display(),
            files.key.display()
        )),
        "{error}"
    );
}

#[test]
fn a_worker_name_must_be_a_name() {
    let dir = TempDir::new().unwrap();
    let files = files(&dir);
    let error = build(&files.read().unwrap(), &files, "not a name!")
        .unwrap_err()
        .to_string();
    assert_eq!(
        error,
        "[tls] worker_name `not a name!` isn't a DNS name or an IP address"
    );
}

#[test]
fn the_provider_is_aws_lc_rs() {
    assert_ne!(provider().cipher_suites.len(), 0);
}
