use std::time::{Duration, SystemTime};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};

use super::*;

fn end_entity(contents: &Contents) -> CertificateDer<'static> {
    CertificateDer::from_pem_slice(&contents.cert).unwrap()
}

#[test]
fn a_certificate_names_the_given_names_and_lives_a_day() {
    let ephemeral = Ephemeral::new(&["127.0.0.1", "schedr-worker"]).unwrap();
    let now = SystemTime::now();
    let contents = ephemeral.issue(now).unwrap();
    let der = end_entity(&contents);
    let cert = webpki::EndEntityCert::try_from(&der).unwrap();
    for name in ["127.0.0.1", "schedr-worker"] {
        let name = ServerName::try_from(name).unwrap();
        cert.verify_is_valid_for_subject_name(&name).unwrap();
    }
    assert!(
        cert.verify_is_valid_for_subject_name(&ServerName::try_from("localhost").unwrap())
            .is_err()
    );
    // Valid now and just before the day's end; not after it.
    let roots = {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(&contents.ca).unwrap())
            .unwrap();
        roots
    };
    let verify = |at: SystemTime| {
        let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
            std::sync::Arc::new(roots.clone()),
            crate::material::provider(),
        )
        .build()
        .unwrap();
        let at = UnixTime::since_unix_epoch(at.duration_since(SystemTime::UNIX_EPOCH).unwrap());
        rustls::client::danger::ServerCertVerifier::verify_server_cert(
            verifier.as_ref(),
            &der,
            &[],
            &ServerName::try_from("schedr-worker").unwrap(),
            &[],
            at,
        )
    };
    verify(now).unwrap();
    verify(now + LIFETIME - Duration::from_mins(1)).unwrap();
    let late = verify(now + LIFETIME + Duration::from_mins(1)).unwrap_err();
    assert!(late.to_string().contains("expired"), "{late}");
}

#[test]
fn each_renewal_is_a_new_key_signed_by_the_same_ca() {
    let ephemeral = Ephemeral::new(&["schedr-worker"]).unwrap();
    let first = ephemeral.issue(SystemTime::now()).unwrap();
    let second = ephemeral.issue(SystemTime::now()).unwrap();
    assert_eq!(first.ca, second.ca);
    assert_ne!(first.key, second.key);
    let tls = Tls::in_memory(first, "schedr-worker", "schedr-worker").unwrap();
    assert_eq!(tls.generation(), 0);
    tls.replace(second).unwrap();
    assert_eq!(tls.generation(), 1);
    // Files are never read for it.
    assert!(matches!(tls.reload(), crate::Reload::Unchanged));
}

#[test]
fn its_debug_shows_no_key() {
    let ephemeral = Ephemeral::new(&["schedr-worker"]).unwrap();
    let shown = format!("{ephemeral:?}");
    assert!(!shown.contains("PRIVATE"), "{shown}");
    assert!(shown.contains("schedr-worker"), "{shown}");
}

#[tokio::test]
async fn renewing_replaces_the_certificate_until_stopped() {
    let ephemeral = Ephemeral::new(&["schedr-worker"]).unwrap();
    let tls = ephemeral.tls("schedr-worker").unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let renewing = tokio::spawn(
        ephemeral.renew(tls.clone(), Duration::from_millis(20), async {
            let _ = stopped.await;
        }),
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while tls.generation() < 2 {
        assert!(std::time::Instant::now() < deadline, "not renewed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.send(()).unwrap();
    renewing.await.unwrap();
}
