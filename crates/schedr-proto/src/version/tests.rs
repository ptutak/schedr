use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;

const fn v(major: u32, minor: u32) -> ProtocolVersion {
    ProtocolVersion { major, minor }
}

#[test]
fn the_current_version_is_0_1() {
    assert_eq!(PROTOCOL_VERSION, v(0, 1));
}

#[rstest]
#[case(v(1, 4), v(1, 4))]
#[case(v(1, 4), v(1, 0))]
fn accepts_the_same_major_and_an_equal_or_lower_minor(
    #[case] master: ProtocolVersion,
    #[case] worker: ProtocolVersion,
) {
    assert_eq!(check_worker_version(master, Some(worker)), Ok(()));
}

#[test]
fn rejects_a_missing_version() {
    assert_eq!(
        check_worker_version(v(1, 0), None),
        Err(VersionMismatch::Missing)
    );
    assert_eq!(
        VersionMismatch::Missing.to_string(),
        "the worker sent no protocol version"
    );
}

#[rstest]
#[case(v(2, 0))]
#[case(v(0, 9))]
fn rejects_another_major(#[case] worker: ProtocolVersion) {
    let err = check_worker_version(v(1, 3), Some(worker)).unwrap_err();
    assert_eq!(
        err,
        VersionMismatch::Major {
            worker_major: worker.major,
            worker_minor: worker.minor,
            master_major: 1,
            master_minor: 3
        }
    );
    assert!(err.to_string().contains("different major version"), "{err}");
}

#[test]
fn rejects_a_newer_minor_and_says_to_upgrade_the_master() {
    let err = check_worker_version(v(1, 3), Some(v(1, 4))).unwrap_err();
    assert_eq!(
        err.to_string(),
        "worker protocol 1.4 is newer than the master's 1.3; upgrade the master first"
    );
}
