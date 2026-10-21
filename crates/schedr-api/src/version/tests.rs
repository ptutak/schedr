use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;

const SERVER: SchedrVersion = SchedrVersion::new(1, 4);

#[rstest]
#[case(None)]
#[case(Some("1.4.0"))]
#[case(Some("1.0"))]
#[case(Some("1.9.2"))]
fn the_same_major_or_no_version_is_served(#[case] client: Option<&str>) {
    assert_eq!(check_client_version(SERVER, client), Ok(()));
}

#[test]
fn another_major_is_refused_with_what_to_install() {
    assert_eq!(
        check_client_version(SERVER, Some("2.0.1")),
        Err(
            "the server is schedr 1.4, this client is schedr 2.0: `pip install 'schedr>=1,<2'`"
                .to_owned()
        )
    );
}

#[test]
fn a_version_that_doesnt_parse_is_refused() {
    assert_eq!(
        check_client_version(SERVER, Some("latest")),
        Err("`schedr-client-version: latest` isn't a schedr version".to_owned())
    );
}
