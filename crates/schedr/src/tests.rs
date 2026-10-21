use super::*;

#[test]
fn version_is_the_package_version() {
    assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
}

#[test]
fn banner_names_schedr_and_its_version() {
    assert_eq!(banner(), format!("schedr {}", env!("CARGO_PKG_VERSION")));
}
