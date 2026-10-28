use axum::http::{HeaderMap, HeaderValue, header};
use pretty_assertions::assert_eq;

use super::*;

fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in pairs {
        map.append(*name, HeaderValue::from_str(value).unwrap());
    }
    map
}

#[test]
fn a_plain_prefix_is_used_without_its_trailing_slash() {
    assert_eq!(prefix(&headers(&[(PREFIX_HEADER, "/schedr/")])), "/schedr");
    assert_eq!(
        prefix(&headers(&[(PREFIX_HEADER, "/a/b-c_d.e~f")])),
        "/a/b-c_d.e~f"
    );
    assert_eq!(prefix(&HeaderMap::new()), "");
}

#[test]
fn a_prefix_that_isnt_a_plain_path_is_ignored() {
    for value in [
        "schedr",
        "/a/../b",
        "//evil.example",
        "/a\"onload=x",
        "/a?b",
        "/a b",
    ] {
        assert_eq!(prefix(&headers(&[(PREFIX_HEADER, value)])), "", "{value}");
    }
}

#[test]
fn the_zone_comes_from_its_cookie_when_known() {
    let with = |cookie: &str| zone(&headers(&[(header::COOKIE.as_str(), cookie)]));
    assert_eq!(
        with("a=1; schedr_tz=Europe%2FWarsaw; b=2").and_then(|z| z.iana_name().map(str::to_owned)),
        Some("Europe/Warsaw".to_owned())
    );
    assert_eq!(
        with("schedr_tz=America/New_York").and_then(|z| z.iana_name().map(str::to_owned)),
        Some("America/New_York".to_owned())
    );
    assert!(with("schedr_tz=Nowhere/Atlantis").is_none());
    assert!(with("other=1").is_none());
}

#[test]
fn times_are_utc_until_the_zone_is_known_then_both() {
    let at: Timestamp = "2026-10-15T03:00:00Z".parse().unwrap();
    let utc_only = Page::new(&HeaderMap::new(), false);
    assert!(!utc_only.has_zone());
    assert_eq!(utc_only.time(&at), "2026-10-15 03:00 UTC");
    let warsaw = Page::new(
        &headers(&[(header::COOKIE.as_str(), "schedr_tz=Europe%2FWarsaw")]),
        false,
    );
    assert_eq!(warsaw.time(&at), "2026-10-15 03:00 UTC · 05:00 CEST");
}

#[test]
fn the_zone_cookie_is_decoded_as_encode_uri_component_wrote_it() {
    let with = |cookie: &str| zone(&headers(&[(header::COOKIE.as_str(), cookie)]));
    let name = |cookie: &str| with(cookie).and_then(|z| z.iana_name().map(str::to_owned));
    assert_eq!(
        name("schedr_tz=Etc%2FGMT%2B1"),
        Some("Etc/GMT+1".to_owned())
    );
    assert_eq!(
        name("schedr_tz=America%2FArgentina%2FBuenos_Aires"),
        Some("America/Argentina/Buenos_Aires".to_owned())
    );
    for bad in [
        "schedr_tz=Europe%2",
        "schedr_tz=Europe%ZZWarsaw",
        "schedr_tz=%FF",
    ] {
        assert!(with(bad).is_none(), "{bad}");
    }
}
