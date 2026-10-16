//! Integration tests for the public `schedr_core` API.

#[test]
fn greet_is_public() {
    assert_eq!(schedr_core::greet("world"), "Hello, world!");
}
