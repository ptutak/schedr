use super::*;

#[test]
fn headers_are_lower_case_and_chunks_hold_a_character() {
    for header in [WORKER_ID_HEADER, LOG_SIZE_HEADER, RUNNING_HEADER] {
        assert_eq!(header, header.to_lowercase());
        assert!(header.starts_with("schedr-"), "{header}");
    }
    assert_eq!(MIN_CHUNK, 4);
    assert_eq!(MAX_CHUNK, 1 << 20);
}
