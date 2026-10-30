use super::*;

#[test]
fn a_stat_line_gives_its_state_and_group() {
    let line = |state: &str, pgrp: i32| format!("4242 (sh) {state} 1 {pgrp} {pgrp} 0 -1 4194560");
    assert_eq!(stat_fields(&line("S", 7)), Some(("S", 7)));
    assert_eq!(stat_fields(&line("Z", 8)), Some(("Z", 8)));
    // A command name may hold spaces and parentheses: the fields start after the last `)`.
    assert_eq!(stat_fields("4242 (a) Z (b) S 1 7 7 0"), Some(("S", 7)));
    assert_eq!(stat_fields("garbage"), None);
    assert_eq!(stat_fields("4242 (sh) S"), None);
}

#[test]
fn only_a_zombie_or_dead_state_has_ended() {
    for state in ["R", "S", "D", "T", "t", "I"] {
        assert!(!ended(state), "{state}");
    }
    for state in ["Z", "X", "x"] {
        assert!(ended(state), "{state}");
    }
}
