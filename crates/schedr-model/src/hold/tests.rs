use super::HoldId;

const ATTEMPT: &str = "01a21948-5911-7459-a18d-36639baeb4b0";

#[test]
fn the_tasks_own_run_is_the_attempts_id() {
    let own = HoldId::own(ATTEMPT);
    assert_eq!(own.to_string(), ATTEMPT);
    assert_eq!(HoldId::parse(ATTEMPT), own);
    assert!(own.ran_the_task());
}

#[test]
fn a_moved_cleanups_hold_carries_its_number() {
    let hold = HoldId::new(ATTEMPT, 2);
    assert_eq!(hold.to_string(), format!("{ATTEMPT}_2"));
    let parsed = HoldId::parse(&format!("{ATTEMPT}_2"));
    assert_eq!((parsed.attempt(), parsed.holder()), (ATTEMPT, 2));
    assert!(!parsed.ran_the_task());
    assert_eq!(HoldId::new(ATTEMPT, 0), HoldId::own(ATTEMPT));
}

#[test]
fn a_suffix_that_isnt_a_holder_stays_part_of_the_id() {
    for wire in [
        format!("{ATTEMPT}_0"),
        format!("{ATTEMPT}_02"),
        format!("{ATTEMPT}_x"),
        format!("{ATTEMPT}_"),
        "_3".to_owned(),
        format!("{ATTEMPT}_99999999999"),
    ] {
        let parsed = HoldId::parse(&wire);
        assert_eq!(
            (parsed.attempt(), parsed.holder()),
            (wire.as_str(), 0),
            "{wire}"
        );
    }
}

/// agy's fifth review of B304: one spelling per hold, so a string and a typed comparison
/// agree; a sign, a space or a leading zero leaves the suffix part of an unknown ID.
#[test]
fn a_hold_has_one_spelling() {
    for wire in [
        format!("{ATTEMPT}_+1"),
        format!("{ATTEMPT}_+01"),
        format!("{ATTEMPT}_ 1"),
        format!("{ATTEMPT}_1 "),
        format!("{ATTEMPT}_１"),
    ] {
        let parsed = HoldId::parse(&wire);
        assert_eq!(parsed.holder(), 0, "{wire}");
        assert_eq!(parsed.to_string(), wire, "kept whole: an unknown attempt");
    }
    for n in [1_u32, 2, 10, 4_294_967_295] {
        let wire = format!("{ATTEMPT}_{n}");
        assert_eq!(HoldId::parse(&wire).to_string(), wire);
    }
}

/// agy's fifth review: one check of what a worker may name, every edge's: an attempt's UUID,
/// or a hold of it spelled as the master writes it.
#[test]
fn a_valid_hold_names_a_uuid_attempt() {
    assert!(HoldId::is_valid(ATTEMPT));
    assert!(HoldId::is_valid(&format!("{ATTEMPT}_3")));
    for bad in [
        String::new(),
        "a1".to_owned(),
        format!("{ATTEMPT}_0"),
        format!("{ATTEMPT}_+1"),
        format!("{ATTEMPT}_x"),
        format!("{}_1", &ATTEMPT[..35]),
        format!("../{ATTEMPT}"),
        ATTEMPT.to_uppercase().replace('-', "_"),
    ] {
        assert!(!HoldId::is_valid(&bad), "{bad:?}");
    }
}
