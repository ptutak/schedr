use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;

#[rstest]
#[case(&[1, 2], &[], SchemaStatus::Uninitialized)]
#[case(&[1, 2], &[1, 2], SchemaStatus::UpToDate)]
#[case(&[1, 2, 3], &[1], SchemaStatus::Outdated { missing: vec![2, 3] })]
#[case(&[1], &[1, 2], SchemaStatus::Newer { unknown: vec![2] })]
#[case(&[1, 3], &[1, 2], SchemaStatus::Outdated { missing: vec![3] })]
#[case(&[], &[], SchemaStatus::UpToDate)]
#[case(&[], &[5], SchemaStatus::Newer { unknown: vec![5] })]
fn compares_embedded_and_applied_versions(
    #[case] embedded: &[i64],
    #[case] applied: &[i64],
    #[case] expected: SchemaStatus,
) {
    assert_eq!(SchemaStatus::compare(embedded, applied), expected);
}

#[rstest]
#[case(&[(1, b"a"), (2, b"b")], &[(1, b"a"), (2, b"b")], SchemaStatus::UpToDate)]
#[case(&[(1, b"a"), (2, b"b")], &[(1, b"x"), (2, b"b")], SchemaStatus::Modified { changed: vec![1] })]
#[case(&[(1, b"a"), (2, b"b")], &[(1, b"x")], SchemaStatus::Modified { changed: vec![1] })]
#[case(&[(1, b"a")], &[(1, b"a"), (2, b"z")], SchemaStatus::Newer { unknown: vec![2] })]
#[case(&[(1, b"a"), (2, b"b")], &[(1, b"a")], SchemaStatus::Outdated { missing: vec![2] })]
#[case(&[(1, b"a")], &[], SchemaStatus::Uninitialized)]
fn a_migration_applied_with_other_contents_makes_the_schema_modified(
    #[case] embedded: &[(i64, &[u8; 1])],
    #[case] applied: &[(i64, &[u8; 1])],
    #[case] expected: SchemaStatus,
) {
    fn slices<'a>(list: &[(i64, &'a [u8; 1])]) -> Vec<(i64, &'a [u8])> {
        list.iter().map(|(v, c)| (*v, c.as_slice())).collect()
    }
    assert_eq!(
        SchemaStatus::compare_checksums(&slices(embedded), &slices(applied)),
        expected
    );
}

#[rstest]
#[case(SchemaStatus::Modified { changed: vec![1] }, false)]
#[case(SchemaStatus::UpToDate, true)]
#[case(SchemaStatus::Newer { unknown: vec![9] }, true)]
#[case(SchemaStatus::Outdated { missing: vec![1] }, false)]
#[case(SchemaStatus::Uninitialized, false)]
fn usable_only_when_up_to_date_or_newer(#[case] status: SchemaStatus, #[case] usable: bool) {
    assert_eq!(status.is_usable(), usable);
}
