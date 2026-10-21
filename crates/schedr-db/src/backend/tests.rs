use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;

#[rstest]
#[case("sqlite::memory:", Backend::Sqlite)]
#[case("sqlite:///var/lib/schedr/schedr.db", Backend::Sqlite)]
#[case("sqlite:schedr.db?mode=rwc", Backend::Sqlite)]
#[case("postgres://schedr@db:5432/schedr", Backend::Postgres)]
#[case("postgresql://schedr:secret@db/schedr", Backend::Postgres)]
fn from_url_recognizes_the_backend(#[case] url: &str, #[case] expected: Backend) {
    assert_eq!(Backend::from_url(url), Ok(expected));
}

#[rstest]
#[case("mysql://user:pw@host/db", "mysql://user:***@host/db")]
#[case("db.sqlite", "db.sqlite")]
#[case("", "")]
fn from_url_rejects_other_schemes_without_leaking_passwords(
    #[case] url: &str,
    #[case] shown: &str,
) {
    let err = Backend::from_url(url).unwrap_err();
    assert_eq!(err, UnsupportedUrl(shown.to_owned()));
    assert_eq!(
        err.to_string(),
        format!("unsupported database URL `{shown}` (use `sqlite:...` or `postgres://...`)")
    );
}

#[rstest]
#[case(
    "postgres://schedr:s3cret@db:5432/schedr",
    "postgres://schedr:***@db:5432/schedr"
)]
#[case("postgres://schedr:p@ss@db/schedr", "postgres://schedr:***@db/schedr")]
#[case("postgres://schedr@db/schedr", "postgres://schedr@db/schedr")]
#[case("postgres://db/schedr", "postgres://db/schedr")]
#[case("postgres://u:pw@db", "postgres://u:***@db")]
#[case("sqlite::memory:", "sqlite::memory:")]
fn redact_password_hides_only_the_password(#[case] url: &str, #[case] expected: &str) {
    assert_eq!(redact_password(url), expected);
}

#[test]
fn both_backends_have_the_same_migration_versions() {
    let sqlite = Backend::Sqlite.migration_versions();
    assert_eq!(sqlite, Backend::Postgres.migration_versions());
    assert_eq!(sqlite.first(), Some(&1));
}

#[test]
fn migrators_belong_to_their_backend() {
    let sqlite_sql = Backend::Sqlite
        .migrator()
        .iter()
        .next()
        .unwrap()
        .sql
        .as_str();
    let postgres_sql = Backend::Postgres
        .migrator()
        .iter()
        .next()
        .unwrap()
        .sql
        .as_str();
    assert!(sqlite_sql.contains("(SQLite)"));
    assert!(postgres_sql.contains("(Postgres)"));
    assert!(postgres_sql.contains("TIMESTAMPTZ"));
}
