use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;

fn list(paths: &[&str]) -> PathList {
    PathList(paths.iter().map(PathBuf::from).collect())
}

fn root(name: &str, path: &str) -> Root {
    Root {
        name: name.into(),
        path: path.into(),
    }
}

fn resolve(paths: &[&str], renames: &[(&str, &str)]) -> Result<Vec<Root>, RootError> {
    let renames: BTreeMap<String, PathBuf> = renames
        .iter()
        .map(|(n, p)| ((*n).to_owned(), PathBuf::from(p)))
        .collect();
    resolve_roots("SCHEDR_DAG_PATH", "dags", &list(paths), &renames)
}

#[rstest]
#[case("/git/dags/a:/git/dags/b", &["/git/dags/a", "/git/dags/b"])]
#[case("/only", &["/only"])]
#[case("a::b:", &["a", "b"])]
#[case("", &[])]
fn path_strings_split_on_colons(#[case] s: &str, #[case] expected: &[&str]) {
    assert_eq!(PathList::from_path_string(s), list(expected));
}

#[derive(Debug, serde::Deserialize)]
struct WithPaths {
    path: PathList,
}

fn extract<T: serde::Serialize>(value: T) -> Result<PathList, Box<figment::Error>> {
    figment::Figment::from(figment::providers::Serialized::defaults(BTreeMap::from([
        ("path", value),
    ])))
    .extract::<WithPaths>()
    .map(|w| w.path)
    .map_err(Box::new)
}

#[test]
fn path_lists_deserialize_from_arrays_and_strings() {
    assert_eq!(extract(vec!["/a", "/b"]).unwrap(), list(&["/a", "/b"]));
    assert_eq!(extract("/a:/b").unwrap(), list(&["/a", "/b"]));
}

#[test]
fn path_lists_reject_other_values() {
    let err = extract(5).unwrap_err();
    assert!(err.to_string().contains("a list of paths"), "{err}");
}

#[rstest]
#[case("/git/dags/./a/", "/git/dags/a")]
#[case("relative/b", "relative/b")]
#[case("/git/../x", "/git/../x")]
fn normalize_removes_dots_and_trailing_separators_only(#[case] path: &str, #[case] expected: &str) {
    assert_eq!(normalize(Path::new(path)), PathBuf::from(expected));
}

#[test]
fn names_roots_after_their_last_component() {
    assert_eq!(
        resolve(&["/git/dags/etl-repo", "/git/other/ml"], &[]),
        Ok(vec![
            root("etl-repo", "/git/dags/etl-repo"),
            root("ml", "/git/other/ml")
        ])
    );
}

#[test]
fn renames_override_the_default_name() {
    assert_eq!(
        resolve(
            &["/git/dags/etl-repo", "/git/x/etl-repo"],
            &[("etl2", "/git/x/etl-repo/")]
        ),
        Ok(vec![
            root("etl-repo", "/git/dags/etl-repo"),
            root("etl2", "/git/x/etl-repo")
        ])
    );
}

#[test]
fn rejects_repeated_entries() {
    assert_eq!(
        resolve(&["/git/a", "/git/./a/"], &[]),
        Err(RootError::DuplicateEntry {
            list: "SCHEDR_DAG_PATH",
            path: "/git/a".into()
        })
    );
}

#[rstest]
#[case(&["/git", "/git/dags"])]
#[case(&["/git/dags", "/git"])]
fn rejects_nested_entries_in_either_order(#[case] paths: &[&str]) {
    assert_eq!(
        resolve(paths, &[]),
        Err(RootError::Nested {
            list: "SCHEDR_DAG_PATH",
            outer: "/git".into(),
            inner: "/git/dags".into()
        })
    );
}

#[test]
fn sibling_with_a_common_prefix_is_not_nested() {
    assert!(resolve(&["/git/dag", "/git/dags"], &[]).is_ok());
}

#[test]
fn rejects_duplicate_names() {
    assert_eq!(
        resolve(&["/a/repo", "/b/repo"], &[]),
        Err(RootError::DuplicateName {
            list: "SCHEDR_DAG_PATH",
            section: "dags",
            name: "repo".into(),
            first: "/a/repo".into(),
            second: "/b/repo".into(),
        })
    );
}

#[test]
fn rejects_a_rename_to_an_existing_name() {
    assert!(matches!(
        resolve(&["/a/one", "/b/two"], &[("one", "/b/two")]),
        Err(RootError::DuplicateName { name, .. }) if name == "one"
    ));
}

#[test]
fn rejects_renames_of_unlisted_directories() {
    assert_eq!(
        resolve(&["/a/one"], &[("x", "/elsewhere")]),
        Err(RootError::RenameNotListed {
            list: "SCHEDR_DAG_PATH",
            section: "dags",
            name: "x".into(),
            path: "/elsewhere".into(),
        })
    );
}

#[test]
fn rejects_unnamed_entries_unless_renamed() {
    assert_eq!(
        resolve(&["/"], &[]),
        Err(RootError::Unnamed {
            list: "SCHEDR_DAG_PATH",
            section: "dags",
            path: "/".into()
        })
    );
    assert_eq!(resolve(&["/"], &[("all", "/")]), Ok(vec![root("all", "/")]));
}

#[test]
fn empty_list_gives_no_roots() {
    assert_eq!(resolve(&[], &[]), Ok(vec![]));
}

#[test]
fn error_messages_explain_the_fix() {
    assert_eq!(
        RootError::Nested {
            list: "SCHEDR_DAG_PATH",
            outer: "/a".into(),
            inner: "/a/b".into()
        }
        .to_string(),
        "SCHEDR_DAG_PATH: `/a/b` is inside `/a`; nested entries aren't allowed"
    );
    assert_eq!(
        RootError::DuplicateName {
            list: "SCHEDR_DAG_PATH",
            section: "dags",
            name: "r".into(),
            first: "/a/r".into(),
            second: "/b/r".into(),
        }
        .to_string(),
        "SCHEDR_DAG_PATH: `/a/r` and `/b/r` are both named `r`; rename one in [dags.roots]"
    );
    assert_eq!(
        RootError::RenameNotListed {
            list: "SCHEDR_DAG_PATH",
            section: "dags",
            name: "x".into(),
            path: "/e".into(),
        }
        .to_string(),
        "[dags.roots] `x` = `/e` is not an entry of SCHEDR_DAG_PATH; renaming never adds directories"
    );
    assert_eq!(
        RootError::DuplicateEntry {
            list: "SCHEDR_DAG_PATH",
            path: "/a".into()
        }
        .to_string(),
        "SCHEDR_DAG_PATH: `/a` is listed twice"
    );
    assert_eq!(
        RootError::Unnamed {
            list: "SCHEDR_DAG_PATH",
            section: "dags",
            path: "/".into()
        }
        .to_string(),
        "SCHEDR_DAG_PATH: `/` has no name; rename it in [dags.roots]"
    );
}
