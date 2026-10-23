//! Path lists (`SCHEDR_DAG_PATH`, `SCHEDR_SCRIPT_PATH`) and the roots made from them.
//!
//! Every entry of a path list is a root, named after its last path component unless renamed
//! (see PROJECT.md, DAG roots and Plain script delivery: script roots). Paths are compared
//! lexically: the configuration is checked without touching the filesystem.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Component, Path, PathBuf};

use serde::de::{self, Deserializer, SeqAccess, Visitor};

#[cfg(test)]
mod tests;

/// A named root: one entry of a path list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Root {
    /// The root's name, used in references such as `etl-repo:team_a/sync.py`.
    pub name: String,
    /// The directory.
    pub path: PathBuf,
}

/// Why a path list and its renames don't make a valid set of roots.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RootError {
    /// The same directory is listed twice.
    #[error("{list}: `{}` is listed twice", path.display())]
    DuplicateEntry {
        /// The path list's name, e.g. `SCHEDR_DAG_PATH`.
        list: &'static str,
        /// The repeated directory.
        path: PathBuf,
    },
    /// One entry is inside another.
    #[error("{list}: `{}` is inside `{}`; nested entries aren't allowed", inner.display(), outer.display())]
    Nested {
        /// The path list's name.
        list: &'static str,
        /// The containing entry.
        outer: PathBuf,
        /// The entry inside it.
        inner: PathBuf,
    },
    /// An entry has no last component to name it after (e.g. `/`).
    #[error("{list}: `{}` has no name; rename it in [{section}.roots]", path.display())]
    Unnamed {
        /// The path list's name.
        list: &'static str,
        /// The configuration section, `dags` or `scripts`.
        section: &'static str,
        /// The entry.
        path: PathBuf,
    },
    /// Two entries have the same name.
    #[error(
        "{list}: `{}` and `{}` are both named `{name}`; rename one in [{section}.roots]",
        first.display(),
        second.display()
    )]
    DuplicateName {
        /// The path list's name.
        list: &'static str,
        /// The configuration section.
        section: &'static str,
        /// The shared name.
        name: String,
        /// The first entry with it.
        first: PathBuf,
        /// The second entry with it.
        second: PathBuf,
    },
    /// A rename names a directory that isn't in the path list.
    #[error(
        "[{section}.roots] `{name}` = `{}` is not an entry of {list}; renaming never adds directories",
        path.display()
    )]
    RenameNotListed {
        /// The path list's name.
        list: &'static str,
        /// The configuration section.
        section: &'static str,
        /// The new name.
        name: String,
        /// The directory that isn't listed.
        path: PathBuf,
    },
}

/// A list of directories, written as an array in TOML or as a `PATH`-style colon-separated
/// string (`/git/dags/a:/git/dags/b`) in environment variables.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize)]
pub struct PathList(pub Vec<PathBuf>);

impl schemars::JsonSchema for PathList {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "PathList".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "anyOf": [
                {"type": "array", "items": {"type": "string"}},
                {"type": "string"}
            ],
            "description": "Directories: an array, or a colon-separated string as in `PATH`."
        })
    }
}

impl PathList {
    /// Splits a `PATH`-style string; empty entries (`a::b`, a trailing `:`) are skipped.
    #[must_use]
    pub fn from_path_string(s: &str) -> Self {
        Self(
            s.split(':')
                .filter(|p| !p.is_empty())
                .map(PathBuf::from)
                .collect(),
        )
    }
}

impl<'de> serde::Deserialize<'de> for PathList {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct PathListVisitor;

        impl<'de> Visitor<'de> for PathListVisitor {
            type Value = PathList;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a list of paths, or a colon-separated string of paths")
            }

            fn visit_str<E: de::Error>(self, s: &str) -> Result<PathList, E> {
                Ok(PathList::from_path_string(s))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<PathList, A::Error> {
                let mut paths = Vec::new();
                while let Some(path) = seq.next_element::<String>()? {
                    paths.push(PathBuf::from(path));
                }
                Ok(PathList(paths))
            }
        }

        d.deserialize_any(PathListVisitor)
    }
}

/// `path` without `.` components or a trailing separator, for lexical comparison.
#[must_use]
pub fn normalize(path: &Path) -> PathBuf {
    path.components()
        .filter(|c| !matches!(c, Component::CurDir))
        .collect()
}

/// Builds the roots of a path list: checks that no entry is repeated or nested in another,
/// names each entry (its last component, or its rename), and checks that names are unique.
///
/// `list` names the path list and `section` its configuration section, for error messages.
///
/// # Errors
///
/// Returns the first [`RootError`] found, in the order: repeated entries, nested entries,
/// renames of unlisted directories, unnamed entries, duplicate names.
pub fn resolve_roots(
    list: &'static str,
    section: &'static str,
    entries: &PathList,
    renames: &BTreeMap<String, PathBuf>,
) -> Result<Vec<Root>, RootError> {
    let paths: Vec<PathBuf> = entries.0.iter().map(|p| normalize(p)).collect();
    for (i, a) in paths.iter().enumerate() {
        for b in &paths[i + 1..] {
            if a == b {
                return Err(RootError::DuplicateEntry {
                    list,
                    path: a.clone(),
                });
            }
            if b.starts_with(a) || a.starts_with(b) {
                let (outer, inner) = if b.starts_with(a) { (a, b) } else { (b, a) };
                return Err(RootError::Nested {
                    list,
                    outer: outer.clone(),
                    inner: inner.clone(),
                });
            }
        }
    }
    let mut names_by_path: BTreeMap<PathBuf, &str> = BTreeMap::new();
    for (name, path) in renames {
        let path = normalize(path);
        if !paths.contains(&path) {
            return Err(RootError::RenameNotListed {
                list,
                section,
                name: name.clone(),
                path,
            });
        }
        names_by_path.insert(path, name);
    }
    let mut roots: Vec<Root> = Vec::with_capacity(paths.len());
    for path in paths {
        let name = match names_by_path.get(&path) {
            Some(name) => (*name).to_owned(),
            None => match path.file_name() {
                Some(name) => name.to_string_lossy().into_owned(),
                None => {
                    return Err(RootError::Unnamed {
                        list,
                        section,
                        path,
                    });
                }
            },
        };
        if let Some(first) = roots.iter().find(|r| r.name == name) {
            return Err(RootError::DuplicateName {
                list,
                section,
                name,
                first: first.path.clone(),
                second: path,
            });
        }
        roots.push(Root { name, path });
    }
    Ok(roots)
}
