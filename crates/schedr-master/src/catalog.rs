//! The DAG list worked out from the current parse of each root.
//!
//! Given the current parse of every root and the stored DAGs, [`plan`] decides what changes
//! (see PROJECT.md, DAG code delivery and DAG ID conflicts):
//!
//! - a DAG defined in **one place** gets the parse's version as its current one; a **new**
//!   DAG is stored paused, and so is a removed DAG that **comes back**;
//! - a DAG ID defined in **more than one place** (two files, two roots) is in **conflict**
//!   and can't run;
//! - a DAG whose file **failed to import** (or holds an invalid DAG) keeps its last good
//!   version: a broken file doesn't remove its DAGs;
//! - a DAG of a reported root that its current parse no longer defines is **removed**.

use std::collections::{BTreeMap, BTreeSet};

use schedr_db::dags::{DagChange, StoredDag};
use schedr_model::runner::ParsedFile;
use schedr_model::{Dag, DagId, DagLocation};

#[cfg(test)]
mod tests;

/// The current parse of a root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    /// The root's name.
    pub root: String,
    /// The parse's ID.
    pub parse_id: String,
    /// Its files (after validation).
    pub files: Vec<ParsedFile>,
}

/// One place defining a DAG ID.
struct Definition<'a> {
    place: DagLocation,
    /// The new definition and its parse; `None` for a file that failed, keeping what is stored.
    new: Option<(&'a Dag, &'a str)>,
}

/// The changes that bring the stored DAGs in line with the sources.
#[must_use]
pub fn plan(sources: &[Source], stored: &[StoredDag]) -> Vec<DagChange> {
    let by_id: BTreeMap<&DagId, &StoredDag> = stored.iter().map(|d| (&d.dag_id, d)).collect();
    let roots: BTreeSet<&str> = sources.iter().map(|s| s.root.as_str()).collect();
    let definitions = definitions(sources, stored);
    let mut changes = Vec::new();
    for (dag_id, defs) in &definitions {
        let known = by_id.get(dag_id).copied();
        let comes_back = known.is_none_or(|d| d.removed);
        if defs.len() > 1 {
            let mut places: Vec<DagLocation> = defs.iter().map(|d| d.place.clone()).collect();
            places.sort();
            if comes_back || known.and_then(|d| d.conflict.as_ref()) != Some(&places) {
                changes.push(DagChange::Conflict {
                    dag_id: (*dag_id).clone(),
                    places,
                    pause: comes_back,
                });
            }
            continue;
        }
        let new = match (defs[0].new, known.and_then(|d| d.current.as_ref())) {
            (Some((dag, parse_id)), _) => Some((dag.clone(), parse_id.to_owned())),
            // The file failed: keep the stored version, only clearing a conflict.
            (None, Some(current)) if known.is_some_and(|d| d.conflict.is_some()) => {
                Some((current.dag.clone(), current.parse_id.clone()))
            }
            (None, _) => None,
        };
        let Some((dag, parse_id)) = new else {
            continue;
        };
        let unchanged = known.is_some_and(|d| {
            !d.removed
                && d.conflict.is_none()
                && d.current.as_ref().is_some_and(|c| c.parse_id == parse_id)
        });
        if !unchanged {
            changes.push(DagChange::Define {
                dag: Box::new(dag),
                parse_id,
                pause: known.is_some_and(|d| d.removed),
            });
        }
    }
    for dag in stored.iter().filter(|d| !d.removed) {
        if !definitions.contains_key(&dag.dag_id)
            && places_of(dag).any(|p| roots.contains(p.root.as_str()))
        {
            changes.push(DagChange::Remove {
                dag_id: dag.dag_id.clone(),
            });
        }
    }
    changes
}

/// Every place defining each DAG ID: the DAGs of the sources' files, and the stored places of
/// files that failed (they keep what was there).
fn definitions<'a>(
    sources: &'a [Source],
    stored: &'a [StoredDag],
) -> BTreeMap<&'a DagId, Vec<Definition<'a>>> {
    let mut found: BTreeMap<&DagId, Vec<Definition<'a>>> = BTreeMap::new();
    let mut failed: BTreeSet<(&str, &str)> = BTreeSet::new();
    for source in sources {
        for file in &source.files {
            if file.error.is_some() {
                failed.insert((&source.root, &file.path));
                continue;
            }
            for dag in &file.dags {
                found.entry(&dag.id).or_default().push(Definition {
                    place: dag.location.clone(),
                    new: Some((dag, &source.parse_id)),
                });
            }
        }
    }
    for dag in stored.iter().filter(|d| !d.removed) {
        let kept: BTreeSet<&DagLocation> = places_of(dag)
            .filter(|p| failed.contains(&(p.root.as_str(), p.path.as_str())))
            .collect();
        for place in kept {
            found.entry(&dag.dag_id).or_default().push(Definition {
                place: place.clone(),
                new: None,
            });
        }
    }
    found
}

/// The places a stored DAG is known at: its current version's, and its conflict's.
fn places_of(dag: &StoredDag) -> impl Iterator<Item = &DagLocation> {
    dag.current
        .iter()
        .map(|c| &c.dag.location)
        .chain(dag.conflict.iter().flatten())
}
