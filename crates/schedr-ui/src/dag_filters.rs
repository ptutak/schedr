//! The DAGs page's filters (the user, 2026-11-16): by name, by the trigger's switch, by tag and
//! by trigger kind. They are in the page's address, so a filtered list can be linked and its
//! live updates keep it; the switch is also remembered per viewer, as the deployment is.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use schedr_client::path_segment;

use crate::pages::DagRow;

/// Which DAGs the page shows by their trigger's switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerFilter {
    /// Every DAG.
    All,
    /// Those whose trigger is on.
    Active,
    /// Those whose trigger is switched off.
    Paused,
}

impl TriggerFilter {
    /// Each, in the page's order, with its label.
    pub const ALL: [(Self, &'static str); 3] = [
        (Self::All, "All"),
        (Self::Active, "Active"),
        (Self::Paused, "Paused"),
    ];

    /// The filter a name names: `active`, `paused`; anything else is all.
    #[must_use]
    pub fn parse(name: &str) -> Self {
        match name {
            "active" => Self::Active,
            "paused" => Self::Paused,
            _ => Self::All,
        }
    }

    /// Its name in the address and the cookie; empty for all.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::All => "",
            Self::Active => "active",
            Self::Paused => "paused",
        }
    }

    /// Whether `row` is shown: a DAG with no trigger (run by hand only) only under all.
    #[must_use]
    pub fn shows(self, row: &DagRow) -> bool {
        match self {
            Self::All => true,
            Self::Active => !row.paused && !row.manual_only(),
            Self::Paused => row.paused,
        }
    }
}

/// What the address asks for; each `None` or empty: not asked.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Asked {
    /// `view`: a deployment's name, or empty for all.
    pub view: Option<String>,
    /// `trigger`: `active`, `paused` or empty.
    pub trigger: Option<String>,
    /// `name`: a part of the DAG's ID, any case.
    pub name: String,
    /// `tag`, repeated: any of them.
    pub tags: BTreeSet<String>,
    /// `kind`, repeated: any of these trigger kinds (`Schedule`, `Run of`, ...).
    pub kinds: BTreeSet<String>,
    /// `order=desc`: Z to A.
    pub descending: bool,
    /// `page`: the page, from 1 (decision 517); 0 (not asked, not a number): the first.
    pub page: u32,
}

impl Asked {
    /// The query string's (`a=1&tag=x&tag=y`) filters; `+` is a space, as forms send it.
    #[must_use]
    pub fn parse(query: Option<&str>) -> Self {
        let mut asked = Self::default();
        for pair in query
            .unwrap_or_default()
            .split('&')
            .filter(|p| !p.is_empty())
        {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let decoded = |text: &str| crate::page::percent_decoded(&text.replace('+', " "));
            // Keys too: a browser may send `%74ag` for `tag` (Codex's review of D500).
            let (Some(key), Some(value)) = (decoded(key), decoded(value)) else {
                continue;
            };
            match key.as_str() {
                "view" => asked.view = Some(value),
                "trigger" => asked.trigger = Some(value),
                "name" => value.trim().clone_into(&mut asked.name),
                "tag" if !value.is_empty() => {
                    asked.tags.insert(value);
                }
                "kind" if !value.is_empty() => {
                    asked.kinds.insert(value);
                }
                "order" => asked.descending = value == "desc",
                "page" => asked.page = value.parse().unwrap_or(0),
                _ => {}
            }
        }
        asked
    }
}

/// The filters in force: the address's, the switch and deployment from the cookies when not
/// asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filters {
    /// The deployment shown; none: all.
    pub view: Option<String>,
    /// By the trigger's switch.
    pub trigger: TriggerFilter,
    /// A part of the DAG's ID; empty: any.
    pub name: String,
    /// Any of these tags; none: any.
    pub tags: BTreeSet<String>,
    /// Any of these trigger kinds; none: any.
    pub kinds: BTreeSet<String>,
    /// Sorted Z to A by the DAG's ID (else A to Z).
    pub descending: bool,
    /// The page shown, from 1: kept by the live update and the numbers only, as any other
    /// link changes what is listed.
    pub page: u32,
}

impl Filters {
    /// The filters `asked` gives, showing `view`; the switch from `remembered` (its cookie)
    /// when not asked.
    #[must_use]
    pub fn of(asked: Asked, view: Option<String>, remembered: Option<String>) -> Self {
        Self {
            trigger: TriggerFilter::parse(&asked.trigger.or(remembered).unwrap_or_default()),
            view,
            name: asked.name,
            tags: asked.tags,
            kinds: asked.kinds,
            descending: asked.descending,
            page: asked.page.max(1),
        }
    }

    /// The query string giving these filters, with `view` and `trigger` as given instead.
    #[must_use]
    pub fn query(&self, view: Option<&str>, trigger: TriggerFilter) -> String {
        let mut query = format!(
            "view={}&trigger={}",
            path_segment(view.unwrap_or_default()),
            trigger.name()
        );
        if !self.name.is_empty() {
            query.push_str("&name=");
            query.push_str(&path_segment(&self.name));
        }
        for tag in &self.tags {
            query.push_str("&tag=");
            query.push_str(&path_segment(tag));
        }
        for kind in &self.kinds {
            query.push_str("&kind=");
            query.push_str(&path_segment(kind));
        }
        if self.descending {
            query.push_str("&order=desc");
        }
        query
    }

    /// The query string of these filters sorted the other way (the DAG title's link).
    #[must_use]
    pub fn reversed(&self) -> String {
        Self {
            descending: !self.descending,
            page: 1,
            ..self.clone()
        }
        .current()
    }

    /// The query string of these filters as they are, at their page.
    #[must_use]
    pub fn current(&self) -> String {
        self.at_page(self.page)
    }

    /// The query string of these filters at page `number` (the first: no `page`).
    #[must_use]
    pub fn at_page(&self, number: u32) -> String {
        let query = self.query(self.view.as_deref(), self.trigger);
        if number > 1 {
            format!("{query}&page={number}")
        } else {
            query
        }
    }

    /// Whether any filter but the deployment's narrows the list.
    #[must_use]
    pub fn is_filtered(&self) -> bool {
        self.trigger != TriggerFilter::All
            || !self.name.is_empty()
            || !self.tags.is_empty()
            || !self.kinds.is_empty()
    }

    /// Whether `row` passes the name, tag and kind filters (not the switch or deployment).
    #[must_use]
    pub fn matches(&self, row: &DagRow) -> bool {
        (self.name.is_empty()
            || row
                .dag_id
                .to_lowercase()
                .contains(&self.name.to_lowercase()))
            && (self.tags.is_empty() || row.tags.iter().any(|tag| self.tags.contains(tag)))
            && (self.kinds.is_empty() || self.kinds.contains(row.trigger_kind))
    }
}

/// A box of the page's filters: what it sends, its label, the DAGs it would show, whether
/// it is ticked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Choice {
    /// Its element's id, the same across the live updates: htmx gives focus back to it after
    /// a swap, so the keys keep moving through the boxes (Codex's review of D500).
    pub id: String,
    /// Its value in the address.
    pub value: String,
    /// What it says.
    pub label: String,
    /// The DAGs with it, among those the other filters show.
    pub count: usize,
    /// Whether it is chosen.
    pub checked: bool,
}

/// The switch's three choices, each counted among `rows` (the other filters applied).
#[must_use]
pub fn switch_choices(rows: &[&DagRow], trigger: TriggerFilter) -> Vec<Choice> {
    TriggerFilter::ALL
        .iter()
        .map(|&(filter, label)| Choice {
            id: element_id("dag-trigger", filter.name()),
            value: filter.name().to_owned(),
            label: label.to_owned(),
            count: rows.iter().filter(|row| filter.shows(row)).count(),
            checked: filter == trigger,
        })
        .collect()
}

/// A box for each value `of` gives the DAGs of `all` (the choices a deployment's DAGs offer),
/// counted among `shown`, and one for each chosen value no DAG has now (so it can be unticked),
/// in name order.
#[must_use]
pub fn boxes(
    group: &str,
    all: &[&DagRow],
    shown: &[&DagRow],
    chosen: &BTreeSet<String>,
    of: impl Fn(&DagRow) -> Vec<String>,
) -> Vec<Choice> {
    let mut counts: BTreeMap<String, usize> = all
        .iter()
        .flat_map(|row| of(row))
        .map(|value| (value, 0))
        .collect();
    for value in chosen {
        counts.entry(value.clone()).or_insert(0);
    }
    for row in shown {
        for value in of(row).into_iter().collect::<BTreeSet<_>>() {
            *counts.entry(value).or_insert(0) += 1;
        }
    }
    counts
        .into_iter()
        .map(|(value, count)| Choice {
            id: element_id(group, &value),
            checked: chosen.contains(&value),
            label: value.clone(),
            value,
            count,
        })
        .collect()
}

/// An element id for `value` in `group`: its letters, digits, `-` and `_` kept, each other
/// character as `.` and its code point, so two values never share one (`Run of` is
/// `dag-kind-Run.32of`).
fn element_id(group: &str, value: &str) -> String {
    let mut id = format!("{group}-");
    for c in value.chars() {
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
            id.push(c);
        } else {
            let _ = write!(id, ".{}", u32::from(c));
        }
    }
    id
}

/// The page's choices for `here` (the DAGs of the deployment shown): the switch's, the kinds'
/// and the tags' boxes, each counted among the DAGs every other group lets through.
#[must_use]
pub fn choices(here: &[&DagRow], filters: &Filters) -> (Vec<Choice>, Vec<Choice>, Vec<Choice>) {
    // The DAGs passing every filter but the left-out groups: the switch, the tags, the kinds.
    let without = |switch: bool, tags: bool, kinds: bool| {
        let only = Filters {
            tags: if tags {
                BTreeSet::new()
            } else {
                filters.tags.clone()
            },
            kinds: if kinds {
                BTreeSet::new()
            } else {
                filters.kinds.clone()
            },
            ..filters.clone()
        };
        here.iter()
            .copied()
            .filter(|row| only.matches(row) && (switch || filters.trigger.shows(row)))
            .collect::<Vec<&DagRow>>()
    };
    (
        switch_choices(&without(true, false, false), filters.trigger),
        boxes(
            "dag-kind",
            here,
            &without(false, false, true),
            &filters.kinds,
            |row| vec![row.trigger_kind.to_owned()],
        ),
        boxes(
            "dag-tag",
            here,
            &without(false, true, false),
            &filters.tags,
            |row| row.tags.clone(),
        ),
    )
}

#[cfg(test)]
mod tests;
