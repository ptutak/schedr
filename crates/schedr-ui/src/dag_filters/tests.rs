use pretty_assertions::assert_eq;

use super::*;

#[test]
fn the_address_reads_repeated_boxes_spaces_and_escapes() {
    let asked = Asked::parse(Some(
        "view=&trigger=paused&name=+my%20dag+&tag=etl&tag=ml&tag=&kind=Run+of&kind=Run%20of&x=1&order=desc",
    ));
    assert_eq!(
        asked,
        Asked {
            view: Some(String::new()),
            trigger: Some("paused".to_owned()),
            name: "my dag".to_owned(),
            tags: BTreeSet::from(["etl".to_owned(), "ml".to_owned()]),
            kinds: BTreeSet::from(["Run of".to_owned()]),
            descending: true,
            page: 0,
        }
    );
    assert!(!Asked::parse(Some("order=asc")).descending);
    assert_eq!(Asked::parse(Some("page=3")).page, 3);
    // A malformed escape is left out; nothing asked is all.
    assert_eq!(
        Asked::parse(Some("tag=%zz&tag=ok")).tags,
        BTreeSet::from(["ok".to_owned()])
    );
    assert_eq!(Asked::parse(None), Asked::default());
    assert_eq!(TriggerFilter::parse("other"), TriggerFilter::All);
}

#[test]
fn the_filters_write_back_the_address_they_read() {
    let filters = Filters {
        view: Some("prod eu".to_owned()),
        trigger: TriggerFilter::Active,
        name: "a&b".to_owned(),
        tags: BTreeSet::from(["x y".to_owned()]),
        kinds: BTreeSet::from(["Run of".to_owned()]),
        descending: true,
        page: 3,
    };
    let query = filters.current();
    let asked = Asked::parse(Some(&query));
    assert_eq!(asked.view.as_deref(), Some("prod eu"));
    assert_eq!(asked.trigger.as_deref(), Some("active"));
    assert_eq!(asked.name, "a&b");
    assert_eq!(asked.tags, filters.tags);
    assert_eq!(asked.kinds, filters.kinds);
    assert!(asked.descending);
    assert_eq!(asked.page, 3, "the live update keeps the page");
    // The title's link: the same filters, the other way, from the first page.
    let other = Asked::parse(Some(&filters.reversed()));
    assert!(!other.descending);
    assert_eq!(other.page, 0);
    // Any other link (a tab, a switch) lists anew: no page.
    assert!(!filters.query(None, TriggerFilter::All).contains("page="));
    assert_eq!((other.name, other.tags), (asked.name, asked.tags));
    assert!(filters.is_filtered());
    let none = Filters {
        view: Some("prod".to_owned()),
        trigger: TriggerFilter::All,
        name: String::new(),
        tags: BTreeSet::new(),
        kinds: BTreeSet::new(),
        descending: true,
        page: 1,
    };
    assert!(!none.is_filtered(), "a deployment alone isn't a filter");
}

#[test]
fn encoded_keys_read_and_ids_stay_apart() {
    // Codex's review of D500: a key may come percent-encoded; each box's id is stable and
    // unique, so htmx gives the focus back after a swap.
    assert_eq!(
        Asked::parse(Some("%74ag=x&vi%65w=prod")),
        Asked {
            view: Some("prod".to_owned()),
            tags: BTreeSet::from(["x".to_owned()]),
            ..Asked::default()
        }
    );
    assert_eq!(element_id("dag-kind", "Run of"), "dag-kind-Run.32of");
    assert_ne!(element_id("dag-tag", "a b"), element_id("dag-tag", "a_b"));
    assert_ne!(element_id("dag-tag", "a.32b"), element_id("dag-tag", "a b"));
    assert_eq!(element_id("dag-trigger", ""), "dag-trigger-");
}
