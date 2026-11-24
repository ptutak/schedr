//! Numbered pages for a list (decision 517 as DN23 corrects it): `< 1 2 [3] 4 5 ... >`, from the
//! first page to two past the one shown as far as pages are known to exist, `...` where more
//! follow; no total and no last page, so nothing is counted. Past page 5 the numbers between the
//! first and two before the one shown give way to `...` (`< 1 ... 8 9 [10] 11 12 ... >`): a page
//! in the address, any number, draws six numbers at most (agy's review of M5.2j: page
//! 4,294,967,295 drew as many).

use schedr_api::views::Ahead;

/// A page's link among the numbers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageLink {
    /// Its number, from 1.
    pub number: u32,
    /// Its address.
    pub href: String,
    /// Whether it is the page shown.
    pub current: bool,
}

/// The numbers under a list, with Previous and Next.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Pager {
    /// The first page, when the numbers below don't start with it.
    pub first: Option<PageLink>,
    /// Whether pages are left out between `first` and `links` (a `...`).
    pub gap: bool,
    /// The pages from two before the one shown to two past it, as far as they exist.
    pub links: Vec<PageLink>,
    /// Whether more pages follow past the last number.
    pub more_past: bool,
    /// The page before, if any.
    pub previous: Option<String>,
    /// The page after, if any.
    pub next: Option<String>,
}

impl Pager {
    /// The numbers for `page` (from 1) with `ahead` past it, each page's address `href(n)`.
    /// None when there is a single page: nothing to number.
    pub fn of(page: u32, ahead: Ahead, href: impl Fn(u32) -> String) -> Option<Self> {
        let page = page.max(1);
        let last = page.saturating_add(ahead.pages_ahead.min(2));
        if last == 1 {
            return None;
        }
        let link = |number| PageLink {
            number,
            href: href(number),
            current: number == page,
        };
        // The numbers from 1 when that is short (up to page 5), else from two before.
        let from = if page <= 5 { 1 } else { page - 2 };
        let next = page.checked_add(1).filter(|_| ahead.more);
        Some(Self {
            first: (from > 1).then(|| link(1)),
            gap: from > 2,
            links: (from..=last).map(link).collect(),
            more_past: ahead.more_past && last < u32::MAX,
            previous: (page > 1).then(|| href(page - 1)),
            next: next.map(&href),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ahead(pages_ahead: u32, more_past: bool) -> Ahead {
        Ahead {
            more: pages_ahead > 0,
            pages_ahead,
            more_past,
        }
    }

    fn numbers(pager: &Pager) -> Vec<(u32, bool)> {
        pager
            .first
            .iter()
            .chain(&pager.links)
            .map(|l| (l.number, l.current))
            .collect()
    }

    /// agy's review of M5.2j: a page from the address, any number, draws a few numbers, not as
    /// many as it says; the last `u32` has no next.
    #[test]
    fn a_deep_page_draws_six_numbers_at_most() {
        let href = |n: u32| format!("/runs?page={n}");
        let deep = Pager::of(1000, ahead(2, true), href).unwrap();
        assert_eq!(
            numbers(&deep),
            [
                (1, false),
                (998, false),
                (999, false),
                (1000, true),
                (1001, false),
                (1002, false)
            ]
        );
        assert!(deep.gap);
        let sixth = Pager::of(6, ahead(0, false), href).unwrap();
        assert_eq!(
            numbers(&sixth),
            [(1, false), (4, false), (5, false), (6, true)]
        );
        assert!(sixth.gap, "2 and 3 left out");
        let fifth = Pager::of(5, ahead(0, false), href).unwrap();
        assert_eq!(numbers(&fifth).len(), 5, "1 to 5, nothing left out");
        assert!(!fifth.gap);
        let top = Pager::of(u32::MAX, ahead(2, true), href).unwrap();
        assert_eq!(numbers(&top).len(), 4);
        assert_eq!(
            (top.next, top.more_past),
            (None, false),
            "no page past the last number"
        );
    }

    #[test]
    fn pages_run_from_the_first_to_two_past_the_one_shown_as_far_as_they_exist() {
        let href = |n: u32| format!("/runs?page={n}");
        assert_eq!(
            Pager::of(1, ahead(0, false), href),
            None,
            "one page: no numbers"
        );
        let third = Pager::of(3, ahead(2, true), href).unwrap();
        assert_eq!(
            numbers(&third),
            [(1, false), (2, false), (3, true), (4, false), (5, false)]
        );
        assert!(third.more_past, "`...`: more past 5");
        assert_eq!(third.previous.as_deref(), Some("/runs?page=2"));
        assert_eq!(third.next.as_deref(), Some("/runs?page=4"));
        let last = Pager::of(4, ahead(0, false), href).unwrap();
        assert_eq!(numbers(&last).last(), Some(&(4, true)));
        assert_eq!(
            (last.next, last.more_past),
            (None, false),
            "the last page: no Next"
        );
        let first = Pager::of(1, ahead(1, false), href).unwrap();
        assert_eq!(first.previous, None);
        assert_eq!(numbers(&first), [(1, true), (2, false)]);
    }
}
