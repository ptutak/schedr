use pretty_assertions::assert_eq;

use super::*;

const fn doc(kind: MetricKind) -> MetricDoc {
    MetricDoc {
        name: "runs",
        kind,
        labels: &[],
        help: "Runs",
    }
}

#[test]
fn the_exposed_name_has_the_prefix_and_the_types_suffix() {
    assert_eq!(
        doc(MetricKind::Counter).exposed(Some("schedr")),
        "schedr_runs_total"
    );
    assert_eq!(
        doc(MetricKind::Gauge).exposed(Some("schedr")),
        "schedr_runs"
    );
    assert_eq!(doc(MetricKind::Histogram).exposed(None), "runs");
    assert_eq!(doc(MetricKind::Info).exposed(None), "runs_info");
}

#[test]
fn kinds_read_as_in_a_scrape() {
    let kinds = [
        MetricKind::Counter,
        MetricKind::Gauge,
        MetricKind::Histogram,
        MetricKind::Info,
    ];
    let names: Vec<String> = kinds.iter().map(ToString::to_string).collect();
    assert_eq!(names, ["counter", "gauge", "histogram", "info"]);
}

const RUNS: MetricDoc = MetricDoc {
    name: "runs",
    kind: MetricKind::Gauge,
    labels: &["state", "kind"],
    help: "Runs by state and kind",
};

const SCRAPE: &str = "\
# HELP schedr_runs Runs by state and kind.
# TYPE schedr_runs gauge
schedr_runs{state=\"running\",kind=\"manual\"} 1
# HELP schedr_tick Tick duration.
# TYPE schedr_tick histogram
schedr_tick_sum 0.5
schedr_tick_count 1
schedr_tick_bucket{le=\"0.001\"} 0
# EOF
";

const TICK: MetricDoc = MetricDoc {
    name: "tick",
    kind: MetricKind::Histogram,
    labels: &[],
    help: "Tick duration",
};

#[test]
fn a_scrape_that_matches_its_docs_has_no_mismatches() {
    assert_eq!(
        scrape_mismatches(SCRAPE, Some("schedr"), &[RUNS, TICK]),
        Vec::<String>::new()
    );
}

#[test]
fn every_kind_of_mismatch_is_named() {
    let other = MetricDoc {
        labels: &["state"],
        help: "Runs",
        ..RUNS
    };
    let missing = MetricDoc {
        name: "workers",
        ..RUNS
    };
    assert_eq!(
        scrape_mismatches(SCRAPE, Some("schedr"), &[other, missing]),
        [
            "`schedr_tick` is not documented",
            "`schedr_runs` has other help text",
            "`schedr_runs` has the labels [\"state\", \"kind\"]",
            "`schedr_workers` isn't in the scrape as a gauge",
        ]
    );
}

#[test]
fn names_without_a_prefix_are_compared_as_they_are() {
    let scrape = "# HELP up Up.\n# TYPE up gauge\nup 1\n";
    let up = MetricDoc {
        name: "up",
        kind: MetricKind::Gauge,
        labels: &[],
        help: "Up",
    };
    assert_eq!(scrape_mismatches(scrape, None, &[up]), Vec::<String>::new());
}
