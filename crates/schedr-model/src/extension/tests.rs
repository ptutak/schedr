use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn declarations(value: serde_json::Value) -> Declarations {
    serde_json::from_value(value).unwrap()
}

fn kubernetes() -> serde_json::Value {
    json!({
        "metrics": [
            {"name": "queue_seconds", "kind": "histogram", "help": "Job created to pod running",
             "unit": "seconds", "buckets": [1, 5, 30, 120, 600]},
            {"name": "pods_running", "kind": "gauge", "help": "Pods running now"},
            {"name": "pods_started", "kind": "counter", "help": "Pods started",
             "labels": [{"name": "phase", "values": ["Succeeded", "Failed"]}, {"name": "namespace"}]}
        ],
        "panels": [
            {"title": "Queue time", "metric": "queue_seconds", "chart": "histogram",
             "row": 1, "cell": "full"},
            {"title": "Queue time p50 / p95", "metric": "queue_seconds", "chart": "quantiles",
             "quantiles": [0.5, 0.95], "row": 2, "cell": "left"},
            {"title": "Running", "metric": "pods_running", "chart": "line", "row": 2, "cell": "center"},
            {"title": "Started", "metric": "pods_started", "chart": "rate", "row": 2, "cell": "right"}
        ]
    })
}

#[test]
fn the_kubernetes_plugins_declarations_are_accepted() {
    let declared = declarations(kubernetes());
    assert_eq!(check_declarations(&declared), Ok(()));
    assert_eq!(declared.panels[0].cell, Cell::Full);
    assert_eq!(declared.panels[2].row, 2);
    assert_eq!(declared.metrics[1].unit, MetricUnit::Count);
}

#[test]
fn a_declaration_breaking_a_rule_is_refused_with_why() {
    let cases: Vec<(serde_json::Value, &str)> = vec![
        (
            json!({"metrics": [{"name": "Queue", "kind": "gauge", "help": "x"}]}),
            "lowercase",
        ),
        (
            json!({"metrics": [{"name": "q", "kind": "gauge", "help": " "}]}),
            "help",
        ),
        (
            json!({"metrics": [{"name": "q", "kind": "histogram", "help": "x"}]}),
            "buckets",
        ),
        (
            json!({"metrics": [{"name": "q", "kind": "histogram", "help": "x", "buckets": [5, 1]}]}),
            "increasing",
        ),
        (
            json!({"metrics": [{"name": "q", "kind": "gauge", "help": "x", "buckets": [1]}]}),
            "only a histogram",
        ),
        (
            json!({"metrics": [{"name": "q", "kind": "gauge", "help": "x"}, {"name": "q", "kind": "counter", "help": "y"}]}),
            "twice",
        ),
        (
            json!({"metrics": [{"name": "q", "kind": "gauge", "help": "x", "labels": [{"name": "a"}, {"name": "a"}]}]}),
            "label `a` twice",
        ),
        (
            json!({"metrics": [{"name": "q", "kind": "gauge", "help": "x", "labels": [{"name": "a", "values": []}]}]}),
            "at least one",
        ),
        (
            json!({"metrics": [{"name": "q", "kind": "gauge", "help": "x"}],
                "panels": [{"title": "t", "metric": "missing", "chart": "line", "row": 1, "cell": "full"}]}),
            "isn't declared",
        ),
        (
            json!({"metrics": [{"name": "q", "kind": "gauge", "help": "x"}],
                "panels": [{"title": "t", "metric": "q", "chart": "histogram", "row": 1, "cell": "full"}]}),
            "can't draw",
        ),
        (
            json!({"metrics": [{"name": "q", "kind": "counter", "help": "x"}],
                "panels": [{"title": "t", "metric": "q", "chart": "line", "row": 1, "cell": "full"}]}),
            "can't draw",
        ),
        (
            json!({"metrics": [{"name": "q", "kind": "histogram", "help": "x", "buckets": [1]}],
                "panels": [{"title": "t", "metric": "q", "chart": "quantiles", "quantiles": [1.5], "row": 1, "cell": "full"}]}),
            "between 0 and 1",
        ),
    ];
    for (value, why) in cases {
        let error = check_declarations(&declarations(value.clone())).unwrap_err();
        assert!(error.contains(why), "{value}: {error}");
    }
}

#[test]
fn an_unknown_field_is_refused_when_read() {
    let wrong = json!({"metrics": [{"name": "q", "kind": "gauge", "help": "x", "colour": "red"}]});
    assert!(
        serde_json::from_value::<Declarations>(wrong).is_err(),
        "styles are schedr's"
    );
}

#[test]
fn a_metric_no_panel_shows_gets_a_full_row_below_them() {
    let declared = declarations(kubernetes());
    let none = Declarations {
        panels: Vec::new(),
        ..declared.clone()
    };
    let places: Vec<(u32, Cell, ChartKind)> = page_panels(&none)
        .iter()
        .map(|p| (p.row, p.cell, p.chart))
        .collect();
    assert_eq!(
        places,
        [
            (1, Cell::Full, ChartKind::Quantiles),
            (2, Cell::Full, ChartKind::Line),
            (3, Cell::Full, ChartKind::Rate),
        ]
    );
    assert_eq!(
        check_declarations(&Declarations {
            panels: page_panels(&none),
            ..none
        }),
        Ok(())
    );
    let only_queue = Declarations {
        panels: declared.panels[..1].to_vec(),
        ..declared
    };
    let panels = page_panels(&only_queue);
    let shown: Vec<(Option<&str>, u32)> = panels
        .iter()
        .map(|p| (p.metric.as_deref(), p.row))
        .collect();
    assert_eq!(
        shown,
        [
            (Some("queue_seconds"), 1),
            (Some("pods_running"), 2),
            (Some("pods_started"), 3),
        ]
    );
}

/// M5.2i12: the built-in service metrics no panel names share one Service row below the rest,
/// memory a line (the largest worker's on "All workers"), the times quantiles; one a panel names
/// is drawn there instead. A plugin may not declare one of their names.
#[test]
fn the_builtin_service_metrics_get_a_service_row_and_their_names_are_schedrs() {
    let mut declared = declarations(kubernetes());
    declared.panels.truncate(1);
    declared.metrics.extend(
        BUILTIN_SERVICE_METRICS
            .iter()
            .filter_map(|name| builtin_service_metric(name)),
    );
    assert_eq!(check_declarations(&declared), Ok(()));
    let panels = page_panels(&declared);
    let service: Vec<(&str, u32, Cell, ChartKind, Option<Combine>)> = panels
        .iter()
        .filter(|p| {
            p.metric
                .as_deref()
                .is_some_and(|m| m.starts_with("service_"))
        })
        .map(|p| {
            (
                p.metric.as_deref().unwrap(),
                p.row,
                p.cell,
                p.chart,
                p.combine,
            )
        })
        .collect();
    assert_eq!(
        service,
        [
            (
                "service_memory_bytes",
                4,
                Cell::Left,
                ChartKind::Line,
                Some(Combine::Max)
            ),
            (
                "service_accept_seconds",
                4,
                Cell::Center,
                ChartKind::Quantiles,
                None
            ),
            (
                "service_call_seconds",
                4,
                Cell::Right,
                ChartKind::Quantiles,
                None
            ),
        ]
    );
    // Named by a panel: drawn there, the row the other two's.
    declared.panels.push(Panel {
        metric: Some("service_memory_bytes".to_owned()),
        chart: ChartKind::Number,
        ..declared.panels[0].clone()
    });
    let laid_out = page_panels(&declared);
    let cells: Vec<(&str, Cell)> = laid_out
        .iter()
        .filter(|p| {
            p.metric
                .as_deref()
                .is_some_and(|m| m.starts_with("service_"))
        })
        .map(|p| (p.metric.as_deref().unwrap(), p.cell))
        .collect();
    assert_eq!(
        cells,
        [
            ("service_memory_bytes", declared.panels[0].cell),
            ("service_accept_seconds", Cell::Left),
            ("service_call_seconds", Cell::Right),
        ]
    );
    let mut own = declarations(kubernetes());
    own.metrics.push(MetricDeclaration {
        name: "service_jobs".to_owned(),
        ..own.metrics[0].clone()
    });
    assert!(
        check_own_declarations(&own, &BUILTIN_SERVICE_METRICS)
            .unwrap_err()
            .contains("are schedr's")
    );
}

#[test]
fn only_http_links_are_shown() {
    assert!(link_url_allowed("https://dash.example/pods/batch/x"));
    assert!(link_url_allowed("HTTP://dash.example/"));
    for bad in [
        "javascript:alert(1)",
        " javascript:alert(1)",
        "data:text/html,x",
        "/relative",
        "ftp://x",
    ] {
        assert!(!link_url_allowed(bad), "{bad}");
    }
    assert!(!link_url_allowed(&format!(
        "https://{}",
        "a".repeat(MAX_TEXT)
    )));
}

#[test]
fn a_rows_cells_make_one_of_three_layouts() {
    assert_eq!(row_cells(&[Cell::Full]), Ok(1));
    assert_eq!(row_cells(&[Cell::Left, Cell::Right]), Ok(2));
    assert_eq!(row_cells(&[Cell::Right]), Ok(2));
    assert_eq!(row_cells(&[Cell::Left, Cell::Center, Cell::Right]), Ok(3));
    assert_eq!(row_cells(&[Cell::Center]), Ok(3));
    assert!(
        row_cells(&[Cell::Full, Cell::Left])
            .unwrap_err()
            .contains("alone")
    );
    assert!(
        row_cells(&[Cell::Left, Cell::Left])
            .unwrap_err()
            .contains("twice")
    );
    let metric = json!([{"name": "q", "kind": "gauge", "help": "Q"}]);
    for (panels, why) in [
        (
            json!([{"title": "a", "metric": "q", "chart": "line", "row": 0, "cell": "full"}]),
            "from the top",
        ),
        (
            json!([{"title": "a", "metric": "q", "chart": "line", "row": 1, "cell": "full"},
                {"title": "b", "metric": "q", "chart": "line", "row": 1, "cell": "left"}]),
            "row 1",
        ),
        (
            json!([{"title": "a", "metric": "q", "chart": "line", "row": 3, "cell": "left"},
                {"title": "b", "metric": "q", "chart": "line", "row": 3, "cell": "left"}]),
            "row 3",
        ),
    ] {
        let error = check_declarations(&declarations(json!({"metrics": metric, "panels": panels})))
            .unwrap_err();
        assert!(error.contains(why), "{error}");
    }
}

#[test]
fn an_events_panel_takes_a_cell_and_no_metric() {
    let metric = json!([{"name": "q", "kind": "gauge", "help": "Q"}]);
    let with = |panel: serde_json::Value| {
        check_declarations(&declarations(json!({"metrics": metric, "panels": [panel]})))
    };
    assert_eq!(
        with(json!({"title": "Events", "chart": "events", "row": 1, "cell": "full"})),
        Ok(())
    );
    let error =
        with(json!({"title": "E", "metric": "q", "chart": "events", "row": 1, "cell": "full"}));
    assert!(error.unwrap_err().contains("no metric"));
    let error = with(json!({"title": "L", "chart": "line", "row": 1, "cell": "full"}));
    assert!(error.unwrap_err().contains("which metric"));
}

/// M5.2i8: a plugin's notifications are named identifiers, each once, with a help; at most 50.
#[test]
fn a_notification_declaration_breaking_a_rule_is_refused_with_why() {
    let with = |notifications: serde_json::Value| {
        check_declarations(&declarations(json!({"notifications": notifications})))
    };
    assert_eq!(
        with(json!([{"name": "image_pull_failed", "help": "An image can't be pulled"}])),
        Ok(())
    );
    for (notifications, why) in [
        (json!([{"name": "Bad Name", "help": "h"}]), "notification"),
        (json!([{"name": "done", "help": ""}]), "`done`'s help"),
        (
            json!([{"name": "done", "help": "h"}, {"name": "done", "help": "h"}]),
            "declared twice",
        ),
    ] {
        let error = with(notifications).unwrap_err();
        assert!(error.contains(why), "{why:?} in {error}");
    }
    let many: Vec<_> = (0..=MAX_NOTIFICATIONS)
        .map(|i| json!({"name": format!("e{i}"), "help": "h"}))
        .collect();
    assert!(with(json!(many)).unwrap_err().contains("at most 50"));
}
