use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;

use super::*;

fn reports() -> AttemptReports {
    let mask = Arc::new(MaskSet::new(4));
    mask.add("token", "s3cret-value");
    AttemptReports::new(mask)
}

#[test]
fn a_status_is_kept_masked_bounded_and_its_progress_clamped() {
    let reports = reports();
    reports
        .status("pulling", Some("with s3cret-value"), Some(1.7))
        .unwrap();
    reports
        .status(&"p".repeat(900), None, Some(f64::NAN))
        .unwrap();
    let kept = reports.statuses();
    assert_eq!(kept[0].phase, "pulling");
    assert!(
        !kept[0].message.as_deref().unwrap().contains("s3cret"),
        "{kept:?}"
    );
    assert_eq!(kept[0].progress, Some(1.0));
    assert_eq!(kept[1].phase.chars().count(), MAX_TEXT);
    assert_eq!(kept[1].progress, None);
    assert_eq!(
        reports.status(" ", None, None),
        Err(Refused::Empty("phase"))
    );
}

#[test]
fn past_its_limit_the_first_status_and_the_latest_stay() {
    let reports = reports();
    for n in 0..=MAX_STATUSES {
        reports.status(&format!("phase {n}"), None, None).unwrap();
    }
    let kept = reports.statuses();
    assert_eq!(kept.len(), MAX_STATUSES);
    assert_eq!(kept[0].phase, "phase 0");
    assert_eq!(kept[1].phase, "phase 2");
    assert_eq!(kept.last().unwrap().phase, format!("phase {MAX_STATUSES}"));
}

#[test]
fn a_link_is_http_only_replaced_by_its_label_and_bounded() {
    let reports = reports();
    reports
        .link("Pod", "https://dash/pods/a?t=s3cret-value")
        .unwrap();
    reports.link("Pod", "https://dash/pods/b").unwrap();
    assert_eq!(
        reports.links(),
        [Link {
            label: "Pod".to_owned(),
            url: "https://dash/pods/b".to_owned(),
        }]
    );
    assert_eq!(reports.link("x", "javascript:alert(1)"), Err(Refused::Url));
    assert_eq!(reports.link("", "https://x"), Err(Refused::Empty("label")));
    for n in 1..MAX_LINKS {
        reports.link(&format!("l{n}"), "https://x").unwrap();
    }
    assert_eq!(
        reports.link("one more", "https://x"),
        Err(Refused::TooManyLinks)
    );
    reports.link("Pod", "https://dash/pods/c").unwrap();
}

#[test]
fn a_link_carrying_a_secret_is_masked() {
    let reports = reports();
    reports
        .link("Job", "https://dash/?token=s3cret-value")
        .unwrap();
    assert!(!reports.links()[0].url.contains("s3cret"));
}

#[test]
fn each_kept_report_is_told_to_the_sink_once() {
    let told = Arc::new(Mutex::new(Vec::new()));
    let sink = {
        let told = Arc::clone(&told);
        ReportSink(Arc::new(move |report: &Report| {
            told.lock().unwrap().push(report.clone());
        }))
    };
    let reports = reports().forwarded(sink);
    reports.status("running", None, None).unwrap();
    let _ = reports.link("bad", "ftp://x");
    reports.link("Pod", "https://x").unwrap();
    let told = told.lock().unwrap();
    assert_eq!(told.len(), 2, "{told:?}");
    assert!(matches!(told[0], Report::Status(_)));
    assert!(matches!(told[1], Report::Link(_)));
}

#[tokio::test(start_paused = true)]
async fn a_stop_reason_is_told_as_the_protocol_names_it_and_a_memory_kill_not_at_all() {
    use schedr_model::extension::StopKind;

    use crate::status::StopReason;
    let reports = reports();
    assert_eq!(reports.stop_reason(), (None, std::time::Duration::ZERO));
    let in_ten = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    for (reason, told) in [
        (StopReason::Cancelled, Some(StopKind::Cancel)),
        (StopReason::TimedOut, Some(StopKind::Timeout)),
        (StopReason::Interrupted, Some(StopKind::Drain)),
        (StopReason::CutOff, Some(StopKind::MasterLost)),
    ] {
        reports.stop_note().set(reason, in_ten);
        assert_eq!(
            reports.stop_reason(),
            (told, std::time::Duration::from_secs(10))
        );
    }
    reports.stop_note().set(StopReason::OomKilled, in_ten);
    assert_eq!(reports.stop_reason(), (None, std::time::Duration::ZERO));
}

#[test]
fn past_the_rate_the_newest_status_waits_and_is_told_at_the_end() {
    // Codex's review of M5.2i5: every status went on to the master, however fast.
    let told = Arc::new(Mutex::new(Vec::new()));
    let sink = {
        let told = Arc::clone(&told);
        ReportSink(Arc::new(move |report: &Report| {
            told.lock().unwrap().push(report.clone());
        }))
    };
    let reports = reports().forwarded(sink);
    for n in 0..10_000 {
        reports.status(&format!("p{n}"), None, None).unwrap();
    }
    let phases = |told: &[Report]| -> Vec<String> {
        told.iter()
            .filter_map(|r| match r {
                Report::Status(s) => Some(s.phase.clone()),
                Report::Link(_) => None,
            })
            .collect()
    };
    let sent = phases(&told.lock().unwrap());
    // The burst, give or take what a slow test machine refills meanwhile.
    assert!(sent.len() < 100, "{}", sent.len());
    assert_eq!(sent[0], "p0");
    reports.flush();
    let sent = phases(&told.lock().unwrap());
    assert_eq!(sent.last().unwrap(), "p9999", "the newest, at the end");
    reports.flush();
    assert_eq!(phases(&told.lock().unwrap()).len(), sent.len(), "once");
}

#[test]
fn the_rate_is_the_workers_hidden_limit() {
    // D493: the rate is `SCHEDR_HIDDEN__WORKER__EXTENSION_REPORTS_*`, not a constant.
    let told = Arc::new(Mutex::new(0_usize));
    let sink = {
        let told = Arc::clone(&told);
        ReportSink(Arc::new(move |report: &Report| {
            if matches!(report, Report::Status(_)) {
                *told.lock().unwrap() += 1;
            }
        }))
    };
    let reports = reports().forwarded(sink).limited(1, 3);
    for n in 0..1000 {
        reports.status(&format!("p{n}"), None, None).unwrap();
    }
    // A burst of three, give or take one refilled meanwhile on a slow machine.
    let sent = *told.lock().unwrap();
    assert!((3..=4).contains(&sent), "{sent}");
}

#[test]
fn a_plugins_labels_declarations_and_the_events_they_make_are_masked() {
    // agy's review of M5.2i5: a secret the attempt read reached the figures and the events.
    let figures = crate::figures::Figures::default();
    let figured_by = |figures: &crate::figures::Figures| Figured {
        figures: figures.clone(),
        extension: "k".to_owned(),
        version: "1".to_owned(),
        configured: schedr_model::extension::Declarations::default(),
        max_series: 100,
    };
    let reports = reports().figured(figured_by(&figures));
    let declarations: schedr_model::extension::Declarations =
        serde_json::from_value(serde_json::json!({
            "metrics": [{"name": "jobs", "kind": "counter", "help": "about s3cret-value",
                         "labels": [{"name": "pod"}]}],
            "panels": [{"title": "Jobs s3cret-value", "metric": "jobs", "chart": "rate",
                        "row": 1, "cell": "full"}]
        }))
        .unwrap();
    reports.declared(crate::channel::Declared {
        declarations,
        dropped: Some("panel `bad s3cret-value`: invalid".to_owned()),
    });
    reports.metrics(&[
        schedr_model::extension::MetricPoint {
            name: "jobs".to_owned(),
            labels: [("pod".to_owned(), "pod-s3cret-value".to_owned())].into(),
            value: 1.0,
        },
        schedr_model::extension::MetricPoint {
            name: "bad_s3cret-value".to_owned(),
            labels: std::collections::BTreeMap::new(),
            value: 1.0,
        },
    ]);
    reports.refused("hello said s3cret-value");
    let sent = serde_json::to_string(&figures.take_figures()).unwrap();
    let events = serde_json::to_string(&figures.take_events()).unwrap();
    assert!(sent.contains("\"jobs\""), "{sent}");
    assert!(!sent.contains("s3cret"), "{sent}");
    assert!(events.contains("dropped"), "{events}");
    assert!(!events.contains("s3cret"), "{events}");
    assert!(!format!("{:?}", reports.declarations()).contains("s3cret"));
}
