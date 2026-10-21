use std::collections::BTreeMap;

use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn run() -> RunVars {
    RunVars {
        dag_id: "sync".into(),
        run_id: "0192c4e1".into(),
        run_kind: "manual".into(),
        task_id: "load".into(),
        map_index: None,
        try_number: 2,
        planned_for: "2026-10-15T03:00:00Z".into(),
        since: "2026-10-14T03:00:00Z".into(),
    }
}

#[test]
fn run_variables_cover_the_run_information() {
    assert_eq!(
        run().variables(),
        map(&[
            ("SCHEDR_DAG_ID", "sync"),
            ("SCHEDR_PLANNED_FOR", "2026-10-15T03:00:00Z"),
            ("SCHEDR_RUN_ID", "0192c4e1"),
            ("SCHEDR_RUN_KIND", "manual"),
            ("SCHEDR_SINCE", "2026-10-14T03:00:00Z"),
            ("SCHEDR_TASK_ID", "load"),
            ("SCHEDR_TRY_NUMBER", "2"),
        ])
    );
}

#[test]
fn mapped_tasks_also_get_their_map_index() {
    let vars = RunVars {
        map_index: Some(3),
        ..run()
    }
    .variables();
    assert_eq!(vars["SCHEDR_MAP_INDEX"], "3");
}

#[rstest]
#[case("PATH", true)]
#[case("HOME", true)]
#[case("LANG", true)]
#[case("LC_ALL", true)]
#[case("LC_TIME", true)]
#[case("TZ", true)]
#[case("TMPDIR", true)]
#[case("SCHEDR_CONFIG__DATABASE__URL", false)]
#[case("VAULT_TOKEN", false)]
#[case("HTTP_PROXY", false)]
#[case("LCX", false)]
fn only_the_allow_list_passes_by_default(#[case] name: &str, #[case] allowed: bool) {
    assert_eq!(is_allowed(name), allowed);
}

#[test]
fn the_environment_is_clean_with_layers_in_order() {
    let agent = map(&[
        ("PATH", "/usr/bin"),
        ("HOME", "/home/schedr"),
        ("LC_ALL", "C.UTF-8"),
        ("HTTP_PROXY", "http://proxy:3128"),
        (
            "SCHEDR_CONFIG__DATABASE__URL",
            "postgres://u:secret@db/schedr",
        ),
        ("VAULT_TOKEN", "s.xyz"),
    ]);
    let task = map(&[("REPORT_MODE", "full"), ("PATH", "/opt/tool/bin")]);
    let env = task_environment(&agent, &["HTTP_PROXY".to_owned()], &task, &run()).unwrap();
    assert_eq!(env["PATH"], "/opt/tool/bin");
    assert_eq!(env["HOME"], "/home/schedr");
    assert_eq!(env["LC_ALL"], "C.UTF-8");
    assert_eq!(env["HTTP_PROXY"], "http://proxy:3128");
    assert_eq!(env["REPORT_MODE"], "full");
    assert_eq!(env["SCHEDR_TASK_ID"], "load");
    assert!(!env.contains_key("SCHEDR_CONFIG__DATABASE__URL"));
    assert!(!env.contains_key("VAULT_TOKEN"));
}

#[test]
fn pass_env_names_missing_from_the_agent_are_ignored() {
    let env = task_environment(&map(&[]), &["HTTP_PROXY".to_owned()], &map(&[]), &run()).unwrap();
    assert!(!env.contains_key("HTTP_PROXY"));
}

#[test]
fn a_task_cant_set_reserved_variables() {
    let err =
        task_environment(&map(&[]), &[], &map(&[("SCHEDR_RUN_ID", "x")]), &run()).unwrap_err();
    assert_eq!(err, EnvError::Reserved("SCHEDR_RUN_ID".into()));
    assert_eq!(
        err.to_string(),
        "environment variable `SCHEDR_RUN_ID` is reserved for schedr (names starting with SCHEDR_)"
    );
}
