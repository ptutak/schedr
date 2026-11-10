use super::*;

const FILES: [&str; 3] = ["team_a/reports/daily.py", "team_a/run.sh", "top.py"];

fn exists(path: &str) -> bool {
    FILES.contains(&path)
}

fn read_at(path: &str, text: &str) -> Result<Vec<Declaration>, Vec<String>> {
    let source = Source {
        root: "etl",
        path,
        exists: &exists,
    };
    read(text, &source)
}

fn read_team(text: &str) -> Result<Vec<Declaration>, Vec<String>> {
    read_at("team_a/schedr.toml", text)
}

/// The one problem `text` has.
fn problem(text: &str) -> String {
    let problems = read_team(text).expect_err("a problem");
    assert_eq!(problems.len(), 1, "{problems:?}");
    problems.into_iter().next().unwrap()
}

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

#[test]
fn a_scheduled_job_is_a_one_task_dag_named_by_its_reference() {
    let text = r#"
        [jobs.report_daily]
        script = "reports/daily.py"
        interpreter = "python"
        args = ["--region", "eu"]
        env = { MODE = "full", DB_PASSWORD = { secret = "etl/db#password" } }
        secrets = ["etl/api#token", { prefix = "conn/", storage = "vault" }]
        schedule = "0 3 * * *"
        timezone = "Europe/Warsaw"
        start = 2026-01-01
        end = 2026-12-31T12:00:00Z
        catchup = true
        max_active_runs = 2
        queue = "reports"
        retries = 2
        retry_delay = "5m"
        timeout = "1h30m"
        max_output_size = "4MiB"
        description = "Daily report"
        tags = ["reports"]
    "#;
    let declared = read_team(text).unwrap();
    assert_eq!(declared.len(), 1);
    let job = &declared[0];
    assert_eq!(job.reference, "etl:team_a/schedr.toml#report_daily");
    let location = DagLocation {
        root: "etl".to_owned(),
        path: "team_a/schedr.toml".to_owned(),
    };
    let dag = job.dag(&location).expect("scheduled: a DAG");
    assert_eq!(dag.id.as_str(), "etl:team_a/schedr.toml#report_daily");
    assert_eq!(dag.location, location);
    assert_eq!(dag.description.as_deref(), Some("Daily report"));
    assert_eq!(dag.tags, ["reports"]);
    assert_eq!(dag.max_active_runs, Some(2));
    assert_eq!(
        dag.trigger,
        Trigger::Cron {
            expression: "0 3 * * *".to_owned(),
            timezone: "Europe/Warsaw".to_owned(),
            catchup: true,
            // Midnight in Warsaw, an hour before UTC's in winter.
            start: Some(ts("2025-12-31T23:00:00Z")),
            end: Some(ts("2026-12-31T12:00:00Z")),
        }
    );
    let [task] = &dag.tasks[..] else {
        panic!("one task")
    };
    assert_eq!(task.id.as_str(), "report_daily");
    let TaskKind::Script {
        job,
        script,
        interpreter,
        args,
        env,
        working_dir,
        ..
    } = &task.kind
    else {
        panic!("a script task")
    };
    assert_eq!(job, "etl:team_a/schedr.toml#report_daily");
    assert_eq!(script, "team_a/reports/daily.py");
    assert_eq!(interpreter, "python");
    assert_eq!(*args, [Template::text("--region"), Template::text("eu")]);
    assert_eq!(env["MODE"], Template::text("full"));
    assert_eq!(
        env["DB_PASSWORD"].references().collect::<Vec<_>>(),
        [&Reference::Secret(SecretName::new(
            "etl/db",
            Some("password")
        ))]
    );
    assert_eq!(working_dir, "team_a");
    assert_eq!(
        task.secrets,
        [
            SecretDecl::Secret(SecretName::new("etl/api", Some("token"))),
            SecretDecl::Prefix {
                storage: Some("vault".to_owned()),
                prefix: "conn/".to_owned()
            },
        ]
    );
    let settings = &task.settings;
    assert_eq!(
        (
            settings.retries,
            settings.retry_delay_ms,
            settings.timeout_ms,
            settings.queue.as_str(),
            settings.max_output_size
        ),
        (2, 300_000, Some(5_400_000), "reports", 4 << 20)
    );
    assert_eq!(task.output_type, TypeDesc::ProcessResult);
}

#[test]
fn a_job_without_a_schedule_is_a_script_for_dags_only() {
    let text = r#"
        [jobs.helper]
        script = "run.sh"
        interpreter = "bash"
    "#;
    let declared = read_team(text).unwrap();
    assert_eq!(declared[0].schedule, None);
    let location = DagLocation {
        root: "etl".to_owned(),
        path: "team_a/schedr.toml".to_owned(),
    };
    assert_eq!(declared[0].dag(&location), None);
    let settings = &declared[0].task.settings;
    assert_eq!(settings.queue, "default");
}

#[test]
fn the_same_script_may_be_declared_twice_and_the_order_is_kept() {
    let text = r#"
        [jobs.b_hourly]
        script = "run.sh"
        interpreter = "bash"
        every = "1h"

        [jobs.a_once]
        script = "run.sh"
        interpreter = "bash"
        once = 2026-12-24T23:00:00
        timezone = "Europe/Warsaw"
    "#;
    let declared = read_team(text).unwrap();
    let names: Vec<&str> = declared.iter().map(|d| d.name.as_str()).collect();
    // toml keeps a table's keys sorted.
    assert_eq!(names, ["a_once", "b_hourly"]);
    let triggers: Vec<&Trigger> = declared
        .iter()
        .map(|d| &d.schedule.as_ref().unwrap().trigger)
        .collect();
    assert_eq!(
        *triggers[0],
        Trigger::Once {
            at: ts("2026-12-24T22:00:00Z")
        }
    );
    assert_eq!(
        *triggers[1],
        Trigger::Every {
            interval_ms: 3_600_000,
            catchup: false,
            start: None,
            end: None
        }
    );
}

#[test]
fn a_file_at_the_root_and_paths_with_dots_resolve_in_the_root() {
    let text = r#"
        [jobs.top]
        script = "./top.py"
        interpreter = "python"
        working_dir = "."
    "#;
    let declared = read_at("schedr.toml", text).unwrap();
    let TaskKind::Script {
        script,
        working_dir,
        ..
    } = &declared[0].task.kind
    else {
        panic!("a script task")
    };
    assert_eq!((script.as_str(), working_dir.as_str()), ("top.py", ""));
}

#[test]
fn an_empty_file_declares_nothing() {
    assert_eq!(read_team(""), Ok(Vec::new()));
}

#[test]
fn bad_toml_is_one_problem() {
    assert!(problem("[jobs.x\n").starts_with("not valid TOML"));
}

#[test]
fn unknown_keys_are_errors_at_the_top_and_in_a_job() {
    let problems = read_team(
        r#"
        title = "x"
        [jobs.x]
        script = "run.sh"
        interpreter = "bash"
        sript = "run.sh"
    "#,
    )
    .unwrap_err();
    assert_eq!(
        problems,
        [
            "unknown key `title`: a `schedr.toml` has `[jobs.<name>]` tables",
            "job `x`: unknown key `sript`"
        ]
    );
}

#[test]
fn notify_is_refused_pointing_to_a_dag_s() {
    let text = "[jobs.x]\nscript = \"run.sh\"\ninterpreter = \"bash\"\nschedule = \"@daily\"\n\
                notify = [\"ops\"]\n";
    assert_eq!(
        problem(text),
        "job `x`: `notify` isn't set on a job: notifications are a DAG's, `Dag(notify=...)` in \
         a DAG file"
    );
}

/// A scheduled job in `team_a` with `rest` after its keys.
fn job_with(rest: &str) -> String {
    format!("[jobs.x]\nscript = \"run.sh\"\ninterpreter = \"bash\"\nschedule = \"@daily\"\n{rest}")
}

#[test]
fn declared_outcomes_are_the_tasks_exit_codes_and_the_operators() {
    let declared = read_team(&job_with("[jobs.x.outcomes]\nSUCCESS = 0\nNO_DATA = 3\n")).unwrap();
    let names: Vec<_> = declared[0]
        .task
        .outcomes
        .iter()
        .map(|o| (o.name.as_str(), o.exit_code))
        .collect();
    assert_eq!(names, [("NO_DATA", Some(3)), ("SUCCESS", Some(0))]);
    assert_eq!(
        declared[0].for_operator().outcomes,
        declared[0].task.outcomes
    );
}

#[test]
fn outcomes_need_names_and_distinct_exit_codes() {
    for (outcomes, expected) in [
        ("A = 256\n", "`outcomes`: `A` is an exit code, 0 to 255"),
        ("A = -1\n", "`outcomes`: `A` is an exit code, 0 to 255"),
        ("A = \"0\"\n", "`outcomes`: `A` is an exit code, 0 to 255"),
        (
            "A = 1\nB = 1\n",
            "`outcomes`: `B` and `A` share the exit code 1",
        ),
        (
            "\"a b\" = 1\n",
            "`outcomes`: `a b` isn't a name: letters, digits and `_`, not starting with a digit",
        ),
        ("", "`outcomes` is at least one name and its exit code"),
    ] {
        assert_eq!(
            problem(&job_with(&format!("[jobs.x.outcomes]\n{outcomes}"))),
            format!("job `x`: {expected}"),
        );
    }
}

#[test]
fn params_take_their_types_from_their_defaults() {
    let text = job_with(
        "[jobs.x.params]\nregion = \"eu\"\nlimit = 100\nratio = 0.5\nfull = false\n\
         day = 2026-01-02\nat = 2026-01-02T03:04:05\nnames = [\"a\", \"b\"]\n\
         [jobs.x.params.nested]\nsize = 8\n",
    )
    .replace(
        "schedule = \"@daily\"",
        "schedule = \"@daily\"\ntimezone = \"Europe/Warsaw\"",
    );
    let declared = read_team(&text).unwrap();
    let location = DagLocation {
        root: "etl".to_owned(),
        path: "team_a/schedr.toml".to_owned(),
    };
    let params = declared[0].dag(&location).unwrap().params.unwrap();
    let TypeDesc::Dataclass { fields, .. } = &params.ty else {
        panic!("a dataclass: {:?}", params.ty)
    };
    let of = |name: &str| &fields.iter().find(|f| f.name == name).unwrap().ty;
    assert_eq!(of("region"), &TypeDesc::Str);
    assert_eq!(of("limit"), &TypeDesc::Int);
    assert_eq!(of("ratio"), &TypeDesc::Float);
    assert_eq!(of("full"), &TypeDesc::Bool);
    assert_eq!(of("day"), &TypeDesc::Date);
    assert_eq!(of("at"), &TypeDesc::Datetime);
    assert_eq!(
        of("names"),
        &TypeDesc::List {
            item: Box::new(TypeDesc::Str)
        }
    );
    assert!(
        matches!(of("nested"), TypeDesc::TypedDict { .. }),
        "{:?}",
        of("nested")
    );
    assert!(
        fields.iter().all(|f| !f.required),
        "every field has its default"
    );
    assert_eq!(params.defaults["region"], "eu");
    assert_eq!(params.defaults["day"], "2026-01-02");
    // Without an offset, in the job's time zone (+01:00 in January).
    assert_eq!(params.defaults["at"], "2026-01-02T02:04:05Z");
    assert_eq!(params.defaults["nested"]["size"], 8);
    let properties = &params.schema["properties"];
    assert_eq!(properties["day"]["format"], "date");
    assert_eq!(properties["limit"]["default"], 100);
    assert_eq!(properties["names"]["items"]["type"], "string");
    assert_eq!(params.schema["additionalProperties"], false);
}

#[test]
fn a_param_whose_type_cant_be_told_is_refused() {
    for (default, why) in [
        (
            "[]",
            "an empty list has no type to take: give a default with an item",
        ),
        ("[1, \"a\"]", "a list mixing types has no type to take"),
        ("[1, 1.5]", "a list mixing types has no type to take"),
        ("{}", "an empty table has no type to take"),
        ("nan", "a number that isn't finite can't be stored"),
        ("03:04:05", "`03:04:05` has no date"),
    ] {
        assert_eq!(
            problem(&job_with(&format!("[jobs.x.params]\nthe = {default}\n"))),
            format!("job `x`: `params`: `the`: {why}"),
        );
    }
    assert_eq!(
        problem(&job_with("[jobs.x.params]\n\"a-b\" = 1\n")),
        "job `x`: `params`: `a-b` isn't a name: letters, digits and `_`, not starting with a digit",
    );
}

#[test]
fn a_cleanup_is_a_script_of_the_same_root_with_its_settings() {
    let text = job_with(
        "cleanup_handles = [\"Job\"]\n[jobs.x.cleanup]\nscript = \"reports/daily.py\"\n\
         interpreter = \"python\"\nargs = [\"--all\"]\ntimeout = \"5m\"\nretries = 1\n\
         retry_delay = \"10s\"\nanywhere = true\n",
    );
    let declared = read_team(&text).unwrap();
    let cleanup = declared[0].task.cleanup.as_ref().unwrap();
    assert_eq!(
        cleanup.run,
        CleanupRun::Script {
            script: "team_a/reports/daily.py".to_owned(),
            interpreter: "python".to_owned(),
            args: vec![Template::text("--all".to_owned())],
        }
    );
    assert_eq!(cleanup.timeout_ms, Some(300_000));
    assert_eq!(cleanup.retries, 1);
    assert_eq!(cleanup.retry_delay_ms, 10_000);
    assert_eq!(cleanup.handles.as_deref(), Some(&["Job".to_owned()][..]));
    assert!(cleanup.anywhere);
    let given = declared[0].for_operator().cleanup.unwrap();
    assert_eq!(given.retries, Some(1));
    assert_eq!(given.handles, cleanup.handles);
}

#[test]
fn a_cleanup_without_settings_has_a_cleanups_defaults_and_the_operator_none() {
    let text = job_with("[jobs.x.cleanup]\nscript = \"run.sh\"\ninterpreter = \"bash\"\n");
    let declared = read_team(&text).unwrap();
    let cleanup = declared[0].task.cleanup.as_ref().unwrap();
    assert_eq!(cleanup.retries, DEFAULT_CLEANUP_RETRIES);
    assert_eq!(cleanup.retry_delay_ms, DEFAULT_CLEANUP_RETRY_DELAY_MS);
    assert_eq!(cleanup.handles, None);
    assert!(!cleanup.anywhere);
    // The DAG's defaults apply to a `ScriptOperator`'s.
    assert_eq!(declared[0].for_operator().cleanup.unwrap().retries, None);
}

#[test]
fn a_cleanup_follows_the_rules_of_a_jobs_script() {
    for (cleanup, expected) in [
        (
            "interpreter = \"bash\"\n",
            "`cleanup`: `script` is required",
        ),
        (
            "script = \"run.sh\"\n",
            "`cleanup`: `interpreter` is required",
        ),
        (
            "script = \"gone.sh\"\ninterpreter = \"bash\"\n",
            "`cleanup`: `script`: `gone.sh` isn't a file in the script root",
        ),
        (
            "script = \"run.sh\"\ninterpreter = \"bash\"\nenv = {}\n",
            "`cleanup`: unknown key `env`",
        ),
    ] {
        assert_eq!(
            problem(&job_with(&format!("[jobs.x.cleanup]\n{cleanup}"))),
            format!("job `x`: {expected}"),
        );
    }
    assert!(
        read_team(&job_with(
            "[jobs.x.cleanup]\nscript = \"../top.py\"\ninterpreter = \"bash\"\n"
        ))
        .is_err()
    );
    assert_eq!(
        problem(&job_with("cleanup_handles = [\"Job\"]\n")),
        "job `x`: `cleanup_handles` without `[cleanup]`: nothing would read them",
    );
}

#[test]
fn a_cleanups_unknown_interpreter_is_warned_of_as_the_jobs_is() {
    let text = job_with("[jobs.x.cleanup]\nscript = \"run.sh\"\ninterpreter = \"other\"\n");
    let declared = read_team(&text).unwrap();
    let warnings = unknown_interpreters(&declared, &["bash".to_owned()]);
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(
        warnings[0].contains("`cleanup.interpreter`: `other`"),
        "{warnings:?}"
    );
}

#[test]
fn params_on_a_script_for_dags_only_would_do_nothing() {
    let text = "[jobs.x]\nscript = \"run.sh\"\ninterpreter = \"bash\"\n[jobs.x.params]\na = 1\n";
    assert!(
        problem(text).starts_with("job `x`: `params`: a script without"),
        "{text}"
    );
}

#[test]
fn a_job_name_is_letters_digits_underscores_and_dashes() {
    let text = "[jobs.\"a.b\"]\nscript = \"run.sh\"\ninterpreter = \"bash\"\n";
    assert!(problem(text).starts_with("job `a.b`: a job's name is letters"));
}

#[test]
fn the_script_and_interpreter_are_required_and_checked() {
    let problems = read_team("[jobs.x]\nretries = 1\n").unwrap_err();
    assert_eq!(
        problems,
        [
            "job `x`: `script` is required",
            "job `x`: `interpreter` is required"
        ]
    );
    let missing = "[jobs.x]\nscript = \"nope.sh\"\ninterpreter = \"bash\"\n";
    assert_eq!(
        problem(missing),
        "job `x`: `script`: `nope.sh` isn't a file in the script root"
    );
    // Checked by the worker running it (rejection 3), not here: workers may differ.
    let unknown = "[jobs.x]\nscript = \"run.sh\"\ninterpreter = \"ruby\"\n";
    assert!(read_team(unknown).is_ok());
}

#[test]
fn a_path_never_leaves_the_files_directory() {
    let up = "[jobs.x]\nscript = \"../top.py\"\ninterpreter = \"python\"\n";
    assert!(problem(up).contains("has `..`"));
    let absolute = "[jobs.x]\nscript = \"/etc/passwd\"\ninterpreter = \"python\"\n";
    assert!(problem(absolute).contains("is absolute"));
    let dir = "[jobs.x]\nscript = \"run.sh\"\ninterpreter = \"bash\"\nworking_dir = \"a/../..\"\n";
    assert!(problem(dir).contains("`working_dir`"));
}

#[test]
fn at_most_one_schedule() {
    let text = "[jobs.x]\nscript = \"run.sh\"\ninterpreter = \"bash\"\nschedule = \"@daily\"\nevery = \"1h\"\n";
    assert_eq!(
        problem(text),
        "job `x`: at most one of `schedule`, `every` and `once` (it has `schedule`, `every`)"
    );
}

#[test]
fn dag_level_keys_without_a_schedule_are_errors() {
    let text = "[jobs.x]\nscript = \"run.sh\"\ninterpreter = \"bash\"\ncatchup = true\n";
    assert!(problem(text).starts_with("job `x`: `catchup`: a script without"));
}

#[test]
fn once_takes_no_start_end_or_catchup() {
    let text = "[jobs.x]\nscript = \"run.sh\"\ninterpreter = \"bash\"\nonce = 2026-01-01\ncatchup = true\n";
    assert!(problem(text).contains("`once` fires once"));
}

#[test]
fn bad_values_name_their_keys() {
    let cases = [
        ("retries = -1", "`retries` is a whole number of at least 0"),
        (
            "max_active_runs = 0",
            "`max_active_runs` is a whole number of at least 1",
        ),
        ("retry_delay = \"5 minutes\"", "`retry_delay`:"),
        ("max_output_size = \"lots\"", "`max_output_size`:"),
        (
            "timezone = \"Mars/Olympus\"",
            "`Mars/Olympus` isn't a known time zone",
        ),
        ("start = 10:00:00", "has no date"),
        ("start = \"2026-01-01\"", "`start` is a TOML date"),
        ("catchup = \"yes\"", "`catchup` is `true` or `false`"),
        ("tags = [1]", "`tags` is a list of strings"),
        ("every = \"0s\"", "`every` is longer than nothing"),
    ];
    for (line, wanted) in cases {
        let schedule = if line.starts_with("every") {
            ""
        } else {
            "schedule = \"@daily\"\n"
        };
        let text =
            format!("[jobs.x]\nscript = \"run.sh\"\ninterpreter = \"bash\"\n{schedule}{line}\n");
        let problems = read_team(&text).unwrap_err();
        assert!(
            problems.iter().any(|p| p.contains(wanted)),
            "{line}: {problems:?}"
        );
    }
}

#[test]
fn env_and_secrets_are_checked() {
    let cases = [
        ("env = { \"1X\" = \"a\" }", "isn't a variable name"),
        ("env = { X = 1 }", "is a string or"),
        (
            "env = { X = { secret = \"etl/db\" } }",
            "a variable takes one key",
        ),
        (
            "env = { X = { secret = \"etl/db#k\", vault = \"v\" } }",
            "unknown key `vault`",
        ),
        ("secrets = [1]", "an entry is"),
        (
            "secrets = [{ prefix = \"conn\" }]",
            "a prefix ends with `/`",
        ),
        ("secrets = [\"etl//db#k\"]", "`secrets`:"),
    ];
    for (line, wanted) in cases {
        let text = format!("[jobs.x]\nscript = \"run.sh\"\ninterpreter = \"bash\"\n{line}\n");
        let problems = read_team(&text).unwrap_err();
        assert!(
            problems.iter().any(|p| p.contains(wanted)),
            "{line}: {problems:?}"
        );
    }
}

#[test]
fn an_id_past_the_dag_id_limit_is_an_error() {
    let long = "a".repeat(240);
    let text = format!(
        "[jobs.{long}]\nscript = \"run.sh\"\ninterpreter = \"bash\"\nschedule = \"@daily\"\n"
    );
    assert!(problem(&text).contains("isn't a valid DAG ID"));
}

#[test]
fn the_problems_are_one_message_a_line_each() {
    assert_eq!(message(&["a".to_owned(), "b".to_owned()]), "a\nb");
}

#[test]
fn a_name_too_long_for_a_task_id_is_a_problem_scheduled_or_not() {
    let name = "j".repeat(251);
    let unscheduled = format!("[jobs.{name}]\nscript = \"run.sh\"\ninterpreter = \"bash\"\n");
    assert!(
        problem(&unscheduled).contains("its name names the job's task"),
        "{}",
        problem(&unscheduled)
    );
}

#[test]
fn a_local_check_warns_of_an_interpreter_it_lacks_and_the_worker_doesnt() {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("team_a")).unwrap();
    std::fs::write(dir.path().join("team_a/run.sh"), "echo hi\n").unwrap();
    std::fs::write(
        dir.path().join("team_a/schedr.toml"),
        "[jobs.x]\nscript = \"run.sh\"\ninterpreter = \"ruby\"\n",
    )
    .unwrap();
    let files = ["team_a/run.sh".to_owned(), "team_a/schedr.toml".to_owned()];
    let wanted = ["team_a/schedr.toml".to_owned()];
    let known = ["bash".to_owned()];
    let checked = read_root("etl", dir.path(), (&files, &wanted), Some(&known));
    assert_eq!(checked.len(), 1);
    assert_eq!(checked[0].error, None);
    assert_eq!(
        checked[0].warnings,
        [
            "job `x`: `interpreter`: `ruby` isn't an `[interpreters]` entry here (a worker \
          without it rejects the job's tasks)"
        ]
    );
    let on_a_worker = read_root("etl", dir.path(), (&files, &wanted), None);
    assert_eq!(on_a_worker[0].warnings, Vec::<String>::new());
}

#[test]
fn a_reference_cant_leave_its_script_root() {
    let dir = tempfile::tempdir().unwrap();
    let copy = dir.path().join("jobs");
    std::fs::create_dir_all(&copy).unwrap();
    std::fs::create_dir_all(dir.path().join("outside")).unwrap();
    std::fs::write(dir.path().join("outside/payload.py"), "").unwrap();
    std::fs::write(
        dir.path().join("outside/schedr.toml"),
        "[jobs.escape]\nscript = \"payload.py\"\ninterpreter = \"py\"\n",
    )
    .unwrap();
    let scripts = BTreeMap::from([(
        "jobs".to_owned(),
        ScriptRoot {
            configured: copy.clone(),
            copy: copy.clone(),
        },
    )]);
    let absolute = format!("{}/../outside/schedr.toml#escape", copy.display());
    for reference in ["jobs:../outside/schedr.toml#escape", absolute.as_str()] {
        let (root, found) = lookup(reference, &scripts);
        assert_eq!(root.as_deref(), Some("jobs"), "{reference}");
        let why = found.expect_err(reference);
        assert!(why.contains("no `..`"), "{why}");
    }
    assert!(stays_inside("team/./schedr.toml"));
    assert!(!stays_inside("/etc/schedr.toml"));
    assert!(!stays_inside(""));
}
