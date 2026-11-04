use pretty_assertions::assert_eq;
use serde_json::{Value as Json, json};

use super::*;

const MASTER: SchedrVersion = SchedrVersion::new(0, 1);

/// The master's message limit in these tests: the default, 64 MiB.
const LIMIT: usize = 64 << 20;

fn task(id: &str, arguments: &Json) -> Json {
    json!({
        "id": id, "kind": "python", "arguments": arguments,
        "settings": {
            "retries": 0, "retry_delay_ms": 300_000, "timeout_ms": null,
            "queue": "default", "max_output_size": 1_048_576
        },
        "output_type": {"type": "int"}
    })
}

fn branch(id: &str) -> Json {
    let mut branch = task(id, &json!({}));
    branch["branch"] = json!(true);
    branch["output_type"] = json!({"type": "list", "item": {"type": "str"}});
    branch
}

fn output_of(task: &str) -> Json {
    json!({"ref": {"kind": "output", "task": task}})
}

fn edge(from: &str, to: &str, kind: &str) -> Json {
    json!({"from": from, "to": to, "kind": kind})
}

fn dag_with(changes: &Json) -> Dag {
    let mut dag = json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "sync.py"},
        "trigger": {"kind": "none"}, "initial_since": null,
        "max_active_runs": 1,
        "tasks": [task("extract", &json!({})), task("load", &json!({"rows": output_of("extract")}))],
        "edges": [edge("extract", "load", "data")]
    });
    for (key, value) in changes.as_object().unwrap() {
        dag[key] = value.clone();
    }
    serde_json::from_value(dag).unwrap()
}

fn problems(changes: &Json) -> Vec<String> {
    dag_problems(&dag_with(changes), "etl", "sync.py", MASTER)
}

#[test]
fn a_valid_dag_has_no_problems() {
    assert_eq!(problems(&json!({})), Vec::<String>::new());
}

#[test]
fn dag_json_from_a_newer_or_other_major_version_cant_be_read() {
    assert_eq!(
        problems(&json!({"produced_by": "0.2"})),
        ["produced by schedr 0.2, which this master (schedr 0.1) can't read"]
    );
    assert_eq!(problems(&json!({"produced_by": "1.0"})).len(), 1);
    let older = dag_with(&json!({"produced_by": "0.1"}));
    assert_eq!(
        dag_problems(&older, "etl", "sync.py", SchedrVersion::new(0, 3)),
        Vec::<String>::new()
    );
}

#[test]
fn the_location_must_be_the_file_parsed() {
    assert_eq!(
        problems(&json!({"location": {"root": "etl", "path": "other.py"}})),
        ["located at etl:other.py in its DAG JSON, but parsed from etl:sync.py"]
    );
}

/// A daily trigger running from `start` to `end`.
fn daily(start: &str, end: &str) -> Json {
    json!({"kind": "cron", "expression": "@daily", "timezone": "UTC", "catchup": false,
           "start": start, "end": end})
}

#[test]
fn run_limits_and_the_window_are_checked() {
    assert_eq!(
        problems(&json!({"max_active_runs": 0})),
        ["max_active_runs must be at least 1"]
    );
    assert_eq!(
        problems(&json!({"max_active_tasks": 0})),
        ["max_active_tasks must be at least 1"]
    );
    let mut limited = task("extract", &json!({}));
    limited["settings"]["max_active"] = json!(0);
    limited["settings"]["max_active_per_run"] = json!(0);
    assert_eq!(
        problems(
            &json!({"tasks": [limited, task("load", &json!({"rows": output_of("extract")}))]})
        ),
        [
            "task `extract`: max_active must be at least 1",
            "task `extract`: max_active_per_run must be at least 1"
        ]
    );
    // Set, they read and pass.
    assert_eq!(
        problems(&json!({"max_active_tasks": 3})),
        Vec::<String>::new()
    );
    assert_eq!(
        problems(&json!({"trigger": daily("2026-02-01T00:00:00Z", "2026-01-01T00:00:00Z")})),
        ["start (2026-02-01T00:00:00Z) is after end (2026-01-01T00:00:00Z)"]
    );
    assert_eq!(
        problems(&json!({"trigger": daily("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")})),
        Vec::<String>::new()
    );
}

#[test]
fn the_trigger_must_be_usable() {
    assert_eq!(
        problems(
            &json!({"trigger": {"kind": "cron", "expression": "0 3 * * *", "timezone": "Mars/Base", "catchup": false}})
        ),
        ["trigger: unknown time zone `Mars/Base`"]
    );
    assert_eq!(
        problems(&json!({"trigger": {"kind": "every", "interval_ms": 0, "catchup": false}})),
        ["trigger: the interval of an Every trigger must be positive"]
    );
}

#[test]
fn task_ids_are_unique() {
    assert_eq!(
        problems(&json!({"tasks": [task("a", &json!({})), task("a", &json!({}))], "edges": []})),
        ["task `a` is defined more than once"]
    );
}

#[test]
fn edges_join_tasks_of_the_dag() {
    assert_eq!(
        problems(
            &json!({"edges": [edge("extract", "load", "data"), edge("extract", "ghost", "order")]})
        ),
        ["edge `extract` -> `ghost`: no task `ghost`"]
    );
    assert_eq!(
        problems(
            &json!({"edges": [edge("extract", "load", "data"), edge("load", "load", "order")]})
        ),
        [
            "task `load` depends on itself",
            "the tasks `load` depend on each other in a cycle"
        ]
    );
}

#[test]
fn selection_edges_come_only_from_branches_and_only_selection_edges_from_them() {
    assert_eq!(
        problems(
            &json!({"edges": [edge("extract", "load", "data"), edge("extract", "load", "selection")]})
        ),
        ["edge `extract` -> `load`: a selection edge, but `extract` isn't a `@branch` task"]
    );
    let tasks = json!([branch("pick"), task("a", &json!({})), task("b", &json!({}))]);
    assert_eq!(
        problems(
            &json!({"tasks": tasks, "edges": [edge("pick", "a", "selection"), edge("pick", "b", "order")]})
        ),
        [
            "edge `pick` -> `b`: of kind `order`, but every edge from the `@branch` task `pick` must be a selection edge (or an order edge out of its group, decision 94)"
        ]
    );
    // A dependency on a group that ends with the branch (decision 94): an order edge out of
    // the group, not one inside it, and never a data edge.
    let mut pick = branch("route.pick");
    pick["group"] = json!("route");
    let mut inside = task("route.audit", &json!({}));
    inside["group"] = json!("route");
    let tasks = json!([pick, inside, task("then", &json!({}))]);
    let groups = json!([{"id": "route", "parent": null}]);
    let edges = json!([
        edge("route.pick", "then", "order"),
        edge("route.pick", "route.audit", "order"),
    ]);
    assert_eq!(
        problems(&json!({"tasks": tasks, "edges": edges, "groups": groups})),
        [
            "edge `route.pick` -> `route.audit`: of kind `order`, but every edge from the `@branch` task `route.pick` must be a selection edge (or an order edge out of its group, decision 94)"
        ]
    );
}

#[test]
fn cycles_name_the_tasks_left_over() {
    let tasks = json!([
        task("a", &json!({})),
        task("b", &json!({})),
        task("c", &json!({})),
        task("d", &json!({}))
    ]);
    let edges = json!([
        edge("a", "b", "order"),
        edge("b", "c", "order"),
        edge("c", "b", "order"),
        edge("c", "d", "order")
    ]);
    assert_eq!(
        problems(&json!({"tasks": tasks, "edges": edges})),
        ["the tasks `b`, `c`, `d` depend on each other in a cycle"]
    );
}

#[test]
fn outputs_are_used_only_along_data_edges() {
    assert_eq!(
        problems(&json!({"edges": [edge("extract", "load", "order")]})),
        ["task `load` uses the output of `extract` without a data edge from it"]
    );
    // A branch's targets read what it chose along their selection edges.
    let tasks = json!([
        branch("extract"),
        task("load", &json!({"rows": output_of("extract")}))
    ]);
    assert_eq!(
        problems(&json!({"tasks": tasks, "edges": [edge("extract", "load", "selection")]})),
        Vec::<String>::new()
    );
    let tasks = json!([
        task("extract", &json!({})),
        task(
            "load",
            &json!({"a": output_of("ghost"), "b": output_of("ghost")})
        )
    ]);
    assert_eq!(
        problems(&json!({"tasks": tasks, "edges": []})),
        ["task `load` uses the output of `ghost`, which isn't a task of the DAG"]
    );
    let run_info = json!({"ref": {"kind": "dag_ctx", "field": "planned_for"}});
    let tasks = json!([task("extract", &json!({"when": run_info}))]);
    assert_eq!(
        problems(&json!({"tasks": tasks, "edges": []})),
        Vec::<String>::new()
    );
}

/// `extract` as a resource's setup, `load` its user, `drop` its cleanup, changed by `changes`.
fn resource_problems_of(resource: &Json, edges: &Json) -> Vec<String> {
    problems(&json!({
        "tasks": [task("extract", &json!({})), task("load", &json!({})), task("drop", &json!({}))],
        "edges": edges,
        "resources": [resource],
    }))
}

#[test]
fn a_resource_joins_its_parts_and_users() {
    let resource = json!({"setup": "extract", "cleanup": "drop", "users": ["load"]});
    let edges = json!([
        edge("extract", "load", "requirement"),
        edge("extract", "drop", "requirement")
    ]);
    assert_eq!(
        resource_problems_of(&resource, &edges),
        Vec::<String>::new()
    );
}

#[test]
fn a_resource_names_tasks_of_the_dag() {
    let resource = json!({"setup": "extract", "cleanup": "drop", "users": ["nowhere"]});
    let edges = json!([edge("extract", "drop", "data")]);
    assert_eq!(
        resource_problems_of(&resource, &edges),
        vec![
            "resource `extract`: `nowhere` isn't a task of the DAG",
            "resource `extract`: its users and the edges from it differ on `nowhere`",
        ]
    );
}

#[test]
fn a_task_is_a_part_of_one_resource() {
    let found = problems(&json!({
        "resources": [
            {"setup": "extract", "cleanup": "load", "users": []},
            {"setup": "extract", "cleanup": "load", "users": []},
        ],
        "edges": [edge("extract", "load", "data")],
    }));
    assert_eq!(
        found,
        vec![
            "task `extract` is a part of two resources",
            "task `load` is a part of two resources",
        ]
    );
}

#[test]
fn a_part_of_a_resource_isnt_its_user() {
    let resource = json!({"setup": "extract", "cleanup": "drop", "users": ["extract"]});
    let edges = json!([edge("extract", "drop", "data")]);
    assert_eq!(
        resource_problems_of(&resource, &edges),
        vec!["resource `extract`: a part of it can't be its user"]
    );
}

#[test]
fn a_cleanup_follows_its_setup_along_a_data_or_requirement_edge() {
    let resource = json!({"setup": "extract", "cleanup": "drop", "users": []});
    assert_eq!(
        resource_problems_of(&resource, &json!([edge("extract", "drop", "order")])),
        vec!["resource `extract`: no data or requirement edge to its cleanup `drop`"]
    );
}

#[test]
fn the_users_and_the_edges_from_the_setup_agree() {
    let edges = json!([edge("extract", "drop", "data")]);
    let listed = json!({"setup": "extract", "cleanup": "drop", "users": ["load"]});
    assert_eq!(
        resource_problems_of(&listed, &edges),
        vec!["resource `extract`: its users and the edges from it differ on `load`"]
    );
    let edges = json!([
        edge("extract", "drop", "data"),
        edge("extract", "load", "requirement")
    ]);
    let unlisted = json!({"setup": "extract", "cleanup": "drop", "users": []});
    assert_eq!(
        resource_problems_of(&unlisted, &edges),
        vec!["resource `extract`: its users and the edges from it differ on `load`"]
    );
}

#[test]
fn a_setup_isnt_mapped() {
    let mut mapped = task("extract", &json!({}));
    mapped["expand"] = json!({"product": [{"name": "n", "value": {"literal": [1, 2]}}]});
    let found = problems(&json!({
        "tasks": [mapped, task("drop", &json!({}))],
        "edges": [edge("extract", "drop", "requirement")],
        "resources": [{"setup": "extract", "cleanup": "drop", "users": []}],
    }));
    assert!(
        found.contains(&"resource `extract`: `extract` can't be mapped".to_owned()),
        "{found:?}"
    );
}

#[test]
fn cleanups_cant_wait_for_each_other() {
    let found = problems(&json!({
        "tasks": [
            task("sa", &json!({})), task("ca", &json!({})),
            task("sb", &json!({})), task("cb", &json!({})),
        ],
        "edges": [
            edge("sa", "ca", "requirement"), edge("sa", "cb", "requirement"),
            edge("sb", "cb", "requirement"), edge("sb", "ca", "requirement"),
        ],
        "resources": [
            {"setup": "sa", "cleanup": "ca", "users": ["cb"]},
            {"setup": "sb", "cleanup": "cb", "users": ["ca"]},
        ],
    }));
    assert_eq!(
        found,
        vec!["the tasks `ca`, `cb` depend on each other in a cycle"]
    );
}

#[test]
fn a_user_cant_wait_for_the_cleanup_waiting_for_it() {
    let resource = json!({"setup": "extract", "cleanup": "drop", "users": ["load"]});
    let edges = json!([
        edge("extract", "drop", "requirement"),
        edge("extract", "load", "requirement"),
        edge("drop", "load", "order"),
    ]);
    assert_eq!(
        resource_problems_of(&resource, &edges),
        vec!["the tasks `drop`, `load` depend on each other in a cycle"]
    );
}

#[test]
fn a_task_that_isnt_a_user_may_wait_for_a_cleanup() {
    let resource = json!({"setup": "extract", "cleanup": "drop", "users": []});
    let edges = json!([
        edge("extract", "drop", "requirement"),
        edge("drop", "load", "order"),
    ]);
    assert_eq!(
        resource_problems_of(&resource, &edges),
        Vec::<String>::new()
    );
}

fn file(path: &str, dags: Vec<Dag>) -> ParsedFile {
    ParsedFile {
        path: path.to_owned(),
        dags,
        connections: Vec::new(),
        error: None,
        warnings: Vec::new(),
    }
}

#[test]
fn files_with_invalid_dags_become_import_errors() {
    let bad = dag_with(&json!({"max_active_runs": 0, "produced_by": "0.9"}));
    let files = validate_files(
        "etl",
        vec![file("sync.py", vec![dag_with(&json!({})), bad])],
        &Checks::new(MASTER, LIMIT),
    );
    assert_eq!(files[0].dags, []);
    let error = files[0].error.as_ref().unwrap();
    assert_eq!(error.exception, INVALID_DAG_EXCEPTION);
    assert_eq!(
        error.message,
        "DAG `sync`: produced by schedr 0.9, which this master (schedr 0.1) can't read\n\
         DAG `sync`: max_active_runs must be at least 1"
    );
}

#[test]
fn valid_files_and_import_errors_are_kept() {
    let error = ErrorSummary {
        exception: "ImportError".to_owned(),
        message: "no module".to_owned(),
        traceback: String::new(),
    };
    let failed = ParsedFile {
        path: "bad.py".to_owned(),
        dags: vec![dag_with(&json!({}))],
        connections: Vec::new(),
        error: Some(error.clone()),
        warnings: Vec::new(),
    };
    let good = file("sync.py", vec![dag_with(&json!({}))]);
    let files = validate_files(
        "etl",
        vec![good.clone(), failed],
        &Checks::new(MASTER, LIMIT),
    );
    assert_eq!(files[0], good);
    assert_eq!(files[1].dags, []);
    assert_eq!(files[1].error, Some(error));
}

#[test]
fn paths_must_stay_inside_the_root() {
    for path in ["", "/etc/x.py", "../x.py", "a/../x.py", "./x.py", "a\\x.py"] {
        // Its connections go too: a connection test would import the file by its path.
        let mut outside = file(path, vec![]);
        outside.connections =
            vec![serde_json::from_value(json!({"name": "db", "params": {}})).unwrap()];
        let files = validate_files("etl", vec![outside], &Checks::new(MASTER, LIMIT));
        assert_eq!(files[0].connections, [], "{path}");
        let error = files[0].error.as_ref().unwrap();
        assert_eq!(error.exception, INVALID_PATH_EXCEPTION, "{path}");
        assert_eq!(
            error.message,
            format!("`{path}` isn't a relative path inside the root")
        );
    }
    let files = validate_files(
        "etl",
        vec![file("team/a.py", vec![])],
        &Checks::new(MASTER, LIMIT),
    );
    assert_eq!(files[0].error, None);
}

#[test]
fn a_task_whose_output_cant_reach_the_master_is_refused() {
    let mut command = task("dump", &json!({}));
    command["kind"] = json!("command");
    command["command"] = json!({"shell": [{"text": "cat big"}]});
    command["settings"]["max_output_size"] = json!(40u64 << 20);
    let mut python = task("load", &json!({}));
    python["settings"]["max_output_size"] = json!(40u64 << 20);
    let dag = dag_with(&json!({"tasks": [command, python], "edges": []}));
    // 40 MiB fits 64 MiB for a Python task; a command's two streams don't.
    assert_eq!(
        output_problems(&dag, LIMIT),
        [
            "task `dump`: max_output_size 40 MiB (stdout and stderr each) can't reach the master, \
          which accepts results up to 32736 KiB ([master] grpc_max_message_size, less 64 KiB \
          for the rest of the result): lower max_output_size, or raise the setting"
        ]
    );
    assert_eq!(output_problems(&dag, 128 << 20), Vec::<String>::new());
    // Through the file check: the DAG is refused as an import error.
    let files = validate_files(
        "etl",
        vec![file("sync.py", vec![dag])],
        &Checks::new(MASTER, LIMIT),
    );
    let error = files[0].error.as_ref().unwrap();
    assert!(
        error
            .message
            .contains("task `dump`: max_output_size 40 MiB"),
        "{}",
        error.message
    );
}

#[test]
fn groups_must_be_unique_nest_properly_and_be_declared() {
    let mut grouped = task("load.x", &json!({}));
    grouped["group"] = json!("load");
    let mut stray = task("y", &json!({}));
    stray["group"] = json!("ghost");
    let groups = json!([
        {"id": "load"},
        {"id": "load.eu", "parent": "load"},
        {"id": "a", "parent": "b"},
        {"id": "b", "parent": "a"},
        {"id": "c", "parent": "missing"},
        {"id": "load"}
    ]);
    assert_eq!(
        problems(&json!({"tasks": [grouped, stray], "edges": [], "groups": groups})),
        [
            "group `load` is defined more than once",
            "group `a` is nested inside itself",
            "group `b` is nested inside itself",
            "group `c`: its parent `missing` isn't a group of the DAG",
            "task `y`: its group `ghost` isn't a group of the DAG"
        ]
    );
    let mut fine = task("load.eu.x", &json!({}));
    fine["group"] = json!("load.eu");
    let groups = json!([{"id": "load"}, {"id": "load.eu", "parent": "load"}]);
    assert_eq!(
        problems(&json!({"tasks": [fine], "edges": [], "groups": groups})),
        Vec::<String>::new()
    );
}

#[test]
fn a_mapped_branch_and_a_kwargs_argument_also_fixed_are_refused() {
    let mut chooser = branch("choose");
    chooser["expand"] = json!({"product": [{"name": "n", "value": {"literal": [1]}}]});
    // `.partial(bucket=...).expand_kwargs([...])` naming `bucket` again, as a literal list
    // and as a list holding references.
    let mut literal = task("load", &json!({"bucket": {"literal": "raw"}}));
    literal["expand"] = json!({"kwargs": {"literal": [{"path": "a"}, {"bucket": "b"}]}});
    let mut listed = task("copy", &json!({"bucket": {"literal": "raw"}}));
    listed["expand"] = json!({"kwargs": {"list": [{"map": {"bucket": output_of("extract")}}]}});
    let tasks = [chooser, literal, listed, task("extract", &json!({}))];
    let edges = [edge("extract", "copy", "data")];
    assert_eq!(
        problems(&json!({"tasks": tasks, "edges": edges})),
        [
            "task `choose`: a @branch task can't be mapped (`.expand()`)",
            "task `load`: argument `bucket` is both fixed (`.partial()`) and mapped \
             (`.expand_kwargs()`)",
            "task `copy`: argument `bucket` is both fixed (`.partial()`) and mapped \
             (`.expand_kwargs()`)",
        ]
    );
}

#[test]
fn a_mapped_tasks_names_are_checked() {
    let mut mapped = task("load", &json!({"bucket": {"literal": "raw"}}));
    mapped["expand"] = json!({"product": [
        {"name": "bucket", "value": {"literal": ["a"]}},
        {"name": "path", "value": {"literal": [1]}},
        {"name": "path", "value": {"literal": [2]}}
    ]});
    let mut empty = task("none", &json!({}));
    empty["expand"] = json!({"product": []});
    assert_eq!(
        problems(&json!({"tasks": [mapped, empty], "edges": []})),
        [
            "task `load`: argument `bucket` is both fixed (`.partial()`) and mapped (`.expand()`)",
            "task `load`: argument `path` is mapped twice (`.expand()`)",
            "task `none`: `.expand()` names no argument"
        ]
    );
    // A reference in what it maps over is a reference like any: it needs its data edge.
    let mut over_output = task("load", &json!({}));
    over_output["expand"] = json!({"kwargs": output_of("extract")});
    let found =
        problems(&json!({"tasks": [task("extract", &json!({})), over_output], "edges": []}));
    assert!(
        found.contains(
            &"task `load` uses the output of `extract` without a data edge from it".to_owned()
        ),
        "{found:?}"
    );
}

#[test]
fn a_shell_command_with_a_placeholder_is_refused() {
    let mut shell = task("notify", &json!({}));
    shell["kind"] = json!("command");
    shell["command"] = json!({"shell": [{"text": "./notify.sh "}, output_of("extract")]});
    let mut argv = task("echo", &json!({}));
    argv["kind"] = json!("command");
    argv["command"] = json!({"argv": [[{"text": "echo"}], [output_of("extract")]]});
    let found = problems(&json!({
        "tasks": [task("extract", &json!({})), shell, argv],
        "edges": [edge("extract", "notify", "data"), edge("extract", "echo", "data")]
    }));
    assert_eq!(
        found,
        [
            "task `notify`: a shell command takes no placeholders (the shell would read a value \
             as code): pass values as arguments or in `env`"
        ]
    );
}

fn params_decl() -> Json {
    json!({
        "type": {"type": "dataclass", "name": "P", "fields": [
            {"name": "region", "type": {"type": "str"}, "required": false},
            {"name": "days", "type": {"type": "list", "item": {"type": "int"}}, "required": false},
        ]},
        "schema": {"type": "object", "properties": {
            "region": {"type": "string"}, "days": {"type": "array", "items": {"type": "integer"}},
        }, "additionalProperties": false},
        "defaults": {"region": "eu", "days": [1]},
    })
}

fn param(path: &Json) -> Json {
    json!({"ref": {"kind": "param", "path": path}})
}

#[test]
fn a_dags_params_references_need_its_params_and_existing_paths() {
    let uses = |path: &Json| {
        json!({"tasks": [task("extract", &json!({"r": param(path)})),
                         task("load", &json!({"rows": output_of("extract")}))]})
    };
    assert_eq!(
        problems(&uses(&json!(["region"]))),
        [
            "task `extract` uses `dag_ctx.params.region`, but the DAG declares no params \
          (`Dag(params=...)`)"
        ]
    );
    let mut with_params = uses(&json!(["days", 0]));
    with_params["params"] = params_decl();
    assert_eq!(problems(&with_params), Vec::<String>::new());
    let mut missing = uses(&json!(["nope"]));
    missing["params"] = params_decl();
    assert_eq!(
        problems(&missing),
        ["task `extract` uses `dag_ctx.params.nope`, which the DAG's params don't have"]
    );
}

#[test]
fn a_task_cant_be_mapped_over_the_runs_params() {
    let mut mapped = task("extract", &json!({}));
    mapped["expand"] = json!({"product": [{"name": "day", "value": param(&json!(["days"]))}]});
    let found = problems(&json!({
        "params": params_decl(),
        "tasks": [mapped, task("load", &json!({"rows": output_of("extract")}))],
    }));
    assert_eq!(
        found,
        [
            "task `extract` is mapped over the run's params: the number of instances can't depend \
          on them"
        ]
    );
}

#[test]
fn a_dags_params_are_checked_with_it() {
    let mut decl = params_decl();
    decl["schema"]["properties"]["region"]["x-widget"] = json!("select");
    let found = problems(&json!({"params": decl}));
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("keyword `x-widget`"), "{found:?}");
    let mut required = params_decl();
    required["defaults"] = json!({"days": [1]});
    required["schema"]["required"] = json!(["region"]);
    let scheduled = json!({"params": required, "trigger": {"kind": "every", "interval_ms": 60_000, "catchup": false}});
    assert_eq!(
        problems(&scheduled),
        ["params: `region` has no default, but the DAG has a schedule (its runs use the defaults)"]
    );
}

fn secret(storage: Option<&str>, path: &str, key: Option<&str>) -> Json {
    let mut reference = json!({"kind": "secret", "path": path});
    if let Some(storage) = storage {
        reference["storage"] = json!(storage);
    }
    if let Some(key) = key {
        reference["key"] = json!(key);
    }
    reference
}

fn command(id: &str, argv: &Json, env: &Json) -> Json {
    let mut command = task(id, &json!({}));
    command["kind"] = json!("command");
    command["command"] = json!({"argv": argv});
    command["env"] = env.clone();
    command["output_type"] = json!({"type": "process_result"});
    command
}

#[test]
fn secrets_where_they_belong_pass() {
    let password = secret(None, "etl/db", Some("password"));
    let mut python = task(
        "load",
        &json!({
            "conn": {"ref": secret(Some("other"), "connections/db", None)},
            "dsn": {"template": [{"text": "postgres://u:"}, {"ref": password}]},
        }),
    );
    python["secrets"] = json!([
        {"kind": "secret", "path": "etl/api", "key": "token"},
        {"kind": "prefix", "storage": "other", "prefix": "tenants/"},
    ]);
    let shell = command(
        "dump",
        &json!([[{"text": "pg_dump"}]]),
        &json!({"PGPASSWORD": [{"ref": password}]}),
    );
    let dag = dag_with(&json!({"tasks": [python, shell], "edges": []}));
    assert_eq!(
        dag_problems(&dag, "etl", "sync.py", MASTER),
        Vec::<String>::new()
    );
    let storages = BTreeSet::from(["other".to_owned()]);
    assert_eq!(storage_problems(&dag, &storages), Vec::<String>::new());
}

#[test]
fn secrets_where_they_dont_belong_are_refused() {
    let password = secret(None, "etl/db", Some("password"));
    let in_argv = command(
        "dump",
        &json!([[{"text": "pg_dump"}], [{"text": "--password="}, {"ref": password}]]),
        &json!({"WHOLE": [{"ref": secret(None, "etl/db", None)}]}),
    );
    let mut bad = task(
        "load",
        &json!({"x": {"ref": secret(None, "etl//db", Some("k"))}}),
    );
    bad["secrets"] = json!([{"kind": "prefix", "prefix": "tenants"}]);
    let mut mapped = task("each", &json!({}));
    mapped["expand"] = json!({"product": [{"name": "x", "value": {"ref": password}}]});
    // A whole secret written into what instances get, as a product or as kwargs.
    let whole = json!({"template": [{"text": "u:"}, {"ref": secret(None, "etl/db", None)}]});
    let mut in_product = task("each_product", &json!({}));
    in_product["expand"] = json!({"product": [{"name": "x", "value": {"list": [whole]}}]});
    let mut in_kwargs = task("each_kwargs", &json!({}));
    in_kwargs["expand"] = json!({"kwargs": {"list": [{"map": {"x": whole}}]}});
    let found = problems(&json!({
        "tasks": [in_argv, bad, mapped, in_product, in_kwargs],
        "edges": []
    }));
    let expected = [
        "task `dump`: `secrets['etl/db']['password']` in the command line",
        "task `dump`: `secrets['etl/db']` is a whole secret",
        "task `load`: `secrets['etl//db']['k']`: a path has no empty part",
        "task `load`: secrets=: `SecretPrefix('tenants')`: a prefix ends with `/`",
        "task `each` is mapped over a secret",
        "task `each_product`: `secrets['etl/db']` is a whole secret",
        "task `each_kwargs`: `secrets['etl/db']` is a whole secret",
    ];
    for start in expected {
        assert!(
            found.iter().any(|p| p.starts_with(start)),
            "{start} not in {found:#?}"
        );
    }
}

#[test]
fn a_storage_the_worker_lacks_is_refused_once_per_task() {
    let mut load = task(
        "load",
        &json!({"a": {"ref": secret(Some("vault"), "a", Some("k"))},
                "b": {"ref": secret(Some("vault"), "b", Some("k"))},
                "c": {"ref": secret(None, "c", Some("k"))}}),
    );
    load["secrets"] = json!([{"kind": "prefix", "storage": "old", "prefix": "x/"}]);
    let dag = dag_with(&json!({"tasks": [load], "edges": []}));
    assert_eq!(
        storage_problems(&dag, &BTreeSet::from(["db".to_owned()])),
        [
            "task `load`: no secret storage `vault` on the worker that parsed it (configured: \
             `db`; `[secret_storage.vault]` in the workers' configuration)",
            "task `load`: no secret storage `old` on the worker that parsed it (configured: \
             `db`; `[secret_storage.old]` in the workers' configuration)",
        ]
    );
    let none = storage_problems(&dag, &BTreeSet::new());
    assert!(none[0].contains("(configured: none;"), "{none:?}");
}

#[test]
fn a_parse_is_checked_against_the_workers_storages() {
    let load = task(
        "load",
        &json!({"a": {"ref": secret(Some("vault"), "a", Some("k"))}}),
    );
    let dag = dag_with(&json!({"tasks": [load], "edges": []}));
    let storages = BTreeSet::new();
    let checks = Checks {
        secret_storages: Some(&storages),
        ..Checks::new(MASTER, LIMIT)
    };
    let files = validate_files("etl", vec![file("sync.py", vec![dag.clone()])], &checks);
    assert_eq!(files[0].dags, []);
    assert!(
        files[0]
            .error
            .as_ref()
            .unwrap()
            .message
            .contains("no secret storage `vault`")
    );
    // Unknown storages (a worker restored from the database): not checked.
    let files = validate_files(
        "etl",
        vec![file("sync.py", vec![dag])],
        &Checks::new(MASTER, LIMIT),
    );
    assert_eq!(files[0].error, None);
}

fn connection(name: &str, host: &str) -> schedr_model::connection::ConnectionDef {
    serde_json::from_value(json!({"name": name, "params": {"host": {"literal": host}}})).unwrap()
}

#[test]
fn a_file_refused_for_its_dag_keeps_its_connections_one_failing_to_import_doesnt() {
    // The registry (worker, DAG page, `dags check`) is built from what validation leaves.
    let mut invalid = file("shared.py", vec![dag_with(&json!({"max_active_runs": 0}))]);
    invalid.connections = vec![connection("warehouse", "db")];
    let mut failed = file("broken.py", Vec::new());
    failed.connections = vec![connection("stale", "db")];
    failed.error = Some(ErrorSummary {
        exception: "ImportError".to_owned(),
        message: "no module".to_owned(),
        traceback: String::new(),
    });
    let files = validate_files("etl", vec![invalid, failed], &Checks::new(MASTER, LIMIT));
    assert!(files[0].error.is_some());
    assert_eq!(files[0].connections, [connection("warehouse", "db")]);
    assert_eq!(files[1].connections, []);
}

#[test]
fn a_connection_over_1_mib_fails_its_files_import() {
    // It would travel with each assignment of a task opening it, within the worker's 4 MiB.
    let mut big = file("sync.py", vec![dag_with(&json!({}))]);
    big.connections = vec![
        connection(
            "huge",
            &"x".repeat(schedr_model::connection::MAX_CONNECTION_SIZE),
        ),
        connection("small", "db"),
    ];
    let files = validate_files("etl", vec![big], &Checks::new(MASTER, LIMIT));
    let error = files[0].error.as_ref().unwrap();
    assert_eq!(
        error.message,
        "connection `huge`: its values take 1048624 bytes, over the 1 MiB a connection may hold \
         (they travel with each task opening it); give bigger data to the task"
    );
    assert_eq!(files[0].dags, []);
    assert_eq!(files[0].connections, [connection("small", "db")]);
}

#[test]
fn a_connection_given_by_prefix_must_be_declared_under_a_variables_prefix() {
    let mut given = command("dump", &json!([[{"text": "psql"}]]), &json!({}));
    given["connections"] = json!(["warehouse"]);
    given["connection_env"] = json!({"DB": "warehouse", "1X": "warehouse", "SCHEDR": "warehouse",
                                     "OTHER": "other"});
    let found = problems(&json!({"tasks": [given], "edges": []}));
    let expected = [
        "task `dump`: connections=: `1X`: a prefix is letters, digits and `_`",
        "task `dump`: connections=: `SCHEDR`: prefixes starting with SCHEDR are schedr's own",
        "task `dump`: connections=: `OTHER` names `other`, which the task doesn't declare",
    ];
    for start in expected {
        assert!(
            found.iter().any(|p| p.starts_with(start)),
            "{start} not in {found:#?}"
        );
    }
    assert!(!found.iter().any(|p| p.contains("`DB`")), "{found:#?}");
}

#[test]
fn a_cleanup_of_another_kind_than_its_task_is_refused() {
    let settings = task("x", &json!({}))["settings"].clone();
    let python_cleanup =
        json!({"run": "python", "timeout_ms": 1000, "retries": 0, "retry_delay_ms": 0});
    let mut command_cleanup = python_cleanup.clone();
    command_cleanup["run"] = json!({"command": {"shell": [{"text": "true"}]}});
    let mut python = task("python_task", &json!({}));
    python["cleanup"] = command_cleanup.clone();
    let command = json!({
        "id": "command_task", "kind": "command", "command": {"shell": [{"text": "true"}]},
        "settings": settings, "output_type": {"type": "process_result"},
        "cleanup": python_cleanup,
    });
    let empty = json!({
        "id": "empty_task", "kind": "empty", "settings": settings,
        "output_type": {"type": "none"}, "cleanup": command_cleanup,
    });
    let matching = {
        let mut python = task("fine", &json!({}));
        python["cleanup"] = command["cleanup"].clone();
        python
    };
    let found = problems(&json!({"tasks": [python, command, empty, matching], "edges": []}));
    let expected = [
        "task `python_task`: a Python task's cleanup is a function",
        "task `command_task`: a command task's cleanup is a command",
        "task `empty_task`: an empty task has nothing to clean up",
    ];
    assert_eq!(found.len(), expected.len(), "{found:?}");
    for text in expected {
        assert!(found.iter().any(|p| p.contains(text)), "{found:?}");
    }
}
