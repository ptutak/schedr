use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;
use crate::value::{DagCtxField, TemplatePart, TemplateRef};

fn id(s: &str) -> TaskId {
    TaskId::new(s).unwrap()
}

fn output_ref(task: &str) -> Reference {
    Reference::Output {
        on: Vec::new(),
        task: id(task),
        path: vec![],
    }
}

fn ref_part(reference: Reference) -> TemplatePart {
    TemplatePart::Ref(TemplateRef {
        reference,
        format: None,
    })
}

#[test]
fn default_settings_match_the_documented_defaults() {
    let settings = TaskSettings::default();
    assert_eq!(settings.retries, 0);
    assert_eq!(settings.retry_delay_ms, 300_000);
    assert_eq!(settings.timeout_ms, None);
    assert_eq!(settings.queue, "default");
    assert_eq!(settings.max_output_size, 1_048_576);
}

#[test]
fn settings_reject_unknown_fields() {
    let mut json = serde_json::to_value(TaskSettings::default()).unwrap();
    json["pool"] = json!("x");
    assert!(serde_json::from_value::<TaskSettings>(json).is_err());
}

#[test]
fn python_task_serializes_flat_with_a_kind_tag() {
    let task = Task {
        id: id("load"),
        kind: TaskKind::Python {
            arguments: BTreeMap::from([("rows".to_owned(), Value::Ref(output_ref("extract")))]),
        },
        settings: TaskSettings::default(),
        output_type: TypeDesc::Int,
        outcomes: crate::default_outcomes(),
    };
    let expected = json!({
        "id": "load",
        "kind": "python",
        "arguments": {"rows": {"ref": {"kind": "output", "task": "extract"}}},
        "settings": {
            "retries": 0, "retry_delay_ms": 300_000, "timeout_ms": null,
            "queue": "default", "max_output_size": 1_048_576,
            "preload": true, "fresh_process": false,
        },
        "output_type": {"type": "int"},
    });
    assert_eq!(serde_json::to_value(&task).unwrap(), expected);
    assert_eq!(serde_json::from_value::<Task>(expected).unwrap(), task);
}

#[test]
fn command_and_empty_tasks_round_trip() {
    let command = Task {
        id: id("notify"),
        kind: TaskKind::Command {
            command: CommandLine::Argv(vec![Template::text("./notify.sh")]),
            env: BTreeMap::from([("TOKEN".to_owned(), Template::text("x"))]),
        },
        settings: TaskSettings::default(),
        output_type: TypeDesc::ProcessResult,
        outcomes: crate::default_outcomes(),
    };
    let json = serde_json::to_value(&command).unwrap();
    assert_eq!(json["kind"], "command");
    assert_eq!(
        json["command"],
        json!({"argv": [[{"text": "./notify.sh"}]]})
    );
    assert_eq!(json["env"], json!({"TOKEN": [{"text": "x"}]}));
    assert_eq!(serde_json::from_value::<Task>(json).unwrap(), command);

    let empty = Task {
        id: id("start"),
        kind: TaskKind::Empty,
        settings: TaskSettings::default(),
        output_type: TypeDesc::None,
        outcomes: crate::default_outcomes(),
    };
    let json = serde_json::to_value(&empty).unwrap();
    assert_eq!(json["kind"], "empty");
    assert_eq!(serde_json::from_value::<Task>(json).unwrap(), empty);
}

#[test]
fn command_without_env_omits_it_and_python_without_arguments_defaults_them() {
    let kind = TaskKind::Command {
        command: CommandLine::Shell(Template::text("ls")),
        env: BTreeMap::new(),
    };
    assert_eq!(
        serde_json::to_value(&kind).unwrap(),
        json!({"kind": "command", "command": {"shell": [{"text": "ls"}]}})
    );
    let python: TaskKind = serde_json::from_value(json!({"kind": "python"})).unwrap();
    assert_eq!(
        python,
        TaskKind::Python {
            arguments: BTreeMap::new()
        }
    );
}

#[test]
fn command_line_templates_in_order() {
    let shell = CommandLine::Shell(Template::text("ls -l"));
    assert_eq!(shell.templates(), vec![&Template::text("ls -l")]);
    let argv = CommandLine::Argv(vec![Template::text("a"), Template::text("b")]);
    assert_eq!(
        argv.templates(),
        vec![&Template::text("a"), &Template::text("b")]
    );
}

#[test]
fn references_of_a_python_task_come_from_its_arguments() {
    let task = Task {
        id: id("load"),
        kind: TaskKind::Python {
            arguments: BTreeMap::from([
                ("a".to_owned(), Value::Ref(output_ref("x"))),
                ("b".to_owned(), Value::Literal(json!(1))),
                (
                    "c".to_owned(),
                    Value::List(vec![Value::Ref(output_ref("y"))]),
                ),
            ]),
        },
        settings: TaskSettings::default(),
        output_type: TypeDesc::None,
        outcomes: crate::default_outcomes(),
    };
    assert_eq!(task.references(), vec![&output_ref("x"), &output_ref("y")]);
}

#[test]
fn references_of_a_command_task_come_from_its_command_line_and_env() {
    let planned_for = Reference::DagCtx {
        field: DagCtxField::PlannedFor,
    };
    let task = Task {
        id: id("notify"),
        kind: TaskKind::Command {
            command: CommandLine::Argv(vec![
                Template::text("./notify.sh"),
                Template(vec![ref_part(planned_for.clone())]),
            ]),
            env: BTreeMap::from([(
                "COUNT".to_owned(),
                Template(vec![ref_part(output_ref("load"))]),
            )]),
        },
        settings: TaskSettings::default(),
        output_type: TypeDesc::ProcessResult,
        outcomes: crate::default_outcomes(),
    };
    assert_eq!(task.references(), vec![&planned_for, &output_ref("load")]);
}

#[test]
fn empty_tasks_have_no_references() {
    let task = Task {
        id: id("start"),
        kind: TaskKind::Empty,
        settings: TaskSettings::default(),
        output_type: TypeDesc::None,
        outcomes: crate::default_outcomes(),
    };
    assert_eq!(task.references(), Vec::<&Reference>::new());
}

#[test]
fn settings_without_how_a_task_starts_read_as_forked_from_the_preloaded_server() {
    let settings: TaskSettings = serde_json::from_value(json!({
        "retries": 0, "retry_delay_ms": 300_000, "timeout_ms": null,
        "queue": "default", "max_output_size": 1_048_576,
    }))
    .unwrap();
    assert_eq!((settings.preload, settings.fresh_process), (true, false));
}
