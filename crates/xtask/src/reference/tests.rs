use pretty_assertions::assert_eq;
use schedr_model::metric_doc::MetricKind;
use serde_json::json;

use super::*;

fn config_schema() -> Value {
    json!({
        "properties": {
            "interpreters": {
                "description": "`[interpreters]`: interpreter names.",
                "type": "object",
                "additionalProperties": {"$ref": "#/$defs/Repr"},
                "default": {}
            },
            "log": {
                "description": "`[log]`: schedr's own logs.",
                "$ref": "#/$defs/LogSection",
                "default": {}
            }
        },
        "$defs": {
            "Repr": {
                "description": "How it is written.",
                "anyOf": [
                    {"description": "A path.", "type": "string"},
                    {"description": "A table.", "$ref": "#/$defs/Table"}
                ]
            },
            "Table": {"type": "object", "properties": {"path": {"type": "string"}}},
            "Duration": {"type": "string", "format": "duration"},
            "LogSection": {
                "type": "object",
                "properties": {
                    "every": {"description": "How often.", "$ref": "#/$defs/Duration", "default": "30s"},
                    "format": {"description": "The format.", "$ref": "#/$defs/Format", "default": "auto"},
                    "url": {"description": "Where | to.", "type": ["string", "null"]},
                    "auth": {"description": "`[log.auth]`.", "$ref": "#/$defs/Auth", "default": {}}
                }
            },
            "Format": {
                "oneOf": [
                    {"const": "auto", "type": "string", "description": "Pick one."},
                    {"const": "json", "type": "string", "description": "JSON."}
                ]
            },
            "Auth": {
                "description": "`[log.auth]`: who.",
                "type": "object",
                "properties": {"header": {"type": "array", "items": {"type": "string"}, "default": []}}
            }
        }
    })
}

#[test]
fn cells_are_one_line_with_pipes_escaped() {
    assert_eq!(cell("a |\n  b"), "a \\| b");
}

#[test]
fn types_read_as_words() {
    let root = config_schema();
    let t = |schema: Value| type_text(&root, &schema);
    assert_eq!(t(json!({"$ref": "#/$defs/Duration"})), "duration");
    assert_eq!(t(json!({"$ref": "#/$defs/Size"})), "size");
    assert_eq!(t(json!({"$ref": "#/$defs/PathList"})), "paths");
    assert_eq!(
        t(json!({"$ref": "#/$defs/Format"})),
        "one of `auto`, `json`"
    );
    assert_eq!(t(json!({"enum": ["a", "b"]})), "one of `a`, `b`");
    assert_eq!(t(json!({"$ref": "#/$defs/Table"})), "`Table`");
    assert_eq!(t(json!({"$ref": "#/$defs/Repr"})), "string or `Table`");
    assert_eq!(
        t(json!({"anyOf": [{"$ref": "#/$defs/Duration"}, {"type": "null"}]})),
        "duration"
    );
    assert_eq!(t(json!({"type": ["integer", "null"]})), "integer");
    assert_eq!(t(json!({"type": "string", "format": "ip"})), "IP address");
    assert_eq!(
        t(json!({"type": "string", "format": "date-time"})),
        "timestamp"
    );
    assert_eq!(
        t(json!({"type": "array", "items": {"type": "string"}})),
        "list of strings"
    );
    assert_eq!(
        t(json!({"type": "array", "items": {"$ref": "#/$defs/Table"}})),
        "list of `Table`"
    );
    assert_eq!(t(json!({"type": "array"})), "list");
    assert_eq!(t(json!({"type": "object"})), "table");
    assert_eq!(t(json!({})), "any");
}

#[test]
fn the_configuration_page_has_a_section_per_table_and_a_row_per_key() {
    let page = config_page(&config_schema());
    let expected = [
        "## `[interpreters]`\n\nInterpreter names.\n\n| Key | Type | Description |\n|---|---|---|\n\
         | any name | string or `Table` | How it is written. A path. A table. \
         Environment: `SCHEDR_CONFIG__INTERPRETERS__<NAME>` (the plain path form). |\n",
        "## `[log]`\n\nSchedr's own logs.\n",
        "| `every` | duration | `30s` | How often. Environment: `SCHEDR_CONFIG__LOG__EVERY`. |",
        "| `format` | one of `auto`, `json` | `auto` | The format. `auto`: Pick one. `json`: JSON. \
         Environment: `SCHEDR_CONFIG__LOG__FORMAT`. |",
        "| `url` | string | not set | Where \\| to. Environment: `SCHEDR_CONFIG__LOG__URL`. |",
        // A description that only names the table falls back to the table's own.
        "### `[log.auth]`\n\nWho.\n",
        "| `header` | list of strings | none |  Environment: `SCHEDR_CONFIG__LOG__AUTH__HEADER`. |",
    ];
    for part in expected {
        assert!(page.contains(part), "{part}\n---\n{page}");
    }
}

#[test]
fn the_metrics_page_shows_exposed_names_by_role() {
    const RUNS: MetricDoc = MetricDoc {
        name: "runs_finished",
        kind: MetricKind::Counter,
        labels: &["dag_id", "result"],
        help: "Runs finished",
    };
    const FDS: MetricDoc = MetricDoc {
        name: "process_open_fds",
        kind: MetricKind::Gauge,
        labels: &[],
        help: "Open files",
    };
    let page = metrics_page(&[
        MetricGroup {
            role: "Master",
            prefix: Some("schedr"),
            metrics: &[RUNS],
        },
        MetricGroup {
            role: "Every role",
            prefix: None,
            metrics: &[FDS],
        },
    ]);
    assert!(
        page.contains(
            "## Master\n\n| Metric | Type | Labels | Description |\n|---|---|---|---|\n\
             | `schedr_runs_finished_total` | counter | `dag_id`, `result` | Runs finished |\n"
        ),
        "{page}"
    );
    assert!(
        page.contains("| `process_open_fds` | gauge | none | Open files |"),
        "{page}"
    );
}

fn cli() -> clap::Command {
    let trigger = clap::Command::new("trigger")
        .about("Trigger a run")
        .arg(clap::Arg::new("dag").required(true).help("The DAG"))
        .arg(
            clap::Arg::new("for")
                .long("for")
                .value_name("TIME")
                .default_value("now")
                .env("SCHEDR_FOR")
                .help("When"),
        )
        .arg(
            clap::Arg::new("json")
                .short('j')
                .long("json")
                .action(clap::ArgAction::SetTrue)
                .help("As JSON"),
        );
    clap::Command::new("schedr")
        .about("A scheduler.")
        .arg(
            clap::Arg::new("config")
                .long("config")
                .global(true)
                .value_name("PATH")
                .help("The file"),
        )
        .subcommand(
            clap::Command::new("runs")
                .about("Runs")
                .subcommand(trigger)
                .subcommand(clap::Command::new("secret").hide(true)),
        )
}

#[test]
fn the_cli_page_shows_every_visible_command_with_its_arguments() {
    let page = cli_page(&cli());
    let expected = [
        "## Options of every command\n\n| Argument | Description |\n|---|---|\n\
         | `--config <PATH>` | The file |\n",
        "## `schedr runs`\n\nRuns\n\n```text\nschedr runs [OPTIONS] [COMMAND]\n```\n",
        "## `schedr runs trigger`\n\nTrigger a run\n\n```text\n\
         schedr runs trigger [OPTIONS] <dag>\n```\n",
        "| `<DAG>` | The DAG |",
        "| `--for <TIME>` | When Default: `now`. Environment: `SCHEDR_FOR`. |",
        "| `-j, --json` | As JSON |",
    ];
    for part in expected {
        assert!(page.contains(part), "{part}\n---\n{page}");
    }
    assert!(!page.contains("secret"), "{page}");
    assert!(!page.contains("`schedr runs help`"), "{page}");
}

fn openapi() -> Value {
    json!({
        "paths": {
            "/api/v1/runs/{run_id}": {
                "post": {
                    "summary": "Cancel a run.",
                    "parameters": [
                        {"name": "run_id", "in": "path", "required": true,
                         "description": "The run", "schema": {"type": "string"}},
                        {"name": "limit", "in": "query", "required": false,
                         "description": "At most", "schema": {"type": "integer"}}
                    ],
                    "requestBody": {"content": {"application/json": {
                        "schema": {"$ref": "#/components/schemas/Body"}}}},
                    "responses": {
                        "202": {"content": {"application/json": {
                            "schema": {"$ref": "#/components/schemas/Body"}}}},
                        "204": {}
                    }
                },
                "parameters": [{"name": "run_id", "in": "path"}]
            }
        },
        "components": {"schemas": {"Body": {
            "description": "A body.",
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "string", "description": "Its ID."},
                "note": {"type": ["string", "null"], "description": "A note."}
            }
        }}}
    })
}

#[test]
fn the_rest_page_shows_operations_and_schemas() {
    let page = rest_page(&openapi());
    let expected = [
        "### `POST /api/v1/runs/{run_id}`\n\nCancel a run.\n",
        "| `run_id` | path | string | The run |",
        "| `limit` | query (optional) | integer | At most |",
        "Body: `Body`.",
        "Answers: `202` `Body`, `204`.",
        "## Schemas\n\n### `Body`\n\nA body.\n",
        "| `id` | string | Its ID. |",
        "| `note` (optional) | string | A note. |",
    ];
    for part in expected {
        assert!(page.contains(part), "{part}\n---\n{page}");
    }
    // A path item's parameters aren't an operation.
    assert!(!page.contains("PARAMETERS"), "{page}");
}

#[test]
fn the_reference_sources_are_four_pages_under_reference() {
    let files = reference_sources(&config_schema(), &cli(), &[], &openapi());
    let paths: Vec<String> = files.iter().map(|f| f.path.display().to_string()).collect();
    assert_eq!(
        paths,
        [
            "docs/_generated/03-reference/01-configuration.md",
            "docs/_generated/03-reference/02-cli.md",
            "docs/_generated/03-reference/03-rest-api.md",
            "docs/_generated/03-reference/04-metrics.md",
        ]
    );
    assert!(files[0].contents.starts_with("# Configuration\n"));
}
