//! The reference pages of the documentation, generated from the code so they can't go stale
//! (see PROJECT.md, Repository and tooling: Documentation): the configuration (from its JSON
//! Schema), the command line (from clap), the metrics (from each role's list) and the REST API
//! (from its OpenAPI description).
//!
//! Each page is engine-neutral Markdown, as the other generated sources.

use std::fmt::Write as _;

use schedr_model::metric_doc::MetricDoc;
use serde_json::Value;

#[cfg(test)]
mod tests;

/// The prefix of the environment variables mirroring the configuration file.
pub const ENV_PREFIX: &str = "SCHEDR_CONFIG__";

/// The keys of an OpenAPI path item that are operations.
const HTTP_METHODS: [&str; 8] = [
    "get", "put", "post", "delete", "options", "head", "patch", "trace",
];

/// One role's metrics, for [`metrics_page`].
#[derive(Debug, Clone, Copy)]
pub struct MetricGroup<'a> {
    /// Who serves them, e.g. `master`.
    pub role: &'a str,
    /// The registry's prefix they are registered under, if any.
    pub prefix: Option<&'a str>,
    /// The metrics.
    pub metrics: &'a [MetricDoc],
}

/// Text for one Markdown table cell: on one line, with `|` escaped.
#[must_use]
pub fn cell(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace('|', "\\|")
}

/// The name a `$ref` points to (`#/$defs/Duration` is `Duration`).
fn ref_name(schema: &Value) -> Option<&str> {
    schema.get("$ref")?.as_str()?.rsplit('/').next()
}

/// The definition a `$ref` points to, in `root`'s `$defs` or `components.schemas`.
fn resolve<'a>(root: &'a Value, schema: &'a Value) -> &'a Value {
    let Some(name) = ref_name(schema) else {
        return schema;
    };
    root.pointer(&format!("/$defs/{name}"))
        .or_else(|| root.pointer(&format!("/components/schemas/{name}")))
        .unwrap_or(schema)
}

/// The values of an enum schema (`oneOf` / `enum` of strings), if it is one.
fn enum_values(schema: &Value) -> Option<Vec<String>> {
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return values
            .iter()
            .map(|v| v.as_str().map(str::to_owned))
            .collect();
    }
    schema
        .get("oneOf")?
        .as_array()?
        .iter()
        .map(|v| v.get("const")?.as_str().map(str::to_owned))
        .collect()
}

/// A schema's type in words: `duration`, `integer`, `list of strings`, `one of a, b`, ….
#[must_use]
pub fn type_text(root: &Value, schema: &Value) -> String {
    match ref_name(schema) {
        Some("Duration") => return "duration".to_owned(),
        Some("Size") => return "size".to_owned(),
        Some("PathList") => return "paths".to_owned(),
        _ => {}
    }
    let any_of = schema
        .get("anyOf")
        .or_else(|| resolve(root, schema).get("anyOf"));
    if let Some(any) = any_of.and_then(Value::as_array) {
        let types: Vec<String> = any
            .iter()
            .filter(|s| s.get("type") != Some(&Value::from("null")))
            .map(|s| type_text(root, s))
            .collect();
        return types.join(" or ");
    }
    let named = ref_name(schema);
    let schema = resolve(root, schema);
    if let Some(values) = enum_values(schema) {
        let values: Vec<String> = values.iter().map(|v| format!("`{v}`")).collect();
        return format!("one of {}", values.join(", "));
    }
    if let Some(name) = named
        && schema.get("properties").is_some()
    {
        return format!("`{name}`");
    }
    let kind = match schema.get("type") {
        Some(Value::Array(kinds)) => kinds
            .iter()
            .find(|k| *k != "null")
            .and_then(Value::as_str)
            .unwrap_or("any"),
        Some(Value::String(kind)) => kind,
        _ => "any",
    };
    match (kind, schema.get("format").and_then(Value::as_str)) {
        ("string", Some("ip")) => "IP address".to_owned(),
        ("string", Some("date-time")) => "timestamp".to_owned(),
        ("array", _) => match schema.get("items") {
            Some(items) => match type_text(root, items) {
                plain if plain.chars().all(|c| c.is_ascii_lowercase()) => {
                    format!("list of {plain}s")
                }
                other => format!("list of {other}"),
            },
            None => "list".to_owned(),
        },
        ("object", _) => "table".to_owned(),
        (kind, _) => kind.to_owned(),
    }
}

/// A default value as it is written in the file: `30s`, `8090`, `none` for an empty list.
fn default_text(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "not set".to_owned(),
        Some(Value::String(s)) => format!("`{s}`"),
        Some(Value::Array(a)) if a.is_empty() => "none".to_owned(),
        Some(Value::Object(o)) if o.is_empty() => "none".to_owned(),
        Some(other) => format!("`{other}`"),
    }
}

/// The description of a schema, with an enum's values and theirs after it.
fn description(root: &Value, schema: &Value) -> String {
    let mut text = schema
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let target = resolve(root, schema);
    if text.is_empty() {
        target
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .clone_into(&mut text);
    }
    // The ways of writing a value that can be written in more than one.
    if let Some(variants) = target.get("anyOf").and_then(Value::as_array) {
        for variant in variants {
            if let Some(about) = variant.get("description").and_then(Value::as_str) {
                let _ = write!(text, " {about}");
            }
        }
    }
    if let Some(variants) = target.get("oneOf").and_then(Value::as_array) {
        for variant in variants {
            if let (Some(value), Some(about)) = (
                variant.get("const").and_then(Value::as_str),
                variant.get("description").and_then(Value::as_str),
            ) {
                let _ = write!(text, " `{value}`: {about}");
            }
        }
    }
    text
}

/// The configuration reference, from the configuration's JSON Schema
/// (`schedr_config::config_schema`): one section per table, one row per key with its type,
/// default, environment variable and doc comment.
#[must_use]
pub fn config_page(schema: &Value) -> String {
    let mut page = String::from(
        "# Configuration\n\n\
         Every key of the configuration file, generated from the code. A key is set in the \
         TOML file (`--config`, else `SCHEDR_CONFIG_FILE`, else `/etc/schedr/config.toml`) or \
         by the environment variable shown with it; flags win over the environment, which wins \
         over the file. Durations are written like `500ms`, `30s`, `5m`, `1h30m` or `2d`; sizes \
         like `512B`, `64KiB`, `4MiB` or `1GiB`; paths as a list, or in the environment as one \
         string separated by `:`. Unknown keys are an error. Secrets (a database \
         URL with a password) come only from the environment.\n",
    );
    sections(&mut page, schema, schema, &[]);
    page
}

/// Writes the sections of `table` (keys under `path`) and of the tables inside it.
fn sections(page: &mut String, root: &Value, table: &Value, path: &[&str]) {
    let Some(properties) = table.get("properties").and_then(Value::as_object) else {
        return;
    };
    let mut keys = Vec::new();
    let mut tables = Vec::new();
    for (key, property) in properties {
        let target = resolve(root, property);
        if ref_name(property).is_some() && target.get("properties").is_some() {
            tables.push((key, property, target));
        } else if path.is_empty() && property.get("additionalProperties").is_some() {
            // A top-level table of names, e.g. `[interpreters]`: a section of its own.
            named_section(page, root, key, property);
        } else {
            keys.push((key, property));
        }
    }
    if !keys.is_empty() {
        let _ = write!(
            page,
            "\n| Key | Type | Default | Description |\n|---|---|---|---|\n"
        );
        for (key, property) in keys {
            let env = env_var(path, key, &type_text(root, property));
            let _ = writeln!(
                page,
                "| `{key}` | {} | {} | {} Environment: `{env}`. |",
                cell(&type_text(root, property)),
                cell(&default_text(property.get("default"))),
                cell(&description(root, property)),
            );
        }
    }
    for (key, property, target) in tables {
        let mut inner = path.to_vec();
        inner.push(key);
        let _ = write!(
            page,
            "\n{} `[{}]`\n\n{}\n",
            "#".repeat(inner.len() + 1),
            inner.join("."),
            section_text(&description(root, property), &description(root, target))
        );
        sections(page, root, target, &inner);
    }
}

/// A top-level table whose keys are names, e.g. `[interpreters]`.
fn named_section(page: &mut String, root: &Value, key: &str, property: &Value) {
    let entry = property.get("additionalProperties").unwrap_or(&Value::Null);
    let _ = write!(
        page,
        "\n## `[{key}]`\n\n{}\n\n| Key | Type | Description |\n|---|---|---|\n",
        section_text(&description(root, property), "")
    );
    let _ = writeln!(
        page,
        "| any name | {} | {} Environment: `{}` (the plain path form). |",
        cell(&type_text(root, entry)),
        cell(&description(root, entry)),
        env_var(&[key], "<NAME>", "")
    );
}

/// A section's text: its description without the leading `` `[name]` `` (the heading says
/// it), else `fallback`'s; capitalized.
fn section_text(description: &str, fallback: &str) -> String {
    let strip = |text: &str| -> String {
        let rest = match text.strip_prefix("`[") {
            Some(rest) => rest.split_once("]`").map_or(text, |(_, after)| after),
            None => text,
        };
        let rest = rest.trim_start_matches([':', '.', ' ']);
        let mut chars = rest.chars();
        chars.next().map_or_else(String::new, |first| {
            first.to_uppercase().chain(chars).collect()
        })
    };
    let text = strip(description);
    if text.is_empty() {
        strip(fallback)
    } else {
        text
    }
}

/// The environment variable of the key `key` in the table at `path`; a table of names takes
/// the name as a last part.
fn env_var(path: &[&str], key: &str, kind: &str) -> String {
    let mut parts: Vec<String> = path.iter().map(|p| p.to_uppercase()).collect();
    parts.push(if key.starts_with('<') {
        key.to_owned()
    } else {
        key.to_uppercase()
    });
    if kind == "table" {
        parts.push("<NAME>".to_owned());
    }
    format!("{ENV_PREFIX}{}", parts.join("__"))
}

/// The metrics reference: one table per role.
#[must_use]
pub fn metrics_page(groups: &[MetricGroup<'_>]) -> String {
    let mut page = String::from(
        "# Metrics\n\n\
         Every metric on the ops port's `/metrics` (`[ops] port`, 8090), generated from the \
         code. Counters are shown with `_total`, histograms with `_bucket`, `_sum` and \
         `_count`. Labels are bounded: never a run, task or attempt ID.\n",
    );
    for group in groups {
        let _ = write!(
            page,
            "\n## {}\n\n| Metric | Type | Labels | Description |\n|---|---|---|---|\n",
            group.role
        );
        for metric in group.metrics {
            let labels: Vec<String> = metric.labels.iter().map(|l| format!("`{l}`")).collect();
            let _ = writeln!(
                page,
                "| `{}` | {} | {} | {} |",
                metric.exposed(group.prefix),
                metric.kind,
                if labels.is_empty() {
                    "none".to_owned()
                } else {
                    labels.join(", ")
                },
                cell(metric.help)
            );
        }
    }
    page
}

/// The command-line reference: every command (hidden ones left out) with its usage and
/// options, from the clap definition.
#[must_use]
pub fn cli_page(command: &clap::Command) -> String {
    let mut command = command.clone();
    command.build();
    let mut page = String::from(
        "# Command line\n\n\
         Every `schedr` command, generated from the code (`schedr <command> --help` shows the \
         same).\n",
    );
    let globals: Vec<&clap::Arg> = command
        .get_arguments()
        .filter(|a| a.is_global_set() && !a.is_hide_set())
        .collect();
    if !globals.is_empty() {
        page.push_str("\n## Options of every command\n");
        argument_table(&mut page, &globals);
    }
    command_section(&mut page, &command, &[]);
    page
}

/// Writes `command`'s section (its path from the root is `parents`) and its subcommands'.
fn command_section(page: &mut String, command: &clap::Command, parents: &[&str]) {
    let mut path = parents.to_vec();
    path.push(command.get_name());
    if !parents.is_empty() {
        let _ = write!(page, "\n## `{}`\n\n", path.join(" "));
        let about = command
            .get_long_about()
            .or_else(|| command.get_about())
            .map(ToString::to_string)
            .unwrap_or_default();
        let _ = writeln!(page, "{about}\n");
        let usage = command.clone().render_usage().to_string();
        let usage = usage.trim_start_matches("Usage: ");
        let _ = writeln!(page, "```text\n{usage}\n```");
        arguments(page, command);
    }
    for sub in command.get_subcommands() {
        // clap's own `help` subcommand repeats what `--help` says.
        if !sub.is_hide_set() && sub.get_name() != "help" {
            command_section(page, sub, &path);
        }
    }
}

/// The table of `command`'s own arguments (global ones are on the root's `--help`).
fn arguments(page: &mut String, command: &clap::Command) {
    let args: Vec<&clap::Arg> = command
        .get_arguments()
        .filter(|a| !a.is_hide_set() && !a.is_global_set())
        .filter(|a| !matches!(a.get_id().as_str(), "help" | "version"))
        .collect();
    argument_table(page, &args);
}

/// A table of arguments: how each is written, and what it does.
fn argument_table(page: &mut String, args: &[&clap::Arg]) {
    if args.is_empty() {
        return;
    }
    page.push_str("\n| Argument | Description |\n|---|---|\n");
    for arg in args {
        let values: Vec<String> = arg
            .get_value_names()
            .unwrap_or_default()
            .iter()
            .map(|v| format!("<{v}>"))
            .collect();
        let value = if arg.get_action().takes_values() {
            values.join(" ")
        } else {
            String::new()
        };
        let takes_value = !value.is_empty();
        let flag = match (arg.get_long(), arg.get_short()) {
            (Some(long), Some(short)) => Some(format!("-{short}, --{long}")),
            (Some(long), None) => Some(format!("--{long}")),
            (None, Some(short)) => Some(format!("-{short}")),
            (None, None) => None,
        };
        let name = match flag {
            Some(flag) if value.is_empty() => flag,
            Some(flag) => format!("{flag} {value}"),
            None if value.is_empty() => format!("<{}>", arg.get_id().as_str().to_uppercase()),
            None => value,
        };
        let mut text = arg
            .get_long_help()
            .or_else(|| arg.get_help())
            .map(ToString::to_string)
            .unwrap_or_default();
        let defaults: Vec<String> = arg
            .get_default_values()
            .iter()
            .map(|v| v.to_string_lossy().into_owned())
            .collect();
        // A flag's default (`false`) says nothing.
        if !defaults.is_empty() && takes_value {
            let _ = write!(text, " Default: `{}`.", defaults.join(","));
        }
        if let Some(env) = arg.get_env() {
            let _ = write!(text, " Environment: `{}`.", env.to_string_lossy());
        }
        let _ = writeln!(page, "| `{name}` | {} |", cell(&text));
    }
}

/// The REST API reference, from its OpenAPI description: each operation with its
/// parameters and answers, then each schema with its fields.
#[must_use]
pub fn rest_page(openapi: &Value) -> String {
    let mut page = String::from(
        "# REST API\n\n\
         The webserver's API (`[webserver] port`, 8080), generated from its OpenAPI \
         description (`schema/openapi.json`). The `schedr` CLI uses it. Actions answer `202` \
         with a command: the master applies it, and `GET /api/v1/commands/{command_id}` shows \
         its decision.\n\n## Operations\n",
    );
    if let Some(paths) = openapi.get("paths").and_then(Value::as_object) {
        for (path, operations) in paths {
            let Some(operations) = operations.as_object() else {
                continue;
            };
            // A path item's other keys (`parameters`, `summary`, …) aren't operations.
            for (method, operation) in operations
                .iter()
                .filter(|(key, _)| HTTP_METHODS.contains(&key.as_str()))
            {
                operation_section(&mut page, openapi, &method.to_uppercase(), path, operation);
            }
        }
    }
    page.push_str("\n## Schemas\n");
    if let Some(schemas) = openapi
        .pointer("/components/schemas")
        .and_then(Value::as_object)
    {
        for (name, schema) in schemas {
            schema_section(&mut page, openapi, name, schema);
        }
    }
    page
}

/// Writes one operation.
fn operation_section(page: &mut String, root: &Value, method: &str, path: &str, op: &Value) {
    let summary = op
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let _ = write!(page, "\n### `{method} {path}`\n\n{}\n", cell(summary));
    if let Some(params) = op.get("parameters").and_then(Value::as_array) {
        page.push_str("\n| Parameter | In | Type | Description |\n|---|---|---|---|\n");
        for param in params {
            let name = param
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let place = param.get("in").and_then(Value::as_str).unwrap_or_default();
            let required = param.get("required") == Some(&Value::Bool(true));
            let about = param
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let _ = writeln!(
                page,
                "| `{name}` | {place}{} | {} | {} |",
                if required { "" } else { " (optional)" },
                cell(
                    &param
                        .get("schema")
                        .map_or_else(String::new, |s| type_text(root, s))
                ),
                cell(about)
            );
        }
    }
    if let Some(body) = op.pointer("/requestBody/content/application~1json/schema") {
        let _ = write!(page, "\nBody: {}.\n", type_text(root, body));
    }
    if let Some(responses) = op.get("responses").and_then(Value::as_object) {
        let answers: Vec<String> = responses
            .iter()
            .map(
                |(status, response)| match response.pointer("/content/application~1json/schema") {
                    Some(schema) => format!("`{status}` {}", type_text(root, schema)),
                    None => format!("`{status}`"),
                },
            )
            .collect();
        let _ = write!(page, "\nAnswers: {}.\n", answers.join(", "));
    }
}

/// Writes one schema: its description and a table of its fields.
fn schema_section(page: &mut String, root: &Value, name: &str, schema: &Value) {
    let _ = write!(
        page,
        "\n### `{name}`\n\n{}\n",
        cell(&description(root, schema))
    );
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return;
    };
    let required: Vec<&str> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|r| r.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    page.push_str("\n| Field | Type | Description |\n|---|---|---|\n");
    for (field, property) in properties {
        let optional = if required.contains(&field.as_str()) {
            ""
        } else {
            " (optional)"
        };
        let _ = writeln!(
            page,
            "| `{field}`{optional} | {} | {} |",
            cell(&type_text(root, property)),
            cell(&description(root, property))
        );
    }
}

/// The reference pages, with paths relative to the repository root (under
/// [`crate::docs::DOCS_GENERATED_DIR`]`/reference`).
#[must_use]
pub fn reference_sources(
    config_schema: &Value,
    cli: &clap::Command,
    metrics: &[MetricGroup<'_>],
    openapi: &Value,
) -> Vec<crate::GeneratedFile> {
    let dir = std::path::Path::new(crate::docs::DOCS_GENERATED_DIR).join("reference");
    [
        ("configuration.md", config_page(config_schema)),
        ("cli.md", cli_page(cli)),
        ("metrics.md", metrics_page(metrics)),
        ("rest-api.md", rest_page(openapi)),
    ]
    .into_iter()
    .map(|(name, contents)| crate::GeneratedFile {
        path: dir.join(name),
        contents,
    })
    .collect()
}
