//! `schedr dag-ctx`: a command task's processes reading the run's parameters and information
//! from the agent, over the task's context channel (decisions 143-147, see
//! `schedr_model::ctx`).
//!
//! Blocking and without the configuration: it runs inside a task, which may have none, and
//! is one request and one answer.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{self, IoSlice, Read, Write};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use rustix::event::{PollFd, PollFlags};
use rustix::net::{SendAncillaryBuffer, SendAncillaryMessage, SendFlags};
use schedr_fd::{Inherited, inherited_datagram};
use schedr_model::ctx::{CTX_FD_ENV, CTX_PROTOCOL, CtxAnswer, CtxRequest, MAX_CTX_MESSAGE_SIZE};
use serde_json::Value;

use schedr_model::SecretName;
use schedr_model::runner::CleanupHandle;

use crate::cli::{
    CleanupCommand, ConnectionArgs, DagCtxCommand, GetCommand, ResourceCommand, SecretCommand,
    TaskCtxCommand, TaskEventsCommand,
};

#[cfg(test)]
mod tests;

/// How long the agent has to answer.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(30);

/// Runs a `schedr dag-ctx` command; the error is the message for stderr.
///
/// # Errors
///
/// Outside a task, when the channel is gone, or when the agent refuses the call or the field
/// asked for isn't there.
pub fn run_dag_ctx(
    command: &DagCtxCommand,
    env: &BTreeMap<String, String>,
    version: &str,
    out: &mut impl Write,
) -> Result<(), String> {
    let (request, GetCommand::Get { field }) = match command {
        DagCtxCommand::Params { command } => (CtxRequest::ParamsGet, command),
        DagCtxCommand::Run { command } => (CtxRequest::RunGet, command),
    };
    let mut stream = connect(env, Ctx::Dag)?;
    let answer = call(&mut stream, version, &request)?;
    let values = values(answer)?;
    let text = printed(&values, field.field.as_deref())?;
    out.write_all(text.as_bytes())
        .map_err(|e| format!("can't write output: {e}"))
}

/// Runs a `schedr task-ctx` command; the error is the message for stderr.
///
/// # Errors
///
/// As the command's own (`run_task_ctx_*`).
pub fn run_task_ctx(
    command: &TaskCtxCommand,
    env: &BTreeMap<String, String>,
    version: &str,
    (out, err): (&mut impl Write, &mut impl Write),
) -> Result<(), String> {
    match command {
        TaskCtxCommand::Secret { command } => run_task_ctx_secret(command, env, version, out),
        TaskCtxCommand::Connection(command) => run_task_ctx_connection(command, env, version, out),
        TaskCtxCommand::Cleanup { command } => run_task_ctx_cleanup(command, env, version, out),
        TaskCtxCommand::Resource { command } => run_task_ctx_resource(command, env, version, out),
        TaskCtxCommand::Api {
            method,
            path,
            data,
            idempotency_key,
        } => {
            let request = (
                method.as_str(),
                path.as_str(),
                data.as_deref(),
                idempotency_key.as_deref(),
            );
            run_task_ctx_api(request, env, version, (out, err))
        }
        TaskCtxCommand::Events { command } => {
            run_task_ctx_events(command, env, version, (out, err))
        }
        TaskCtxCommand::Notify {
            channel,
            text,
            data,
        } => {
            let data = crate::user::given_event_data(data.as_deref())
                .map_err(|why| why.replace("--event-data", "--data"))?;
            let request = CtxRequest::ApiCall {
                method: "POST".to_owned(),
                path: "/api/v1/notifications".to_owned(),
                body: Some(serde_json::json!({
                    "channel": channel,
                    "text": text,
                    "data": data.unwrap_or_default(),
                })),
                idempotency_key: None,
            };
            api_call(env, version, &request, err).map(|_| ())
        }
    }
}

/// Which command connects, for the message outside a task.
#[derive(Debug, Clone, Copy)]
enum Ctx {
    Dag,
    Task,
}

/// Runs `schedr task-ctx secret get`: prints a secret the task declares (a key's value as
/// it is, with no newline; a whole secret as one line of JSON); the error is the message for
/// stderr, with the error kind.
///
/// # Errors
///
/// Outside a task, when the channel is gone, or when the secret can't be read (not declared,
/// not found, its storage down).
pub fn run_task_ctx_secret(
    command: &SecretCommand,
    env: &BTreeMap<String, String>,
    version: &str,
    out: &mut impl Write,
) -> Result<(), String> {
    let SecretCommand::Get { secret, storage } = command;
    let (path, key) = match secret.split_once('#') {
        Some((path, key)) => (path, Some(key)),
        None => (secret.as_str(), None),
    };
    let name = SecretName {
        storage: storage.clone(),
        path: path.to_owned(),
        key: key.map(str::to_owned),
        default: None,
    };
    name.check()?;
    let mut stream = connect(env, Ctx::Task)?;
    let request = CtxRequest::SecretGet { secret: name };
    let text = match call(&mut stream, version, &request)? {
        CtxAnswer::Secret {
            value: Value::String(text),
        } => text,
        CtxAnswer::Secret { value } => value.to_string(),
        CtxAnswer::SecretFailed { kind, reason } => return Err(format!("{reason} ({kind})")),
        CtxAnswer::Error { reason } | CtxAnswer::Refused { reason } => return Err(reason),
        _ => return Err("the agent answered with something else than the secret".to_owned()),
    };
    out.write_all(text.as_bytes())
        .map_err(|e| format!("can't write output: {e}"))
}

/// Runs `schedr task-ctx connection NAME`: prints a connection's values, its secrets filled
/// in (decision 174), as one line of JSON in the form `--params` takes, or with `--env
/// PREFIX` as `export PREFIX_PARAM='value'` lines for `eval` (each value quoted for the
/// shell, so nothing in one runs); the error is the message for stderr, with the error kind.
///
/// # Errors
///
/// Outside a task, when the channel is gone, when the prefix isn't a variable name's start,
/// or when the connection can't be opened (not declared, not in the DAG folder, a secret it
/// names unreadable).
pub fn run_task_ctx_connection(
    command: &ConnectionArgs,
    env: &BTreeMap<String, String>,
    version: &str,
    out: &mut impl Write,
) -> Result<(), String> {
    let ConnectionArgs {
        name, env: prefix, ..
    } = command;
    if let Some(prefix) = prefix {
        schedr_model::connection::check_prefix(prefix).map_err(|e| format!("--env {e}"))?;
    }
    let mut stream = connect(env, Ctx::Task)?;
    let request = match prefix {
        None => CtxRequest::ConnectionGet { name: name.clone() },
        Some(prefix) => CtxRequest::ConnectionEnvGet {
            name: name.clone(),
            prefix: prefix.clone(),
        },
    };
    let text = match call(&mut stream, version, &request)? {
        CtxAnswer::Connection { values } => {
            let shown = schedr_model::params::shown(&Value::Object(values.into_iter().collect()));
            format!("{shown}\n")
        }
        CtxAnswer::ConnectionEnv { variables } => exports(&variables),
        CtxAnswer::ConnectionFailed { kind, reason } => return Err(format!("{reason} ({kind})")),
        CtxAnswer::Error { reason } | CtxAnswer::Refused { reason } => return Err(reason),
        _ => return Err("the agent answered with something else than the connection".to_owned()),
    };
    out.write_all(text.as_bytes())
        .map_err(|e| format!("can't write output: {e}"))
}

/// Runs `schedr task-ctx cleanup add|list`: registers a handle before the work it cleans up
/// after, or, in a cleanup, prints the attempt's handles in registration order: one line of
/// JSON each, or with `--field` one field's raw value each; the error is the message for
/// stderr.
///
/// # Errors
///
/// Outside a task, when the channel is gone, when a field or `--json` can't be read, when the
/// agent refuses the handle (no cleanup declared, a type not declared, too large, already in
/// the cleanup) or the list (not in a cleanup), or when a handle lacks the field asked for.
pub fn run_task_ctx_cleanup(
    command: &CleanupCommand,
    env: &BTreeMap<String, String>,
    version: &str,
    out: &mut impl Write,
) -> Result<(), String> {
    match command {
        CleanupCommand::Add {
            handle_type,
            fields,
            json,
        } => {
            let value = handle_value(fields, json.as_deref())?;
            let mut stream = connect(env, Ctx::Task)?;
            let request = CtxRequest::HandleAdd {
                handle: CleanupHandle {
                    handle_type: handle_type.clone(),
                    value,
                },
            };
            match call(&mut stream, version, &request)? {
                CtxAnswer::HandleAdded => Ok(()),
                CtxAnswer::HandleRefused { reason }
                | CtxAnswer::Error { reason }
                | CtxAnswer::Refused { reason } => Err(reason),
                _ => Err("the agent answered with something else than the handle".to_owned()),
            }
        }
        CleanupCommand::List { handle_type, field } => {
            let mut stream = connect(env, Ctx::Task)?;
            let handles = match call(&mut stream, version, &CtxRequest::HandleList)? {
                CtxAnswer::Handles { handles } => handles,
                CtxAnswer::Error { reason } | CtxAnswer::Refused { reason } => return Err(reason),
                _ => return Err("the agent answered with something else than handles".to_owned()),
            };
            let text = listed(&handles, handle_type.as_deref(), field.field.as_deref())?;
            out.write_all(text.as_bytes())
                .map_err(|e| format!("can't write output: {e}"))
        }
    }
}

/// `schedr task-ctx resource set|get`: reports the value of the resource a `CommandResource`
/// setup creates (checked against its type at once), or prints the value a resource's
/// `cleanup_command` cleans up (as `--params` takes it: dates as text, tuples as lists).
///
/// # Errors
///
/// Outside a task, when the channel is gone, when a field or `--json` can't be read, or when
/// the agent refuses: `set` outside a setup, twice, or with a value of another type; `get`
/// outside a cleanup; a missing `--field`.
pub fn run_task_ctx_resource(
    command: &ResourceCommand,
    env: &BTreeMap<String, String>,
    version: &str,
    out: &mut impl Write,
) -> Result<(), String> {
    match command {
        ResourceCommand::Set { fields, json } => {
            let value = resource_value(fields, json.as_deref())?;
            let mut stream = connect(env, Ctx::Task)?;
            match call(&mut stream, version, &CtxRequest::ResourceSet { value })? {
                CtxAnswer::ResourceReported => Ok(()),
                CtxAnswer::ResourceRefused { reason }
                | CtxAnswer::Error { reason }
                | CtxAnswer::Refused { reason } => Err(reason),
                _ => Err("the agent answered with something else than the resource".to_owned()),
            }
        }
        ResourceCommand::Get { field } => {
            let mut stream = connect(env, Ctx::Task)?;
            let value = match call(&mut stream, version, &CtxRequest::ResourceGet)? {
                CtxAnswer::Resource { value } => schedr_model::params::shown(&value),
                CtxAnswer::ResourceRefused { reason }
                | CtxAnswer::Error { reason }
                | CtxAnswer::Refused { reason } => return Err(reason),
                _ => {
                    return Err(
                        "the agent answered with something else than the resource".to_owned()
                    );
                }
            };
            let text = printed(&value, field.field.as_deref())?;
            out.write_all(text.as_bytes())
                .map_err(|e| format!("can't write output: {e}"))
        }
    }
}

/// Runs `schedr task-ctx api`: prints the answer's body as one line of JSON.
///
/// # Errors
///
/// Outside a task, when `--data` isn't JSON, when the channel is gone, or when the API answers
/// an error status (its message).
pub fn run_task_ctx_api(
    (method, path, data, idempotency_key): (&str, &str, Option<&str>, Option<&str>),
    env: &BTreeMap<String, String>,
    version: &str,
    (out, err): (&mut impl Write, &mut impl Write),
) -> Result<(), String> {
    let body = data
        .map(|text| serde_json::from_str(text).map_err(|e| format!("--data isn't JSON: {e}")))
        .transpose()?;
    let request = CtxRequest::ApiCall {
        method: method.to_owned(),
        path: path.to_owned(),
        body,
        idempotency_key: idempotency_key.map(str::to_owned),
    };
    let answer = api_call(env, version, &request, err)?;
    out.write_all(format!("{answer}\n").as_bytes())
        .map_err(|e| format!("can't write output: {e}"))
}

/// Runs `schedr task-ctx events`: the event posted, or those listed, one line of JSON each.
///
/// # Errors
///
/// Outside a task, when `--data` or `--at` doesn't read, when the channel is gone, or when
/// the API refuses (its message).
pub fn run_task_ctx_events(
    command: &TaskEventsCommand,
    env: &BTreeMap<String, String>,
    version: &str,
    (out, err): (&mut impl Write, &mut impl Write),
) -> Result<(), String> {
    let request = match command {
        TaskEventsCommand::Post {
            name,
            key,
            data,
            at,
            idempotency_key,
        } => {
            let data = crate::user::given_event_data(data.as_deref())
                .map_err(|why| why.replace("--event-data", "--data"))?;
            let mut body = serde_json::json!({
                "name": name,
                "key": key,
                "data": data.unwrap_or_default(),
            });
            if let Some(text) = at {
                let parsed = crate::times::parse_for(text).map_err(|why| format!("--at: {why}"))?;
                if let Some(warning) = parsed.warning {
                    let _ = writeln!(err, "{warning}");
                }
                body["at"] = Value::String(parsed.at.to_string());
            }
            CtxRequest::ApiCall {
                method: "POST".to_owned(),
                path: "/api/v1/events".to_owned(),
                body: Some(body),
                idempotency_key: idempotency_key.clone(),
            }
        }
        TaskEventsCommand::List { name, limit } => {
            let mut path = format!("/api/v1/events?limit={limit}");
            if let Some(name) = name {
                let name =
                    schedr_model::EventName::new(name).map_err(|e| format!("--name: {e}"))?;
                // Encoded: a name may hold `#` or `/` (Grok's review: `#` cut the query).
                let _ = write!(path, "&name={}", query_value(name.as_str()));
            }
            CtxRequest::ApiCall {
                method: "GET".to_owned(),
                path,
                body: None,
                idempotency_key: None,
            }
        }
    };
    let answer = api_call(env, version, &request, err)?;
    let text = match answer.get("events").and_then(Value::as_array) {
        Some(events) if matches!(command, TaskEventsCommand::List { .. }) => {
            events.iter().fold(String::new(), |mut text, event| {
                let _ = writeln!(text, "{event}");
                text
            })
        }
        _ => format!("{answer}\n"),
    };
    out.write_all(text.as_bytes())
        .map_err(|e| format!("can't write output: {e}"))
}

/// `text` as a query's value: all but unreserved characters percent-encoded.
fn query_value(text: &str) -> String {
    text.bytes().fold(String::new(), |mut encoded, byte| {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
        encoded
    })
}

/// An API call through the task's agent: the answer's body, or its error as the message.
fn api_call(
    env: &BTreeMap<String, String>,
    version: &str,
    request: &CtxRequest,
    err: &mut impl Write,
) -> Result<Value, String> {
    let mut stream = connect(env, Ctx::Task)?;
    // The agent waits for the master up to its limit, over reconnects: the answer comes then.
    stream
        .set_read_timeout(Some(
            schedr_agent::api::API_CALL_LIMIT.saturating_add(ANSWER_TIMEOUT),
        ))
        .map_err(|e| format!("can't set up the connection: {e}"))?;
    let answer = call(&mut stream, version, request)?;
    if let CtxAnswer::ApiAnswer {
        note: Some(note), ..
    } = &answer
    {
        let _ = writeln!(err, "{note}");
    }
    match answer {
        CtxAnswer::ApiAnswer { status, body, .. } if status < 400 => Ok(body),
        CtxAnswer::ApiAnswer { status, body, .. } => {
            let why = body
                .get("error")
                .and_then(Value::as_str)
                .map_or_else(|| body.to_string(), str::to_owned);
            Err(format!("the API answered {status}: {why}"))
        }
        CtxAnswer::Error { reason } | CtxAnswer::Refused { reason } => Err(reason),
        _ => Err("the agent answered with something else than the API's answer".to_owned()),
    }
}

/// A resource's value: an object from `key=value` and `key:=json` fields, or any JSON from
/// `--json` (`--json 7` for `value=int`).
fn resource_value(fields: &[String], json: Option<&str>) -> Result<Value, String> {
    match json {
        Some(text) if fields.is_empty() => {
            serde_json::from_str(text).map_err(|e| format!("--json isn't JSON: {e}"))
        }
        Some(_) => Err("give the value as fields or as --json, not both".to_owned()),
        None => handle_value(fields, None),
    }
}

/// A handle's value from `key=value` and `key:=json` fields, or from `--json`.
fn handle_value(fields: &[String], json: Option<&str>) -> Result<Value, String> {
    if let Some(text) = json {
        if !fields.is_empty() {
            return Err("give the handle as fields or as --json, not both".to_owned());
        }
        let value: Value =
            serde_json::from_str(text).map_err(|e| format!("--json isn't JSON: {e}"))?;
        if !value.is_object() {
            return Err("--json must be one JSON object".to_owned());
        }
        return Ok(value);
    }
    let mut object = serde_json::Map::new();
    for field in fields {
        // A field is never quoted back: it may be a secret (B210).
        let Some((name, text)) = field.split_once('=') else {
            return Err("a field isn't `key=value` or `key:=json`".to_owned());
        };
        let (name, value) = match name.strip_suffix(':') {
            Some(name) => {
                let value = serde_json::from_str(text)
                    .map_err(|e| format!("the value of `{name}` isn't JSON: {e}"))?;
                (name, value)
            }
            None => (name, Value::String(text.to_owned())),
        };
        if name.is_empty() {
            return Err("a field has no key (`=value`)".to_owned());
        }
        if object.insert(name.to_owned(), value).is_some() {
            return Err(format!("the field `{name}` is given twice"));
        }
    }
    Ok(Value::Object(object))
}

/// The handles as printed, of one type if asked: each as a line of JSON, or its field's raw
/// value.
fn listed(
    handles: &[CleanupHandle],
    handle_type: Option<&str>,
    field: Option<&str>,
) -> Result<String, String> {
    let mut text = String::new();
    let wanted = handles
        .iter()
        .filter(|handle| handle_type.is_none_or(|name| name == handle.handle_type));
    for handle in wanted {
        if field.is_some() {
            text.push_str(&printed(&handle.value, field)?);
        } else {
            let line = serde_json::to_string(handle).map_err(|e| e.to_string())?;
            let _ = writeln!(text, "{line}");
        }
    }
    Ok(text)
}

/// `export NAME='value'` lines, one per variable, in order, single-quoted for the shell (a
/// `'` written `'\''`): the agent named them, its files written already.
fn exports(variables: &BTreeMap<String, String>) -> String {
    let mut lines = String::new();
    for (variable, text) in variables {
        let quoted = text.replace('\'', "'\\''");
        let _ = writeln!(lines, "export {variable}='{quoted}'");
    }
    lines
}

/// The values an answer holds, as printed: params in the JSON form `--params` takes.
fn values(answer: CtxAnswer) -> Result<Value, String> {
    match answer {
        CtxAnswer::Params { params } => Ok(schedr_model::params::shown(&params)),
        CtxAnswer::Run(run) => serde_json::to_value(run).map_err(|e| e.to_string()),
        CtxAnswer::Error { reason } | CtxAnswer::Refused { reason } => Err(reason),
        CtxAnswer::Welcome => Err("the agent answered `welcome` to a request".to_owned()),
        CtxAnswer::ApiAnswer { .. } => {
            Err("the agent answered with the API's answer to another request".to_owned())
        }
        CtxAnswer::HandleAdded | CtxAnswer::HandleRefused { .. } | CtxAnswer::Handles { .. } => {
            Err("the agent answered with a cleanup handle to another request".to_owned())
        }
        CtxAnswer::ResourceReported
        | CtxAnswer::Resource { .. }
        | CtxAnswer::ResourceRefused { .. } => {
            Err("the agent answered with a resource to another request".to_owned())
        }
        // Never printed in a message: it may hold a value.
        CtxAnswer::Secret { .. }
        | CtxAnswer::SecretFailed { .. }
        | CtxAnswer::Connection { .. }
        | CtxAnswer::ConnectionEnv { .. }
        | CtxAnswer::ConnectionFailed { .. } => {
            Err("the agent answered with a secret to another request".to_owned())
        }
    }
}

/// The output: the values as one line of JSON, or one field's raw value (text as it is,
/// anything else as JSON), each with a newline.
fn printed(values: &Value, field: Option<&str>) -> Result<String, String> {
    let Some(name) = field else {
        return Ok(format!("{values}\n"));
    };
    match values.get(name) {
        Some(Value::String(text)) => Ok(format!("{text}\n")),
        Some(value) => Ok(format!("{value}\n")),
        None => Err(format!("there is no field `{name}`")),
    }
}

/// A connection of this call's own: a fresh stream pair, one end sent to the agent over the
/// task's channel.
fn connect(env: &BTreeMap<String, String>, what: Ctx) -> Result<UnixStream, String> {
    let Some(text) = env.get(CTX_FD_ENV) else {
        let (command, python) = match what {
            Ctx::Dag => ("dag-ctx", "reads `dag_ctx`"),
            Ctx::Task => ("task-ctx", "reads `task_ctx`"),
        };
        return Err(format!(
            "`schedr {command}` must be run inside a schedr task: a command task's processes \
             can use it ({CTX_FD_ENV} isn't set here); a Python task {python}"
        ));
    };
    let fd: i32 = text
        .parse()
        .map_err(|_| format!("{CTX_FD_ENV}=`{text}` isn't a descriptor number"))?;
    let channel = inherited_datagram(fd).map_err(|e| channel_lost(fd, e))?;
    let (mine, theirs) = UnixStream::pair().map_err(|e| format!("can't make a connection: {e}"))?;
    send_end(&channel, &OwnedFd::from(theirs), ANSWER_TIMEOUT).map_err(|e| {
        match e.raw_os_error() {
            _ if e.kind() == io::ErrorKind::TimedOut => format!(
                "the task's agent didn't take the call within {}s",
                ANSWER_TIMEOUT.as_secs()
            ),
            Some(code) if code == rustix::io::Errno::CONNREFUSED.raw_os_error() => {
                "the task's agent no longer serves its context: the task has ended (a process it \
             left running can't read it)"
                    .to_owned()
            }
            _ => format!("can't reach the task's agent: {e}"),
        }
    })?;
    mine.set_read_timeout(Some(ANSWER_TIMEOUT))
        .and_then(|()| mine.set_write_timeout(Some(ANSWER_TIMEOUT)))
        .map_err(|e| format!("can't set up the connection: {e}"))?;
    Ok(mine)
}

/// Why the inherited channel can't be used, for the message.
fn channel_lost(fd: i32, error: Inherited) -> String {
    let closed = format!(
        "a parent process closed it (Python's `subprocess` closes inherited descriptors unless \
         given `pass_fds=[{fd}]`; in a Python script, `from schedr.script import dag_ctx` reads \
         the same values)"
    );
    match error {
        Inherited::Closed(_) => {
            format!("the task's context channel ({CTX_FD_ENV}={fd}) isn't open here: {closed}")
        }
        Inherited::NotChannel(_) => format!(
            "descriptor {fd} ({CTX_FD_ENV}) isn't the task's context channel any more: {closed}, \
             and the number was reused"
        ),
        other => format!("the task's context channel ({CTX_FD_ENV}={fd}): {other}"),
    }
}

/// Sends `end` over the channel in one datagram (`SCM_RIGHTS`), waiting for room in it until
/// `wait` ([`ANSWER_TIMEOUT`]): never blocking on the send itself (the inherited socket is shared with
/// every process of the task, and may block).
fn send_end(channel: &OwnedFd, end: &OwnedFd, wait: Duration) -> io::Result<()> {
    let deadline = Instant::now() + wait;
    let ends = [end.as_fd()];
    loop {
        let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
        let mut control = SendAncillaryBuffer::new(&mut space);
        if !control.push(SendAncillaryMessage::ScmRights(&ends)) {
            return Err(io::Error::other("no room for the descriptor"));
        }
        match rustix::net::sendmsg(channel, &[IoSlice::new(b"c")], &mut control, SEND_FLAGS) {
            Ok(_) => return Ok(()),
            Err(rustix::io::Errno::AGAIN | rustix::io::Errno::INTR) => {}
            Err(errno) => return Err(errno.into()),
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(io::ErrorKind::TimedOut.into());
        }
        let left = rustix::event::Timespec::try_from(left).map_err(io::Error::other)?;
        let mut room = [PollFd::new(channel, PollFlags::OUT)];
        match rustix::event::poll(&mut room, Some(&left)) {
            Ok(_) | Err(rustix::io::Errno::INTR) => {}
            Err(errno) => return Err(errno.into()),
        }
    }
}

/// The send doesn't wait (`MSG_DONTWAIT`), nor raise `SIGPIPE` (Linux's flag; elsewhere
/// Rust's runtime ignores it already).
#[cfg(target_os = "linux")]
const SEND_FLAGS: SendFlags = SendFlags::DONTWAIT.union(SendFlags::NOSIGNAL);
#[cfg(not(target_os = "linux"))]
const SEND_FLAGS: SendFlags = SendFlags::DONTWAIT;

/// The handshake, then `request`'s answer.
fn call(
    stream: &mut (impl Read + Write),
    version: &str,
    request: &CtxRequest,
) -> Result<CtxAnswer, String> {
    let hello = CtxRequest::Hello {
        protocol: CTX_PROTOCOL,
        schedr_version: version.to_owned(),
    };
    // An agent refusing the connection (too many open) says why before closing it, maybe
    // before the hello arrives: its answer is read even when the hello can't be written.
    let said = write_message(stream, &hello);
    let answer = match (read_answer(stream), said) {
        (Ok(answer), _) => answer,
        (Err(_), Err(not_said)) => return Err(not_said),
        (Err(not_read), Ok(())) => return Err(not_read),
    };
    match answer {
        CtxAnswer::Welcome => {}
        CtxAnswer::Refused { reason } | CtxAnswer::Error { reason } => return Err(reason),
        CtxAnswer::Secret { .. }
        | CtxAnswer::Connection { .. }
        | CtxAnswer::ConnectionEnv { .. } => {
            return Err("the agent answered hello with a secret".to_owned());
        }
        other => return Err(format!("the agent answered hello with {other:?}")),
    }
    write_message(stream, request)?;
    read_answer(stream)
}

fn lost(e: &io::Error) -> String {
    match e.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => format!(
            "the task's agent didn't answer within {}s",
            ANSWER_TIMEOUT.as_secs()
        ),
        io::ErrorKind::UnexpectedEof
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::BrokenPipe => {
            "the task's agent closed the connection without an answer (the task may have ended)"
                .to_owned()
        }
        _ => format!("talking to the task's agent: {e}"),
    }
}

fn write_message(stream: &mut impl Write, request: &CtxRequest) -> Result<(), String> {
    let body = serde_json::to_vec(request).map_err(|e| e.to_string())?;
    let size = u32::try_from(body.len()).map_err(|e| e.to_string())?;
    stream
        .write_all(&size.to_be_bytes())
        .and_then(|()| stream.write_all(&body))
        .map_err(|e| lost(&e))
}

fn read_answer(stream: &mut impl Read) -> Result<CtxAnswer, String> {
    let mut header = [0; 4];
    stream.read_exact(&mut header).map_err(|e| lost(&e))?;
    let size = u32::from_be_bytes(header);
    if size > MAX_CTX_MESSAGE_SIZE {
        return Err(format!(
            "the agent's answer of {size} bytes is over the limit of {MAX_CTX_MESSAGE_SIZE}"
        ));
    }
    let mut body = vec![0; size as usize];
    stream.read_exact(&mut body).map_err(|e| lost(&e))?;
    serde_json::from_slice(&body).map_err(|e| format!("the agent's answer doesn't read: {e}"))
}
