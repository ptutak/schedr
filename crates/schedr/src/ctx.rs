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

use crate::cli::{ConnectionArgs, DagCtxCommand, GetCommand, SecretCommand};

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
        check_prefix(prefix)?;
    }
    let mut stream = connect(env, Ctx::Task)?;
    let request = CtxRequest::ConnectionGet { name: name.clone() };
    let values = match call(&mut stream, version, &request)? {
        CtxAnswer::Connection { values } => Value::Object(values.into_iter().collect()),
        CtxAnswer::ConnectionFailed { kind, reason } => return Err(format!("{reason} ({kind})")),
        CtxAnswer::Error { reason } | CtxAnswer::Refused { reason } => return Err(reason),
        _ => return Err("the agent answered with something else than the connection".to_owned()),
    };
    let shown = schedr_model::params::shown(&values);
    let text = match prefix {
        None => format!("{shown}\n"),
        Some(prefix) => exports(prefix, &shown)?,
    };
    out.write_all(text.as_bytes())
        .map_err(|e| format!("can't write output: {e}"))
}

/// A `--env` prefix: the start of an environment variable's name (`MONGO`, `DB_1`).
fn check_prefix(prefix: &str) -> Result<(), String> {
    let mut chars = prefix.chars();
    let starts = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    if starts && chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(())
    } else {
        Err(format!(
            "--env `{prefix}`: a prefix is letters, digits and `_`, not starting with a digit"
        ))
    }
}

/// `export PREFIX_PARAM='value'` lines, one per parameter, in order: text as it is, anything
/// else as JSON, single-quoted for the shell (a `'` written `'\''`).
///
/// # Errors
///
/// A parameter whose name isn't one a shell takes (`région`), or two that become one
/// variable (`host` and `HOST`).
fn exports(prefix: &str, values: &Value) -> Result<String, String> {
    let mut lines = String::new();
    let mut named: BTreeMap<String, &str> = BTreeMap::new();
    for (param, value) in values.as_object().into_iter().flatten() {
        if !param.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(format!(
                "--env: parameter `{param}` can't be a shell variable's name (letters, digits \
                 and `_` only); read it with --json"
            ));
        }
        let variable = format!("{prefix}_{}", param.to_ascii_uppercase());
        if let Some(other) = named.insert(variable.clone(), param) {
            return Err(format!(
                "--env: parameters `{other}` and `{param}` would both be `{variable}`; read them \
                 with --json"
            ));
        }
        let text = match value {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        let quoted = text.replace('\'', "'\\''");
        let _ = writeln!(lines, "export {variable}='{quoted}'");
    }
    Ok(lines)
}

/// The values an answer holds, as printed: params in the JSON form `--params` takes.
fn values(answer: CtxAnswer) -> Result<Value, String> {
    match answer {
        CtxAnswer::Params { params } => Ok(schedr_model::params::shown(&params)),
        CtxAnswer::Run(run) => serde_json::to_value(run).map_err(|e| e.to_string()),
        CtxAnswer::Error { reason } | CtxAnswer::Refused { reason } => Err(reason),
        CtxAnswer::Welcome => Err("the agent answered `welcome` to a request".to_owned()),
        // Never printed in a message: it may hold a value.
        CtxAnswer::Secret { .. }
        | CtxAnswer::SecretFailed { .. }
        | CtxAnswer::Connection { .. }
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
        CtxAnswer::Secret { .. } | CtxAnswer::Connection { .. } => {
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
