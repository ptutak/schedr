//! Write a schedr extension's plugin in Rust (decision 510).
//!
//! A plugin is a program a schedr worker runs for each task of its extension, as its own process
//! in a task slot. It speaks the runner protocol over the socket the worker hands it
//! (`SCHEDR_AGENT_FD`): a `hello` naming [`PROTOCOL`], then one `extension_task` request,
//! answered with `task_succeeded` or `task_failed`. [`main`] is that loop; a plugin gives it a
//! [`Plugin`]:
//!
//! ```no_run
//! use schedr_extension::{Channel, Plugin, Request, Value, json};
//!
//! struct Spark;
//!
//! impl Plugin for Spark {
//!     fn name(&self) -> &str {
//!         "spark"
//!     }
//!
//!     fn versions(&self) -> Vec<String> {
//!         vec!["1.0".to_owned()]
//!     }
//!
//!     fn run(&mut self, request: &Request, channel: &mut Channel) -> Result<Value, String> {
//!         let token = channel.secret("spark/api", Some("token"), None)?;
//!         channel.add_handle("job", json!({"id": 7}))?;
//!         println!("submitting {} with a token of {} bytes", request.payload, token.to_string().len());
//!         Ok(json!({"rows": 3}))
//!     }
//! }
//!
//! fn main() -> std::process::ExitCode {
//!     schedr_extension::main(Spark)
//! }
//! ```
//!
//! What it prints goes to the attempt's log; what `run` returns is the task's output; an error
//! fails the attempt. A cancel or timeout ends the process (`SIGTERM`, then `SIGKILL` after the
//! grace). With `ExtensionTask(..., cleanup=True)`, the program is run again after each attempt,
//! whatever its end, and [`Plugin::cleanup`] is called with the handles it registered.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::process::ExitCode;

pub mod protocol;
pub mod socket;

pub use protocol::{
    ErrorSummary, ExtensionTask, Handle, MAX_MESSAGE_SIZE, PROTOCOL, PluginMessage, SecretName,
    WorkerMessage,
};
pub use serde_json::{Value, json};

#[cfg(test)]
mod tests;

/// The environment variable naming the worker's socket.
pub const FD_ENV: &str = "SCHEDR_AGENT_FD";

/// What went wrong talking to the worker.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The socket failed.
    #[error("the worker's channel: {0}")]
    Io(#[from] std::io::Error),
    /// A message wasn't JSON of the protocol.
    #[error("the worker's channel: {0}")]
    Json(#[from] serde_json::Error),
    /// A message is over [`MAX_MESSAGE_SIZE`].
    #[error("a message of {0} bytes is over the protocol's limit")]
    TooLarge(usize),
    /// Not the message the protocol has next.
    #[error("expected `{expected}`, got `{got}`")]
    Unexpected {
        /// What was expected.
        expected: &'static str,
        /// What came.
        got: String,
    },
    /// The worker refused the plugin, or what it asked (a secret, a handle).
    #[error("{0}")]
    Refused(String),
}

impl From<Error> for String {
    fn from(error: Error) -> Self {
        error.to_string()
    }
}

/// What the worker asks the plugin to do.
#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    /// The extension's name.
    pub name: String,
    /// The version the task is written for.
    pub version: String,
    /// What the task's `payload=` says.
    pub payload: Value,
    /// For a cleanup, the handles the attempt registered, in order.
    pub handles: Vec<Handle>,
    /// Whether this is a cleanup, not the task.
    pub cleanup: bool,
    /// The whole request, as sent: the task, its run, the secrets it brought.
    pub raw: ExtensionTask,
}

impl Request {
    /// The request in `raw`.
    ///
    /// # Errors
    ///
    /// [`Error::Unexpected`] for a task that isn't an extension's.
    pub fn of(raw: ExtensionTask) -> Result<Self, Error> {
        if raw.task.kind != "extension" {
            return Err(Error::Unexpected {
                expected: "an extension task",
                got: raw.task.kind,
            });
        }
        Ok(Self {
            name: raw.task.name.clone(),
            version: raw.task.version.clone(),
            payload: raw.task.payload.clone(),
            handles: raw.cleanup.clone().unwrap_or_default(),
            cleanup: raw.cleanup.is_some(),
            raw,
        })
    }
}

/// An extension's plugin.
pub trait Plugin {
    /// The extension it runs.
    fn name(&self) -> &str;

    /// The versions it serves.
    fn versions(&self) -> Vec<String>;

    /// The task: returns its output (JSON), or why it failed.
    ///
    /// # Errors
    ///
    /// Why the attempt failed, in its log and its error.
    fn run(&mut self, request: &Request, channel: &mut Channel) -> Result<Value, String>;

    /// The cleanup, given the handles the attempt registered (`request.handles`). By default
    /// the extension has none, and a cleanup request fails.
    ///
    /// # Errors
    ///
    /// Why the cleanup failed: it is tried again as the task's `cleanup_retries` say.
    fn cleanup(&mut self, request: &Request, channel: &mut Channel) -> Result<Value, String> {
        let _ = (request, channel);
        Err(format!("extension `{}` has no cleanup", self.name()))
    }
}

/// The worker's channel: what a plugin may ask while it runs.
#[derive(Debug)]
pub struct Channel {
    stream: UnixStream,
    given: Vec<(SecretName, Value)>,
}

impl Channel {
    /// The channel over `stream`, the secrets `request` brought known.
    #[must_use]
    pub fn new(stream: UnixStream, request: &ExtensionTask) -> Self {
        let given = request
            .secrets
            .iter()
            .map(|s| (s.name.clone(), s.value.clone()))
            .collect();
        Self { stream, given }
    }

    /// Sends one message.
    ///
    /// # Errors
    ///
    /// [`Error::TooLarge`], or the socket's error.
    pub fn send(&mut self, message: &PluginMessage) -> Result<(), Error> {
        write(&mut self.stream, message)
    }

    /// Receives one message.
    ///
    /// # Errors
    ///
    /// The socket's error, or one that isn't the protocol's.
    pub fn receive(&mut self) -> Result<WorkerMessage, Error> {
        read(&mut self.stream)
    }

    fn ask(&mut self, message: &PluginMessage) -> Result<WorkerMessage, Error> {
        self.send(message)?;
        self.receive()
    }

    /// A secret the task declared: a key's value, or the whole secret (`key` `None`).
    ///
    /// # Errors
    ///
    /// [`Error::Refused`] when the worker can't give it (not declared, not found, denied).
    pub fn secret(
        &mut self,
        path: &str,
        key: Option<&str>,
        storage: Option<&str>,
    ) -> Result<Value, Error> {
        let name = SecretName {
            storage: storage.map(ToOwned::to_owned),
            path: path.to_owned(),
            key: key.map(ToOwned::to_owned),
        };
        if let Some((_, value)) = self.given.iter().find(|(given, _)| *given == name) {
            return Ok(value.clone());
        }
        match self.ask(&PluginMessage::SecretGet { secret: name })? {
            WorkerMessage::Secret { value } => Ok(value),
            WorkerMessage::SecretFailed { kind, message } => {
                Err(Error::Refused(format!("{kind}: {message}")))
            }
            other => Err(unexpected("secret", &other)),
        }
    }

    /// A connection the task declared: its values by parameter.
    ///
    /// # Errors
    ///
    /// [`Error::Refused`] when the worker can't give it.
    pub fn connection(
        &mut self,
        name: &str,
    ) -> Result<std::collections::BTreeMap<String, Value>, Error> {
        match self.ask(&PluginMessage::ConnectionGet {
            name: name.to_owned(),
        })? {
            WorkerMessage::Connection { values } => Ok(values),
            WorkerMessage::ConnectionFailed { kind, message } => {
                Err(Error::Refused(format!("{kind}: {message}")))
            }
            other => Err(unexpected("connection", &other)),
        }
    }

    /// Registers what the cleanup needs, made by this attempt (a job's ID, a pod's name).
    ///
    /// # Errors
    ///
    /// [`Error::Refused`] when the worker refuses it (no cleanup, a type not allowed).
    pub fn add_handle(&mut self, handle_type: &str, value: Value) -> Result<(), Error> {
        let handle = Handle {
            handle_type: handle_type.to_owned(),
            value,
        };
        match self.ask(&PluginMessage::HandleAdd { handle })? {
            WorkerMessage::HandleAdded => Ok(()),
            WorkerMessage::HandleRefused { message } => Err(Error::Refused(message)),
            other => Err(unexpected("handle_added", &other)),
        }
    }

    /// A call to schedr's REST API, made by the master as the task: its status and body.
    ///
    /// # Errors
    ///
    /// The channel's.
    pub fn api(
        &mut self,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> Result<(u16, Value), Error> {
        let call = PluginMessage::ApiCall {
            method: method.to_owned(),
            path: path.to_owned(),
            body,
            idempotency_key: None,
        };
        match self.ask(&call)? {
            WorkerMessage::ApiAnswer { status, body, .. } => Ok((status, body)),
            other => Err(unexpected("api_answer", &other)),
        }
    }
}

fn unexpected(expected: &'static str, got: &WorkerMessage) -> Error {
    let got = serde_json::to_value(got)
        .ok()
        .and_then(|v| v["type"].as_str().map(ToOwned::to_owned))
        .unwrap_or_default();
    Error::Unexpected { expected, got }
}

/// Writes one message: its length (4 bytes, big-endian), then its JSON.
///
/// # Errors
///
/// [`Error::TooLarge`], or the socket's error.
pub fn write(stream: &mut impl Write, message: &PluginMessage) -> Result<(), Error> {
    let body = serde_json::to_vec(message)?;
    let size = u32::try_from(body.len())
        .ok()
        .filter(|&n| n <= MAX_MESSAGE_SIZE)
        .ok_or(Error::TooLarge(body.len()))?;
    stream.write_all(&size.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

/// Reads one message.
///
/// # Errors
///
/// [`Error::TooLarge`], the socket's error, or JSON that isn't the protocol's.
pub fn read(stream: &mut impl Read) -> Result<WorkerMessage, Error> {
    let mut header = [0; 4];
    stream.read_exact(&mut header)?;
    let size = u32::from_be_bytes(header);
    if size > MAX_MESSAGE_SIZE {
        return Err(Error::TooLarge(size as usize));
    }
    let mut body = vec![0; size as usize];
    stream.read_exact(&mut body)?;
    Ok(serde_json::from_slice(&body)?)
}

/// Runs `plugin`: answers `--describe`, or the one request the worker sends over
/// `SCHEDR_AGENT_FD`. Call it first thing in `main`, before starting threads.
#[must_use]
pub fn main(plugin: impl Plugin) -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let fd = std::env::var(FD_ENV).ok();
    ExitCode::from(run_main(plugin, &args, fd.as_deref()))
}

/// [`main`]'s work, given the arguments and `SCHEDR_AGENT_FD`: the exit code, 0 when the task
/// succeeded, 1 when it failed or the channel did, 2 on a wrong start.
pub fn run_main(mut plugin: impl Plugin, args: &[String], fd: Option<&str>) -> u8 {
    if args == ["--describe"] {
        let description = serde_json::json!({
            "name": plugin.name(),
            "versions": plugin.versions(),
            "protocol": PROTOCOL,
        });
        println!("{description}");
        return 0;
    }
    let Some(fd) = fd.and_then(|fd| fd.parse::<i32>().ok()) else {
        eprintln!("{FD_ENV} is not set: a plugin is started by a schedr worker");
        return 2;
    };
    let stream = match socket::inherited(fd) {
        Ok(stream) => stream,
        Err(error) => {
            eprintln!("can't use the worker's socket: {error}");
            return 2;
        }
    };
    match serve(&mut plugin, stream) {
        Ok(true) => 0,
        Ok(false) => 1,
        Err(Error::Refused(reason)) => {
            eprintln!("the worker refused the plugin: {reason}");
            2
        }
        Err(error) => {
            eprintln!("{error}");
            1
        }
    }
}

/// The handshake and the one request over `stream`: whether the task succeeded.
///
/// # Errors
///
/// [`Error::Refused`] when the worker refused the plugin, or the channel's error.
pub fn serve(plugin: &mut impl Plugin, mut stream: UnixStream) -> Result<bool, Error> {
    write(
        &mut stream,
        &PluginMessage::Hello {
            schedr_version: env!("CARGO_PKG_VERSION").to_owned(),
            mode: "extension".to_owned(),
            protocol: PROTOCOL,
        },
    )?;
    match read(&mut stream)? {
        WorkerMessage::Welcome => {}
        WorkerMessage::Refused { reason } => return Err(Error::Refused(reason)),
        other => return Err(unexpected("welcome", &other)),
    }
    let raw = match read(&mut stream)? {
        WorkerMessage::ExtensionTask(raw) => *raw,
        other => return Err(unexpected("extension_task", &other)),
    };
    let max_output_size = raw.task.settings.max_output_size;
    let request = Request::of(raw)?;
    let mut channel = Channel::new(stream, &request.raw);
    let ran = if request.cleanup {
        plugin.cleanup(&request, &mut channel)
    } else {
        plugin.run(&request, &mut channel)
    };
    let result = result_of(ran, max_output_size);
    let succeeded = matches!(result, PluginMessage::TaskSucceeded { .. });
    channel.send(&result)?;
    Ok(succeeded)
}

/// The message `ran` ends with: its output, under the task's limit, or its error.
#[must_use]
pub fn result_of(ran: Result<Value, String>, max_output_size: u64) -> PluginMessage {
    let failed = |message: String, kind: Option<&str>| PluginMessage::TaskFailed {
        error: ErrorSummary {
            exception: "ExtensionError".to_owned(),
            message,
            traceback: String::new(),
        },
        kind: kind.map(ToOwned::to_owned),
    };
    match ran {
        Ok(output) => {
            let size = serde_json::to_vec(&output).map_or(usize::MAX, |json| json.len());
            if u64::try_from(size).unwrap_or(u64::MAX) > max_output_size {
                return failed(
                    format!("the output takes {size} bytes, over its {max_output_size}"),
                    Some("output_too_large"),
                );
            }
            PluginMessage::TaskSucceeded {
                output,
                outcome: "SUCCESS".to_owned(),
            }
        }
        Err(message) => failed(message, None),
    }
}
