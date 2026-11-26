//! `schedr extension shell -- <script> [args]` (M5.2i6, decision 519): any script an extension's
//! plugin.
//!
//! Configured as an extension's command (`[extensions.backup] command = ["schedr", "extension",
//! "shell", "--", "/opt/ops/backup.sh"]`), it speaks the plugin protocol with the worker and runs
//! the script: the task's payload as JSON on its stdin, the run as `SCHEDR_*` variables, its
//! stdout and stderr the attempt's log, exit 0 succeeding. The script talks to it as a command
//! task talks to its agent, through `schedr task-ctx` (`SCHEDR_CTX_FD`): each request answered by
//! passing it on to the worker (a secret, a connection, a handle, a status, a link, a metric
//! point, the stop reason, an API call), or from what the request brought (the run, its
//! parameters, a cleanup's handles); its output is what it sets with `schedr task-ctx extension
//! output set` (none: null). A cleanup runs the same script with `SCHEDR_CLEANUP=1`. Its
//! metrics and panels are the extension's config's (`[extensions.<name>] metrics`, `panels`).
//!
//! The worker signals the plugin's whole process group, the script in it: a stop reaches the
//! script directly, and this process waits for it to end to say how.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::{Arc, Mutex, PoisonError};

use schedr_agent::ctx::Answers;
use schedr_extension::{
    Channel, Error, PROTOCOL, PluginMessage, Request, StopKind, WorkerMessage, read, write,
};
use schedr_model::ctx::{CTX_FD_ENV, CtxAnswer, CtxRequest, CtxRun};
use schedr_model::runner::CleanupHandle;
use serde_json::Value;

#[cfg(test)]
mod tests;

/// The variable a cleanup's script gets, set to `1`.
pub const CLEANUP_ENV: &str = "SCHEDR_CLEANUP";

/// Runs the plugin for the one request the worker sends: its exit code, 0 when the task
/// succeeded, 1 when it failed or the channel did, 2 on a wrong start.
pub fn run(command: &[OsString], env: &BTreeMap<String, String>, err: &mut impl Write) -> u8 {
    let Some(fd) = env
        .get(schedr_extension::FD_ENV)
        .and_then(|fd| fd.parse::<i32>().ok())
    else {
        let _ = writeln!(
            err,
            "error: {} is not set: `schedr extension shell` is started by a schedr worker, as \
             an extension's command",
            schedr_extension::FD_ENV
        );
        return 2;
    };
    let stream = match schedr_extension::socket::inherited(fd) {
        Ok(stream) => stream,
        Err(error) => {
            let _ = writeln!(err, "error: can't use the worker's socket: {error}");
            return 2;
        }
    };
    // The attempt's connection to its extension's service (decision 518), the bridge's alone.
    let service = match env
        .get(schedr_extension::SERVICE_FD_ENV)
        .and_then(|fd| fd.parse::<i32>().ok())
        .map(schedr_extension::socket::inherited)
    {
        None => None,
        Some(Ok(service)) => Some(service),
        Some(Err(error)) => {
            let _ = writeln!(
                err,
                "error: can't use the extension's service connection: {error}"
            );
            return 2;
        }
    };
    match serve(command, env, (stream, service)) {
        Ok(true) => 0,
        Ok(false) => 1,
        Err(Error::Refused(reason)) => {
            let _ = writeln!(err, "error: the worker refused the plugin: {reason}");
            2
        }
        Err(error) => {
            let _ = writeln!(err, "error: {error}");
            1
        }
    }
}

/// The handshake, the request, the script run, and its result told: whether it succeeded.
fn serve(
    command: &[OsString],
    env: &BTreeMap<String, String>,
    (mut stream, service): (
        std::os::unix::net::UnixStream,
        Option<std::os::unix::net::UnixStream>,
    ),
) -> Result<bool, Error> {
    write(
        &mut stream,
        &PluginMessage::Hello {
            schedr_version: crate::VERSION.to_owned(),
            mode: "extension".to_owned(),
            protocol: PROTOCOL,
            // The config's (decision 519): a script declares nothing.
            metrics: Vec::new(),
            panels: Vec::new(),
            notifications: Vec::new(),
        },
    )?;
    match read(&mut stream)? {
        WorkerMessage::Welcome => {}
        WorkerMessage::Refused { reason } => return Err(Error::Refused(reason)),
        _ => {
            return Err(Error::Unexpected {
                expected: "welcome",
                got: "another message".to_owned(),
            });
        }
    }
    let raw = match read(&mut stream)? {
        WorkerMessage::ExtensionTask(raw) => *raw,
        _ => {
            return Err(Error::Unexpected {
                expected: "extension_task",
                got: "another message".to_owned(),
            });
        }
    };
    let max_output_size = raw.task.settings.max_output_size;
    let request = Request::of(raw)?;
    let run: ShellRun = serde_json::from_value(request.raw.run.clone())?;
    let channel = Channel::new(stream, &request.raw).with_service(service);
    let bridge = Arc::new(Bridge::new(&request, &run, channel, max_output_size));
    let ran = run_script(command, env, &request, &run, &bridge);
    let result = schedr_extension::result_of(ran, max_output_size);
    let succeeded = matches!(result, PluginMessage::TaskSucceeded { .. });
    bridge.channel().send(&result)?;
    Ok(succeeded)
}

/// The run's information a script gets, read as the protocol says: a field it doesn't know
/// ignored (agy's review of M5.2i6: the worker's own type refused a newer worker's).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ShellRun {
    /// The DAG's ID.
    pub dag_id: String,
    /// The run's ID.
    pub run_id: String,
    /// The run's kind.
    pub kind: schedr_model::states::RunKind,
    /// The moment the run is for.
    pub planned_for: jiff::Timestamp,
    /// The start of its window.
    pub since: jiff::Timestamp,
    /// The attempt's try number.
    pub try_number: u32,
    /// Which instance of a mapped task.
    #[serde(default)]
    pub map_index: Option<u32>,
    /// The run's parameters, as stored.
    #[serde(default = "no_params")]
    pub params: Value,
}

fn no_params() -> Value {
    Value::Object(serde_json::Map::new())
}

/// The script's variables: the run's, as a command task's (`SCHEDR_DAG_ID`, ...), the
/// extension's, and `SCHEDR_CLEANUP` in a cleanup.
#[must_use]
pub fn script_variables(request: &Request, run: &ShellRun) -> BTreeMap<String, String> {
    let mut vars = schedr_agent::env::RunVars {
        dag_id: run.dag_id.clone(),
        run_id: run.run_id.clone(),
        run_kind: run.kind.to_string(),
        task_id: request.raw.task.id.clone(),
        map_index: run.map_index,
        try_number: run.try_number,
        planned_for: run.planned_for.to_string(),
        since: run.since.to_string(),
    }
    .variables();
    vars.insert("SCHEDR_EXTENSION".to_owned(), request.name.clone());
    vars.insert(
        "SCHEDR_EXTENSION_VERSION".to_owned(),
        request.version.clone(),
    );
    if request.cleanup {
        vars.insert(CLEANUP_ENV.to_owned(), "1".to_owned());
    }
    vars
}

/// The script's `PATH`: this program's folder first, so the `schedr-ctx` and `schedr task-ctx` it
/// calls are this build, speaking its context's protocol; then the worker's. Every task's too
/// (decision 532).
#[must_use]
pub fn script_path(path: Option<&str>) -> Option<String> {
    let exe = std::env::current_exe().ok();
    path_with(exe.as_deref().and_then(std::path::Path::parent), path)
}

/// The system's default `PATH` (POSIX's `_CS_PATH` on Linux), kept after the worker's folder
/// when the worker was started without one.
const DEFAULT_PATH: &str = "/usr/bin:/bin";

/// `path` with `own` first (agy's review of M5.3c): only an absolute folder whose name is text
/// without `:` (one would split it, adding an empty entry, the current directory), not when it is
/// first already; no `path`, the system's default after it; none to set, none.
#[must_use]
pub fn path_with(own: Option<&std::path::Path>, path: Option<&str>) -> Option<String> {
    let own = own
        .filter(|dir| dir.is_absolute())
        .and_then(std::path::Path::to_str)
        .filter(|dir| !dir.contains(':'));
    match (own, path) {
        (Some(own), Some(path)) if path.split(':').next() == Some(own) => Some(path.to_owned()),
        (Some(own), Some(path)) if !path.is_empty() => Some(format!("{own}:{path}")),
        (Some(own), _) => Some(format!("{own}:{DEFAULT_PATH}")),
        (None, path) => path.map(str::to_owned),
    }
}

/// Runs the script to its end, serving its context meanwhile: its output, or why it failed.
fn run_script(
    command: &[OsString],
    env: &BTreeMap<String, String>,
    request: &Request,
    run: &ShellRun,
    bridge: &Arc<Bridge>,
) -> Result<Value, String> {
    let Some((program, args)) = command.split_first() else {
        return Err("no script given: `schedr extension shell -- <script> [args]`".to_owned());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| format!("can't start: {e}"))?;
    let result = runtime.block_on(async {
        // A stop reaches the script in the same process group; this process stays to say how
        // it ended (its default would end it first).
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        let (ours, theirs) =
            schedr_agent::ctx::channel().map_err(|e| format!("can't make its context: {e}"))?;
        let serving = schedr_agent::ctx::serve_with(ours, Arc::clone(bridge) as Arc<dyn Answers>);
        // Inheritable for the script only: no other process is started here.
        rustix::io::fcntl_setfd(&theirs, rustix::io::FdFlags::empty())
            .map_err(|e| format!("can't pass its context: {e}"))?;
        let mut child = tokio::process::Command::new(program)
            .args(args)
            .env_clear()
            .envs(env.iter().filter(|(name, _)| {
                name.as_str() != schedr_extension::FD_ENV
                    && name.as_str() != schedr_extension::SERVICE_FD_ENV
                    && name.as_str() != CTX_FD_ENV
            }))
            .envs(script_variables(request, run))
            .envs(script_path(env.get("PATH").map(String::as_str)).map(|path| ("PATH", path)))
            .env(CTX_FD_ENV, theirs.as_raw_fd().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| format!("can't start `{}`: {e}", program.to_string_lossy()))?;
        drop(theirs);
        // A stop that came before the script started was taken here: passed on (agy's review
        // of M5.2i6); one after reaches it in the process group.
        if let Some(terminate) = terminate.as_mut()
            && tokio::time::timeout(std::time::Duration::ZERO, terminate.recv())
                .await
                .is_ok()
            && let Some(pid) = child
                .id()
                .and_then(|id| rustix::process::Pid::from_raw(i32::try_from(id).ok()?))
        {
            let _ = rustix::process::kill_process(pid, rustix::process::Signal::TERM);
        }
        // Written beside the wait: a script that leaves it unread (a process it started
        // holding stdin) can't hold this one up (agy's review of M5.2i6).
        let writer = child.stdin.take().map(|mut stdin| {
            let payload = serde_json::to_vec(&request.payload).unwrap_or_default();
            tokio::spawn(async move {
                // A script that doesn't read it closes its end: not an error.
                let _ = tokio::io::AsyncWriteExt::write_all(&mut stdin, &payload).await;
            })
        });
        let status = child
            .wait()
            .await
            .map_err(|e| format!("can't wait for the script: {e}"));
        if let Some(writer) = writer {
            writer.abort();
        }
        // Nothing more is passed on: a call still queued is refused (agy's review of M5.2i6).
        bridge.close();
        drop(serving);
        ended(status?, bridge.output())
    });
    // A call the worker is answering finishes on its own thread; not waited for here.
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    result
}

/// The task's end from the script's: exit 0 succeeding with the output it set (null without).
fn ended(status: std::process::ExitStatus, output: Option<Value>) -> Result<Value, String> {
    use std::os::unix::process::ExitStatusExt as _;
    match (status.code(), status.signal()) {
        (Some(0), _) => Ok(output.unwrap_or(Value::Null)),
        (Some(code), _) => Err(format!("the script exited with {code}")),
        (None, Some(signal)) => Err(format!("the script was ended by signal {signal}")),
        (None, None) => Err("the script ended without an exit code".to_owned()),
    }
}

/// The script's context, answered from the request or by the worker through the plugin's
/// channel.
pub struct Bridge {
    channel: Mutex<Channel>,
    /// The attempt's connection to its service, apart from the channel (agy's review: a call
    /// to a hung service held the channel, and the script's end waited on it for ever).
    service: Mutex<Option<schedr_extension::ServiceCalls>>,
    run: CtxRun,
    params: Value,
    cleanup: Option<Vec<CleanupHandle>>,
    max_output_size: u64,
    output: Mutex<Option<Value>>,
    closed: std::sync::atomic::AtomicBool,
    /// One request at a time, checked for `closed` once its turn comes.
    turn: Mutex<()>,
}

impl Bridge {
    /// The bridge of `request`'s run, talking to the worker over `channel`.
    #[must_use]
    pub fn new(
        request: &Request,
        run: &ShellRun,
        mut channel: Channel,
        max_output_size: u64,
    ) -> Self {
        Self {
            service: Mutex::new(channel.take_service()),
            channel: Mutex::new(channel),
            run: CtxRun {
                dag_id: run.dag_id.clone(),
                id: run.run_id.clone(),
                kind: run.kind,
                planned_for: run.planned_for,
                since: run.since,
            },
            params: run.params.clone(),
            cleanup: request.cleanup.then(|| {
                request
                    .handles
                    .iter()
                    .map(|h| CleanupHandle {
                        handle_type: h.handle_type.clone(),
                        value: h.value.clone(),
                    })
                    .collect()
            }),
            max_output_size,
            output: Mutex::new(None),
            closed: std::sync::atomic::AtomicBool::new(false),
            turn: Mutex::new(()),
        }
    }

    /// The script has ended: nothing more is passed on to the worker.
    pub fn close(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn channel(&self) -> std::sync::MutexGuard<'_, Channel> {
        self.channel.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn output(&self) -> Option<Value> {
        self.output
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The answer to `request`: the worker asked, one request at a time, blocking.
    #[must_use]
    pub fn answered(&self, request: CtxRequest) -> CtxAnswer {
        let request = match self.answered_apart(request) {
            Ok(answer) => return answer,
            Err(request) => request,
        };
        let _turn = self.turn.lock().unwrap_or_else(PoisonError::into_inner);
        if self.is_closed() {
            return CtxAnswer::Error {
                reason: "the script has ended".to_owned(),
            };
        }
        match request {
            CtxRequest::ParamsGet => CtxAnswer::Params {
                params: self.params.clone(),
            },
            CtxRequest::RunGet => CtxAnswer::Run(self.run.clone()),
            CtxRequest::SecretGet { secret } => {
                let asked = self.channel().secret(
                    &secret.path,
                    secret.key.as_deref(),
                    secret.storage.as_deref(),
                );
                match asked {
                    Ok(value) => CtxAnswer::Secret { value },
                    Err(error) => {
                        let (kind, reason) = kind_of(&error, "secret_unavailable");
                        CtxAnswer::SecretFailed { kind, reason }
                    }
                }
            }
            CtxRequest::ConnectionGet { name } => match self.channel().connection(&name) {
                Ok(values) => CtxAnswer::Connection { values },
                Err(error) => {
                    let (kind, reason) = kind_of(&error, "connection_not_found");
                    CtxAnswer::ConnectionFailed { kind, reason }
                }
            },
            CtxRequest::ConnectionEnvGet { name, .. } => CtxAnswer::ConnectionFailed {
                kind: "connection_not_found".to_owned(),
                reason: format!(
                    "connection `{name}`: `--env` isn't served in a shell plugin (it writes \
                     files in a command task's folder); use `--json`"
                ),
            },
            CtxRequest::ApiCall {
                method,
                path,
                body,
                idempotency_key,
            } => self.api_call(&PluginMessage::ApiCall {
                method,
                path,
                body,
                idempotency_key,
            }),
            CtxRequest::HandleAdd { handle } => {
                if self.cleanup.is_some() {
                    return CtxAnswer::HandleRefused {
                        reason: "a cleanup registers no handles".to_owned(),
                    };
                }
                match self.channel().add_handle(&handle.handle_type, handle.value) {
                    Ok(()) => CtxAnswer::HandleAdded,
                    Err(error) => CtxAnswer::HandleRefused {
                        reason: error.to_string(),
                    },
                }
            }
            CtxRequest::HandleList => match &self.cleanup {
                Some(handles) => CtxAnswer::Handles {
                    handles: handles.clone(),
                },
                None => CtxAnswer::Error {
                    reason: "`cleanup list` works only inside a cleanup".to_owned(),
                },
            },
            CtxRequest::ResourceSet { .. } | CtxRequest::ResourceGet => CtxAnswer::Error {
                reason: "a shell plugin is no resource's command part".to_owned(),
            },
            request @ (CtxRequest::StatusSet { .. }
            | CtxRequest::LinkSet { .. }
            | CtxRequest::MetricAdd { .. }
            | CtxRequest::ExtensionNotify { .. }
            | CtxRequest::StopReasonGet
            | CtxRequest::OutputSet { .. }) => self.reported(request),
            CtxRequest::ServiceCall { .. } => CtxAnswer::Error {
                reason: "answered apart".to_owned(),
            },
            CtxRequest::Hello { .. } => CtxAnswer::Error {
                reason: "`hello` was said already".to_owned(),
            },
        }
    }

    /// The worker's answer to the script's API call.
    fn api_call(&self, call: &PluginMessage) -> CtxAnswer {
        let mut channel = self.channel();
        match channel.send(call).and_then(|()| channel.receive()) {
            // Bounded as a command task's, its masking note kept (agy's review of M5.2i6).
            Ok(WorkerMessage::ApiAnswer { status, body, note }) => {
                schedr_agent::ctx::api_answer(status, body, note)
            }
            Ok(_) => CtxAnswer::Error {
                reason: "the worker answered with something else than the API's answer".to_owned(),
            },
            Err(error) => CtxAnswer::Error {
                reason: error.to_string(),
            },
        }
    }

    /// A service call's answer, taking no turn: the worker's requests go on beside it; any
    /// other request back.
    fn answered_apart(&self, request: CtxRequest) -> Result<CtxAnswer, CtxRequest> {
        let CtxRequest::ServiceCall { method, args } = request else {
            return Err(request);
        };
        if self.is_closed() {
            return Ok(CtxAnswer::Error {
                reason: "the script has ended".to_owned(),
            });
        }
        Ok(self.service_call(&method, args))
    }

    /// The extension's service's answer to a call of the script's (decision 518).
    fn service_call(&self, method: &str, args: serde_json::Value) -> CtxAnswer {
        let mut service = self.service.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(service) = service.as_mut() else {
            return CtxAnswer::Error {
                reason: "the extension's service: this extension has no service \
                         ([extensions.<name>] service)"
                    .to_owned(),
            };
        };
        match service.call(method, args) {
            Ok(result) => CtxAnswer::ServiceAnswer { result },
            Err(error) => CtxAnswer::Error {
                reason: error.to_string(),
            },
        }
    }

    /// The answer to a status, link, metric point, the stop reason or the output.
    fn reported(&self, request: CtxRequest) -> CtxAnswer {
        match request {
            CtxRequest::StatusSet {
                phase,
                message,
                progress,
            } => self.sent(|channel| channel.status(&phase, message.as_deref(), progress)),
            CtxRequest::LinkSet { label, url } => self.sent(|channel| channel.link(&label, &url)),
            CtxRequest::MetricAdd {
                name,
                value,
                labels,
            } => {
                let labels: Vec<(&str, &str)> = labels
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                self.sent(|channel| channel.metric(&name, value, &labels))
            }
            CtxRequest::ExtensionNotify { event, text, data } => {
                self.sent(|channel| channel.notify(&event, text.as_deref(), data))
            }
            CtxRequest::StopReasonGet => match self.channel().stop_reason() {
                Ok((reason, left)) => CtxAnswer::StopReason {
                    reason: reason.map(|kind| stop_name(kind).to_owned()),
                    seconds_left: left.as_secs_f64(),
                },
                Err(error) => CtxAnswer::Error {
                    reason: error.to_string(),
                },
            },
            CtxRequest::OutputSet { value } => {
                let size = serde_json::to_vec(&value).map_or(usize::MAX, |json| json.len());
                if u64::try_from(size).unwrap_or(u64::MAX) > self.max_output_size {
                    return CtxAnswer::ReportRefused {
                        reason: format!(
                            "the output takes {size} bytes, over its {}",
                            self.max_output_size
                        ),
                    };
                }
                *self.output.lock().unwrap_or_else(PoisonError::into_inner) = Some(value);
                CtxAnswer::Reported
            }
            _ => CtxAnswer::Error {
                reason: "not a report".to_owned(),
            },
        }
    }

    /// A report sent to the worker (no answer: it keeps it, masked and bounded, or drops it
    /// with a warning of its own).
    fn sent(&self, send: impl FnOnce(&mut Channel) -> Result<(), Error>) -> CtxAnswer {
        match send(&mut self.channel()) {
            Ok(()) => CtxAnswer::Reported,
            Err(error) => CtxAnswer::ReportRefused {
                reason: error.to_string(),
            },
        }
    }
}

impl Answers for Bridge {
    fn version(&self) -> &str {
        crate::VERSION
    }

    fn max_connections(&self) -> usize {
        schedr_agent::ctx::MAX_CONNECTIONS
    }

    fn answer(
        &self,
        request: CtxRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = CtxAnswer> + Send + '_>> {
        // The channel blocks: answered on a thread of its own, one at a time (its lock).
        Box::pin(async move { tokio::task::block_in_place(|| self.answered(request)) })
    }
}

/// A refusal's kind and reason, `kind: reason` as the channel words it, or `default`.
fn kind_of(error: &Error, default: &str) -> (String, String) {
    let text = error.to_string();
    match text.split_once(": ") {
        Some((kind, reason))
            if !kind.is_empty() && kind.chars().all(|c| c.is_ascii_lowercase() || c == '_') =>
        {
            (kind.to_owned(), reason.to_owned())
        }
        _ => (default.to_owned(), text),
    }
}

/// A stop reason as `task-ctx extension stop-reason` prints it.
const fn stop_name(kind: StopKind) -> &'static str {
    match kind {
        StopKind::Cancel => "cancel",
        StopKind::Timeout => "timeout",
        StopKind::Drain => "drain",
        StopKind::MasterLost => "master_lost",
    }
}
