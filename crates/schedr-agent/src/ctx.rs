//! A command task's context channel (decisions 143-146): the agent's side of `schedr dag-ctx`
//! and `schedr.script`.
//!
//! The task inherits one end of a datagram socket pair (`SCHEDR_CTX_FD`), and so does every
//! process it starts. A call makes a stream socket pair of its own and sends one end over it
//! (`SCM_RIGHTS`, one datagram); the agent serves each received end as a connection of its
//! own (see `schedr_model::ctx`), until the attempt ends. A caller is accepted by the protocol
//! it speaks, not its schedr version (decision 148). The agent never sends on the
//! datagram socket, so concurrent callers can't take each other's answers.

use std::io;
use std::mem::MaybeUninit;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustix::net::{
    AddressFamily, RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, SendFlags, SocketType,
};
use schedr_model::ctx::{CTX_PROTOCOL, CtxAnswer, CtxRequest, CtxRun, MAX_CTX_MESSAGE_SIZE};
use schedr_model::runner::RunInfo;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, Interest, ReadBuf};
use tokio::net::{UnixDatagram, UnixStream};
use tokio::task::JoinSet;

use crate::handles::{AttemptHandles, HandleError};
use crate::resource::{AttemptResource, ResourceError};
use crate::secrets::{AttemptSecrets, ConnectionError, SecretError};

#[cfg(test)]
mod tests;

/// The most connections a task may have open at once; an end sent beyond it is closed at
/// once (the caller sees the connection close).
pub const MAX_CONNECTIONS: usize = 64;

/// The most descriptors read from one datagram: Linux's limit per message (`SCM_MAX_FD`), so a
/// datagram isn't truncated and every descriptor it carries is closed (a datagram must carry
/// exactly one). On macOS, truncated control data needs rustix 1.1.6's clamp of `cmsg_len`.
const MAX_FDS_PER_DATAGRAM: usize = 253;

/// What a task's processes can read over the channel.
#[derive(Debug, Clone)]
pub struct CtxInfo {
    /// The run information.
    pub run: CtxRun,
    /// The run's parameters, as stored.
    pub params: serde_json::Value,
    /// The agent's schedr version (for a refusal's message; callers are accepted by their
    /// protocol, decision 148).
    pub version: String,
    /// The attempt's secrets (`schedr task-ctx secret get`); none for a task without any.
    pub secrets: Option<Arc<AttemptSecrets>>,
    /// The attempt's cleanup handles (`schedr task-ctx cleanup`); none outside an attempt.
    pub handles: Option<Arc<AttemptHandles>>,
    /// The resource value it reports or reads (`schedr task-ctx resource`); none for a task
    /// that is no resource's command part.
    pub resource: Option<Arc<AttemptResource>>,
}

impl CtxInfo {
    /// The information of an attempt's run.
    #[must_use]
    pub fn of(run: &RunInfo, version: &str) -> Self {
        Self {
            run: CtxRun {
                dag_id: run.dag_id.clone(),
                id: run.run_id.clone(),
                kind: run.kind,
                planned_for: run.planned_for,
                since: run.since,
            },
            params: run.params.clone(),
            version: version.to_owned(),
            secrets: None,
            handles: None,
            resource: None,
        }
    }
}

/// A new channel: the agent's end, and the task's end to pass to its process (close-on-exec,
/// as every socket the agent makes; made inheritable only while that process starts).
///
/// # Errors
///
/// An I/O error if the socket pair can't be made.
pub fn channel() -> io::Result<(UnixDatagram, OwnedFd)> {
    let (ours, theirs) = std::os::unix::net::UnixDatagram::pair()?;
    ours.set_nonblocking(true)?;
    Ok((UnixDatagram::from_std(ours)?, OwnedFd::from(theirs)))
}

/// Serving a channel; stops (its connections closed) when dropped.
#[derive(Debug)]
pub struct Serving(tokio::task::JoinHandle<()>);

impl Drop for Serving {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Serves the agent's end of a channel in the background, until the returned guard drops.
#[must_use]
pub fn serve(socket: UnixDatagram, info: CtxInfo) -> Serving {
    Serving(tokio::spawn(accept(socket, Arc::new(info))))
}

/// Takes the ends sent over `socket` and serves each; its connections end with it.
async fn accept(socket: UnixDatagram, info: Arc<CtxInfo>) {
    let mut connections = JoinSet::new();
    loop {
        let end = match receive_end(&socket).await {
            Ok(Some(end)) => end,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(%error, "the task's context channel failed; it is closed");
                return;
            }
        };
        while connections.try_join_next().is_some() {}
        let Some(end) = connection(end) else {
            continue;
        };
        if connections.len() >= MAX_CONNECTIONS {
            tracing::debug!("a task has {MAX_CONNECTIONS} context connections open; refused one");
            // Said on the fresh connection (its buffer is empty, so the send doesn't wait),
            // then closed.
            let reason = format!(
                "the task has {MAX_CONNECTIONS} context connections open, the most the agent \
                 serves at once"
            );
            if let Ok(frame) = framed(&CtxAnswer::Error { reason }) {
                let _ = rustix::net::send(&end, &frame, SEND_FLAGS);
            }
            continue;
        }
        let stream = std::os::unix::net::UnixStream::from(end);
        // Tokio expects a non-blocking socket when registering it (a debug assertion; a task
        // clearing the flag in between makes its own accept task panic, nothing more). The
        // reads and writes don't rely on the flag (`Received`).
        if stream.set_nonblocking(true).is_err() {
            continue;
        }
        let Ok(stream) = UnixStream::from_std(stream).map(Received) else {
            continue;
        };
        let info = Arc::clone(&info);
        connections.spawn(async move {
            let mut stream = stream;
            if let Err(error) = converse(&mut stream, &info).await {
                tracing::debug!(%error, "a context connection ended");
            }
        });
    }
}

/// A received end, read and written with `MSG_DONTWAIT` on each call. Its `O_NONBLOCK` flag
/// can't be relied on: it belongs to the open file description, which the sender shares and
/// can clear, and a blocking read would hold one of the runtime's threads.
/// Tokio's stream gives the readiness only; its own reads and writes, which rely on that flag,
/// are never used.
#[derive(Debug)]
struct Received(UnixStream);

/// Sends never wait, and a closed peer is an error rather than `SIGPIPE` (Linux's flag;
/// elsewhere Rust's runtime ignores `SIGPIPE` already).
#[cfg(target_os = "linux")]
const SEND_FLAGS: SendFlags = SendFlags::DONTWAIT.union(SendFlags::NOSIGNAL);
#[cfg(not(target_os = "linux"))]
const SEND_FLAGS: SendFlags = SendFlags::DONTWAIT;

impl AsyncRead for Received {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let stream = &self.get_mut().0;
        loop {
            std::task::ready!(stream.poll_read_ready(cx))?;
            let unfilled = buf.initialize_unfilled();
            let read = stream.try_io(Interest::READABLE, || {
                rustix::net::recv(stream, &mut *unfilled, RecvFlags::DONTWAIT)
                    .map(|(n, _)| n)
                    .map_err(io::Error::from)
            });
            match read {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => return Poll::Ready(result.map(|n| buf.advance(n))),
            }
        }
    }
}

impl AsyncWrite for Received {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let stream = &self.get_mut().0;
        loop {
            std::task::ready!(stream.poll_write_ready(cx))?;
            let sent = stream.try_io(Interest::WRITABLE, || {
                rustix::net::send(stream, data, SEND_FLAGS).map_err(io::Error::from)
            });
            match sent {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => return Poll::Ready(result),
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let stream = &self.get_mut().0;
        Poll::Ready(
            rustix::net::shutdown(stream, rustix::net::Shutdown::Write).map_err(io::Error::from),
        )
    }
}

/// Receives one datagram: the end it carries, or `None` for one carrying none, several, or a
/// truncated set (all closed).
async fn receive_end(socket: &UnixDatagram) -> io::Result<Option<OwnedFd>> {
    loop {
        socket.readable().await?;
        // Without `MSG_CMSG_CLOEXEC`, the ends are open across exec until marked; no process
        // starts meanwhile.
        #[cfg(not(target_os = "linux"))]
        let _spawning = crate::process::SPAWNING.lock().await;
        match socket.try_io(tokio::io::Interest::READABLE, || receive_now(socket)) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            result => return result,
        }
    }
}

#[cfg(target_os = "linux")]
const RECV_FLAGS: RecvFlags = RecvFlags::CMSG_CLOEXEC;
#[cfg(not(target_os = "linux"))]
const RECV_FLAGS: RecvFlags = RecvFlags::empty();

fn receive_now(socket: &UnixDatagram) -> io::Result<Option<OwnedFd>> {
    let mut data = [0_u8; 16];
    let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_FDS_PER_DATAGRAM))];
    let mut control = RecvAncillaryBuffer::new(&mut space);
    let received = rustix::net::recvmsg(
        socket,
        &mut [io::IoSliceMut::new(&mut data)],
        &mut control,
        RECV_FLAGS,
    )?;
    let mut ends: Vec<OwnedFd> = Vec::new();
    for message in control.drain() {
        if let RecvAncillaryMessage::ScmRights(fds) = message {
            ends.extend(fds);
        }
    }
    #[cfg(not(target_os = "linux"))]
    for end in &ends {
        rustix::io::fcntl_setfd(end, rustix::io::FdFlags::CLOEXEC)?;
    }
    let whole = !received.flags.contains(rustix::net::ReturnFlags::CTRUNC);
    Ok(match (whole, ends.len()) {
        (true, 1) => ends.pop(),
        _ => None,
    })
}

/// The received end, if it is the end of a Unix stream socket (else closed).
fn connection(end: OwnedFd) -> Option<OwnedFd> {
    let stream = rustix::net::sockopt::socket_type(&end).ok() == Some(SocketType::STREAM);
    let unix = rustix::net::getsockname(&end)
        .is_ok_and(|address| address.address_family() == AddressFamily::UNIX);
    if !(stream && unix) {
        tracing::debug!("a task sent a descriptor that isn't a Unix stream socket; closed");
        return None;
    }
    Some(end)
}

/// Why a conversation ended early.
#[derive(Debug, thiserror::Error)]
pub enum CtxError {
    /// A message over [`MAX_CTX_MESSAGE_SIZE`].
    #[error("a message of {0} bytes is over the limit of {MAX_CTX_MESSAGE_SIZE} bytes")]
    TooLarge(u32),
    /// Reading or writing failed.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// A message that doesn't encode.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// One connection: the handshake, then one answer per request until the caller closes it.
///
/// # Errors
///
/// [`CtxError`] when the connection breaks or a message is over the limit.
pub async fn converse<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    info: &CtxInfo,
) -> Result<(), CtxError> {
    match read_request(stream).await? {
        Some(Ok(CtxRequest::Hello { protocol, .. })) if protocol == CTX_PROTOCOL => {
            write_answer(stream, &CtxAnswer::Welcome).await?;
        }
        Some(Ok(CtxRequest::Hello {
            protocol,
            schedr_version,
        })) => {
            let reason = format!(
                "this schedr ({schedr_version}) speaks protocol {protocol} of the task's \
                 context, the task's agent ({}) protocol {CTX_PROTOCOL}: use schedr {} (or one \
                 speaking protocol {CTX_PROTOCOL}) in the task",
                info.version, info.version
            );
            return write_answer(stream, &CtxAnswer::Refused { reason }).await;
        }
        Some(_) => return refuse(stream, "say `hello` first").await,
        None => return Ok(()),
    }
    loop {
        let answer = match read_request(stream).await? {
            None => return Ok(()),
            Some(Ok(CtxRequest::ParamsGet)) => CtxAnswer::Params {
                params: info.params.clone(),
            },
            Some(Ok(CtxRequest::RunGet)) => CtxAnswer::Run(info.run.clone()),
            Some(Ok(CtxRequest::SecretGet { secret })) => match &info.secrets {
                Some(secrets) => match secrets.read(&secret).await {
                    Ok(value) => CtxAnswer::Secret { value },
                    Err(error) => CtxAnswer::SecretFailed {
                        kind: error.kind().to_owned(),
                        reason: error.to_string(),
                    },
                },
                None => CtxAnswer::SecretFailed {
                    kind: SecretError::Denied(String::new()).kind().to_owned(),
                    reason: format!("`{secret}` isn't declared in the task's `secrets=`"),
                },
            },
            Some(Ok(CtxRequest::HandleAdd { handle })) => {
                let added = match info.handles.as_ref() {
                    Some(handles) => handles.add(handle).await,
                    None => Err(HandleError::NoCleanup),
                };
                match added {
                    Ok(()) => CtxAnswer::HandleAdded,
                    Err(error) => CtxAnswer::HandleRefused {
                        reason: error.to_string(),
                    },
                }
            }
            Some(Ok(CtxRequest::HandleList)) => match info.handles.as_ref() {
                Some(handles) if handles.in_cleanup() => CtxAnswer::Handles {
                    handles: handles.list(),
                },
                _ => CtxAnswer::Error {
                    reason: "`cleanup list` works only inside a cleanup".to_owned(),
                },
            },
            Some(Ok(CtxRequest::ConnectionGet { name })) => {
                let opened = match &info.secrets {
                    Some(secrets) => secrets.connection(&name).await,
                    None => Err(ConnectionError::NotFound(format!(
                        "connection `{name}`: no connections are known for this task"
                    ))),
                };
                match opened {
                    Ok(values) => CtxAnswer::Connection { values },
                    Err(error) => CtxAnswer::ConnectionFailed {
                        kind: error.kind().to_owned(),
                        reason: error.to_string(),
                    },
                }
            }
            Some(Ok(CtxRequest::ConnectionEnvGet { name, prefix })) => {
                let given = match &info.secrets {
                    Some(secrets) => secrets.connection_env(&name, &prefix).await,
                    None => Err(ConnectionError::NotFound(format!(
                        "connection `{name}`: no connections are known for this task"
                    ))),
                };
                match given {
                    Ok(variables) => CtxAnswer::ConnectionEnv { variables },
                    Err(error) => CtxAnswer::ConnectionFailed {
                        kind: error.kind().to_owned(),
                        reason: error.to_string(),
                    },
                }
            }
            Some(Ok(CtxRequest::ResourceSet { value })) => resource_answer(info, Some(&value)),
            Some(Ok(CtxRequest::ResourceGet)) => resource_answer(info, None),
            Some(Ok(CtxRequest::Hello { .. })) => {
                return refuse(stream, "`hello` was said already").await;
            }
            Some(Err(reason)) => return refuse(stream, &reason).await,
        };
        write_answer(stream, &answer).await?;
    }
}

/// The answer to `resource set` (with the value) or `resource get`.
fn resource_answer(info: &CtxInfo, set: Option<&serde_json::Value>) -> CtxAnswer {
    let refused = |error: ResourceError| CtxAnswer::ResourceRefused {
        reason: error.to_string(),
    };
    match (info.resource.as_ref(), set) {
        (Some(resource), Some(value)) => match resource.set(value) {
            Ok(()) => CtxAnswer::ResourceReported,
            Err(error) => refused(error),
        },
        (Some(resource), None) => match resource.get() {
            Ok(value) => CtxAnswer::Resource { value },
            Err(error) => refused(error),
        },
        (None, Some(_)) => refused(ResourceError::NotSetup),
        (None, None) => refused(ResourceError::NotCleanup),
    }
}

async fn refuse<W: AsyncWrite + Unpin>(stream: &mut W, reason: &str) -> Result<(), CtxError> {
    let reason = reason.to_owned();
    write_answer(stream, &CtxAnswer::Error { reason }).await
}

/// The next request: `None` at the end of the stream, `Some(Err(why))` for one that doesn't
/// read (an unknown type: a newer caller).
async fn read_request<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Result<CtxRequest, String>>, CtxError> {
    let mut header = [0; 4];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let size = u32::from_be_bytes(header);
    if size > MAX_CTX_MESSAGE_SIZE {
        return Err(CtxError::TooLarge(size));
    }
    let mut body = vec![0; size as usize];
    reader.read_exact(&mut body).await?;
    Ok(Some(serde_json::from_slice(&body).map_err(|e| {
        format!("not a request this agent knows: {e}")
    })))
}

/// An answer as sent: its length, then its JSON.
fn framed(answer: &CtxAnswer) -> Result<Vec<u8>, CtxError> {
    let body = serde_json::to_vec(answer)?;
    let size = u32::try_from(body.len())
        .ok()
        .filter(|&n| n <= MAX_CTX_MESSAGE_SIZE)
        .ok_or(CtxError::TooLarge(u32::MAX))?;
    let mut frame = size.to_be_bytes().to_vec();
    frame.extend(body);
    Ok(frame)
}

async fn write_answer<W: AsyncWrite + Unpin>(
    writer: &mut W,
    answer: &CtxAnswer,
) -> Result<(), CtxError> {
    writer.write_all(&framed(answer)?).await?;
    writer.flush().await?;
    Ok(())
}
