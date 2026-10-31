//! The agent's end of the runner channel: length-prefixed JSON and the handshake.
//!
//! Each message is a 4-byte big-endian length followed by that many bytes of UTF-8 JSON (see
//! `schedr-model`, `runner`). Generic over any byte stream, so it is tested in memory.

use schedr_model::runner::{
    AgentMessage, MAX_MESSAGE_SIZE, RunnerMessage, RunnerMode, check_runner_version,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[cfg(test)]
mod tests;

/// Why a message couldn't be exchanged with the runner.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    /// The runner closed the connection.
    #[error("the runner closed the connection")]
    Closed,
    /// A message is over the size limit.
    #[error("a message of {0} bytes is over the limit of {MAX_MESSAGE_SIZE} bytes")]
    TooLarge(u64),
    /// A message isn't a valid message of the expected kind.
    #[error("invalid message: {0}")]
    Invalid(#[from] serde_json::Error),
    /// Reading or writing the socket failed.
    #[error("runner channel: {0}")]
    Io(#[from] std::io::Error),
    /// The memory watchdog killed the runner.
    #[error("the runner {0}")]
    OverMemory(String),
    /// The runner sent a message that doesn't fit the conversation.
    #[error("expected {expected}, got {got}")]
    Unexpected {
        /// What was expected.
        expected: &'static str,
        /// What arrived.
        got: String,
    },
    /// The runner's version doesn't match the agent's; it was told so and refused.
    #[error("runner refused: {0}")]
    Refused(String),
}

/// Sends one message.
///
/// # Errors
///
/// Returns [`ChannelError::TooLarge`] for an oversize message, or an I/O error.
pub async fn write_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &impl Serialize,
) -> Result<(), ChannelError> {
    let body = serde_json::to_vec(message)?;
    let size = u32::try_from(body.len())
        .ok()
        .filter(|&n| n <= MAX_MESSAGE_SIZE)
        .ok_or(ChannelError::TooLarge(body.len() as u64))?;
    writer.write_all(&size.to_be_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

/// Receives one message.
///
/// # Errors
///
/// Returns [`ChannelError::Closed`] at the end of the stream, [`ChannelError::TooLarge`],
/// [`ChannelError::Invalid`] for a message that doesn't parse, or an I/O error.
pub async fn read_message<R: AsyncRead + Unpin, T: DeserializeOwned>(
    reader: &mut R,
) -> Result<T, ChannelError> {
    let mut header = [0; 4];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(ChannelError::Closed);
        }
        Err(e) => return Err(e.into()),
    }
    let size = u32::from_be_bytes(header);
    if size > MAX_MESSAGE_SIZE {
        return Err(ChannelError::TooLarge(u64::from(size)));
    }
    let mut body = vec![0; size as usize];
    match reader.read_exact(&mut body).await {
        Ok(_) => Ok(serde_json::from_slice(&body)?),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Err(ChannelError::Closed),
        Err(e) => Err(e.into()),
    }
}

/// A stream read a message at a time, safe to cancel: a message read in part when its read
/// is dropped (a `select!` another branch won) waits here for its rest, where
/// [`read_message`] would lose it and read the rest as a new message (B57).
#[derive(Debug)]
pub struct Messages<R> {
    reader: R,
    /// Bytes read and not yet a whole message.
    pending: Vec<u8>,
}

impl<R: AsyncRead + Unpin> From<R> for Messages<R> {
    fn from(reader: R) -> Self {
        Self {
            reader,
            pending: Vec::new(),
        }
    }
}

impl<R: AsyncRead + Unpin> Messages<R> {
    /// Receives the next message; cancel-safe.
    ///
    /// # Errors
    ///
    /// As [`read_message`]'s.
    pub async fn next<T: DeserializeOwned>(&mut self) -> Result<T, ChannelError> {
        loop {
            if let Some(message) = self.whole()? {
                return Ok(message);
            }
            let mut chunk = [0; 8192];
            // `read` is cancel-safe: dropped, it has read nothing.
            let read = self.reader.read(&mut chunk).await?;
            if read == 0 {
                return Err(ChannelError::Closed);
            }
            self.pending.extend_from_slice(&chunk[..read]);
        }
    }

    /// The first whole message in what was read, taken out of it; `None` until there is one.
    fn whole<T: DeserializeOwned>(&mut self) -> Result<Option<T>, ChannelError> {
        let Some(header) = self.pending.first_chunk::<4>() else {
            return Ok(None);
        };
        let size = u32::from_be_bytes(*header);
        if size > MAX_MESSAGE_SIZE {
            return Err(ChannelError::TooLarge(u64::from(size)));
        }
        let end = 4 + size as usize;
        if self.pending.len() < end {
            return Ok(None);
        }
        let message = serde_json::from_slice(&self.pending[4..end]);
        self.pending.drain(..end);
        Ok(Some(message?))
    }
}

/// Accepts a newly connected runner: reads its `hello`, refuses a version mismatch, and
/// welcomes it otherwise.
///
/// # Errors
///
/// Returns [`ChannelError::Refused`] after sending `refused` for a version mismatch,
/// [`ChannelError::Unexpected`] if the first message isn't `hello`, or a channel error.
pub async fn accept_runner<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    agent_version: &str,
) -> Result<RunnerMode, ChannelError> {
    let message: RunnerMessage = read_message(stream).await?;
    let RunnerMessage::Hello {
        schedr_version,
        mode,
    } = message
    else {
        return Err(ChannelError::Unexpected {
            expected: "hello",
            got: describe(&message),
        });
    };
    if let Err(reason) = check_runner_version(agent_version, &schedr_version) {
        write_message(
            stream,
            &AgentMessage::Refused {
                reason: reason.clone(),
            },
        )
        .await?;
        return Err(ChannelError::Refused(reason));
    }
    write_message(stream, &AgentMessage::Welcome).await?;
    Ok(mode)
}

/// The `type` of a runner message, for messages.
pub(crate) fn describe(message: &RunnerMessage) -> String {
    serde_json::to_value(message)
        .ok()
        .and_then(|v| v["type"].as_str().map(str::to_owned))
        .unwrap_or_default()
}
