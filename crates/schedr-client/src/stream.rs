//! Streamed answers: a response body read as it comes ([`Chunks`]), and server-sent events
//! read from one ([`EventReader`]), for `GET /api/v1/events`.

use tokio::sync::mpsc;

use crate::ClientError;

#[cfg(test)]
mod tests;

/// A response body as it arrives, chunk by chunk.
#[derive(Debug)]
pub struct Chunks {
    receiver: mpsc::Receiver<Result<Vec<u8>, ClientError>>,
}

impl Chunks {
    /// Chunks handed over through a channel (by a task reading the body), and its sending end.
    #[must_use]
    pub fn channel() -> (mpsc::Sender<Result<Vec<u8>, ClientError>>, Self) {
        let (sender, receiver) = mpsc::channel(16);
        (sender, Self { receiver })
    }

    /// The next chunk; `None` at the end of the body.
    pub async fn next(&mut self) -> Option<Result<Vec<u8>, ClientError>> {
        self.receiver.recv().await
    }
}

/// A server-sent event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerEvent {
    /// Its name (`message` when the server gave none).
    pub name: String,
    /// Its data, its `data:` lines joined by newlines.
    pub data: String,
}

/// Server-sent events read from a stream (the event-stream format: `event:` and `data:`
/// lines, a blank line ending each event, `:` comments skipped).
#[derive(Debug)]
pub struct EventReader {
    chunks: Chunks,
    /// Bytes read and not yet parsed (an event's start, possibly a character cut in two).
    pending: Vec<u8>,
}

impl EventReader {
    /// Events from `chunks`.
    #[must_use]
    pub const fn new(chunks: Chunks) -> Self {
        Self {
            chunks,
            pending: Vec::new(),
        }
    }

    /// The next event; `None` when the stream ends.
    pub async fn next(&mut self) -> Option<Result<ServerEvent, ClientError>> {
        loop {
            if let Some(event) = self.parse_one() {
                return Some(Ok(event));
            }
            match self.chunks.next().await? {
                Ok(chunk) => self.pending.extend_from_slice(&chunk),
                Err(error) => return Some(Err(error)),
            }
        }
    }

    /// The first whole event in what is pending, taken out; blocks of comments only are
    /// dropped on the way.
    fn parse_one(&mut self) -> Option<ServerEvent> {
        loop {
            let end = blank_line(&self.pending)?;
            let block: Vec<u8> = self.pending.drain(..end.1).take(end.0).collect();
            let text = String::from_utf8_lossy(&block);
            let mut name = None;
            let mut data: Option<String> = None;
            for line in text.split('\n').map(|l| l.strip_suffix('\r').unwrap_or(l)) {
                let (field, value) = line.split_once(':').unwrap_or((line, ""));
                let value = value.strip_prefix(' ').unwrap_or(value);
                match field {
                    "event" => name = Some(value.to_owned()),
                    "data" => match &mut data {
                        Some(data) => {
                            data.push('\n');
                            data.push_str(value);
                        }
                        None => data = Some(value.to_owned()),
                    },
                    _ => {}
                }
            }
            if name.is_some() || data.is_some() {
                return Some(ServerEvent {
                    name: name.unwrap_or_else(|| "message".to_owned()),
                    data: data.unwrap_or_default(),
                });
            }
        }
    }
}

/// Where the first event in `bytes` ends: its length, and its length with the blank line
/// after it (`\n\n` or `\r\n\r\n`).
fn blank_line(bytes: &[u8]) -> Option<(usize, usize)> {
    (0..bytes.len()).find_map(|i| {
        if bytes[i..].starts_with(b"\n\n") {
            Some((i, i + 2))
        } else if bytes[i..].starts_with(b"\r\n\r\n") {
            Some((i, i + 4))
        } else {
            None
        }
    })
}
