//! The master's gRPC service and its loop: workers' `Control` streams, parse uploads and
//! `FetchInputs`, over [`MasterCore`] (see PROJECT.md, Master–worker protocol).
//!
//! A [`Hub`] holds the core and each live worker's outgoing stream; whatever the core decides
//! to send (assignments, cancels, acknowledgements) is delivered through it, from a stream's
//! messages and from the loop's ticks alike.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use jiff::Timestamp;
use schedr_proto::pb::master_server::{Master, MasterServer};
use schedr_proto::pb::{self, master_message, worker_message};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::core::{CoreError, MasterCore};
use crate::dispatch::Outgoing;

#[cfg(test)]
mod tests;

/// How many messages wait for a worker before the oldest sender waits.
const STREAM_BUFFER: usize = 1024;

/// The size of one `FetchInputs` chunk.
pub const INPUT_CHUNK: usize = 1024 * 1024;

type Outbound = mpsc::Sender<Result<pb::MasterMessage, Status>>;

/// The core and the workers' outgoing streams.
#[derive(Debug, Clone)]
pub struct Hub {
    core: Arc<Mutex<MasterCore>>,
    streams: Arc<Mutex<BTreeMap<String, Outbound>>>,
    clock: fn() -> Timestamp,
}

impl Hub {
    /// A hub over `core`, with the system clock.
    #[must_use]
    pub fn new(core: MasterCore) -> Self {
        Self::with_clock(core, Timestamp::now)
    }

    /// A hub over `core` with another clock (tests).
    #[must_use]
    pub fn with_clock(core: MasterCore, clock: fn() -> Timestamp) -> Self {
        Self {
            core: Arc::new(Mutex::new(core)),
            streams: Arc::new(Mutex::new(BTreeMap::new())),
            clock,
        }
    }

    /// The core, for status and tests.
    #[must_use]
    pub fn core(&self) -> Arc<Mutex<MasterCore>> {
        Arc::clone(&self.core)
    }

    /// The gRPC service for a tonic server.
    #[must_use]
    pub fn service(&self) -> MasterServer<Self> {
        MasterServer::new(self.clone())
    }

    /// One tick of the master's loop, its messages delivered.
    ///
    /// # Errors
    ///
    /// The core's failure (the next tick goes on).
    pub async fn tick(&self) -> Result<(), CoreError> {
        let outgoing = self.core.lock().await.tick((self.clock)()).await?;
        self.deliver(outgoing).await;
        Ok(())
    }

    /// Runs the master's loop: a tick every `interval` until `stop` completes. A failing tick
    /// is logged; the next one goes on.
    pub async fn run(&self, interval: Duration, stop: impl Future<Output = ()>) {
        let mut ticks = tokio::time::interval(interval);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::pin!(stop);
        loop {
            tokio::select! {
                () = &mut stop => return,
                _ = ticks.tick() => {
                    if let Err(error) = self.tick().await {
                        tracing::error!(%error, "master tick failed");
                    }
                }
            }
        }
    }

    /// Sends messages to their workers' streams; one for a worker without a live stream is
    /// dropped (an assignment then expires without `Started`; a cancel is sent again when the
    /// worker comes back and reports the attempt in its journal).
    async fn deliver(&self, outgoing: Vec<Outgoing>) {
        let streams = self.streams.lock().await;
        for Outgoing { worker_id, message } in outgoing {
            if let Some(stream) = streams.get(&worker_id) {
                if stream.send(Ok(message)).await.is_err() {
                    tracing::warn!(worker_id, "worker stream closed; message dropped");
                }
            } else {
                tracing::warn!(worker_id, "no stream for worker; message dropped");
            }
        }
    }

    async fn serve_stream(
        self,
        worker_id: String,
        stream: Outbound,
        mut inbound: Streaming<pb::WorkerMessage>,
    ) {
        loop {
            let message = match inbound.message().await {
                Ok(Some(pb::WorkerMessage {
                    message: Some(message),
                })) => message,
                Ok(Some(pb::WorkerMessage { message: None })) => continue,
                Ok(None) => break,
                Err(status) => {
                    tracing::warn!(worker_id, %status, "worker stream failed");
                    break;
                }
            };
            let handled = self
                .core
                .lock()
                .await
                .handle(&worker_id, message, (self.clock)())
                .await;
            match handled {
                Ok(outgoing) => self.deliver(outgoing).await,
                Err(error) => tracing::error!(worker_id, %error, "worker message not handled"),
            }
        }
        self.core.lock().await.disconnected(&worker_id);
        let mut streams = self.streams.lock().await;
        if streams
            .get(&worker_id)
            .is_some_and(|s| s.same_channel(&stream))
        {
            streams.remove(&worker_id);
        }
    }
}

#[tonic::async_trait]
impl Master for Hub {
    type ControlStream = ReceiverStream<Result<pb::MasterMessage, Status>>;
    type FetchInputsStream = ReceiverStream<Result<pb::InputChunk, Status>>;

    async fn control(
        &self,
        request: Request<Streaming<pb::WorkerMessage>>,
    ) -> Result<Response<Self::ControlStream>, Status> {
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("the stream ended before `Register`"))?;
        let Some(worker_message::Message::Register(register)) = first.message else {
            return Err(Status::invalid_argument(
                "the first message must be `Register`",
            ));
        };
        let registered = self
            .core
            .lock()
            .await
            .register(&register, (self.clock)())
            .await;
        let (registered, outgoing) = registered.map_err(|error| match error {
            CoreError::Refused(reason) => Status::failed_precondition(reason),
            other => Status::internal(other.to_string()),
        })?;
        let (stream, receiver) = mpsc::channel(STREAM_BUFFER);
        let first = pb::MasterMessage {
            message: Some(master_message::Message::Registered(registered)),
        };
        stream
            .send(Ok(first))
            .await
            .map_err(|_| Status::internal("the stream closed at once"))?;
        self.streams
            .lock()
            .await
            .insert(register.worker_id.clone(), stream.clone());
        self.deliver(outgoing).await;
        tokio::spawn(
            self.clone()
                .serve_stream(register.worker_id, stream, inbound),
        );
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn upload_parse_result(
        &self,
        request: Request<pb::ParseResult>,
    ) -> Result<Response<pb::ParseResultAck>, Status> {
        let uploaded = self
            .core
            .lock()
            .await
            .upload(request.into_inner(), (self.clock)())
            .await;
        match uploaded {
            Ok(()) => Ok(Response::new(pb::ParseResultAck {})),
            Err(error @ CoreError::BadMessage { .. }) => {
                Err(Status::invalid_argument(error.to_string()))
            }
            Err(error) => Err(Status::internal(error.to_string())),
        }
    }

    async fn fetch_inputs(
        &self,
        request: Request<pb::FetchInputsRequest>,
    ) -> Result<Response<Self::FetchInputsStream>, Status> {
        let attempt_id = request.into_inner().attempt_id;
        let inputs = self
            .core
            .lock()
            .await
            .inputs(&attempt_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found(format!("no open attempt `{attempt_id}`")))?;
        let chunks: Vec<Vec<u8>> = inputs.chunks(INPUT_CHUNK).map(<[u8]>::to_vec).collect();
        let (sender, receiver) = mpsc::channel(chunks.len().max(1));
        for data in chunks {
            // The channel holds every chunk, so sending never waits.
            let _ = sender.try_send(Ok(pb::InputChunk { data }));
        }
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}
