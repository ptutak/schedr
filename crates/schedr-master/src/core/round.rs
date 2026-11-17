//! The proof of being in a round (M5.2cB, decision 505): only the round moves runs forward, so
//! what it reads before the core lock stays true until its writes. Every function that changes
//! an instance's or a run's state asks for an [`InRound`], and only this module makes one: a
//! writer outside the round doesn't compile.

use schedr_proto::pb::worker_message;

use super::{CoreError, Group, MasterCore, RoundPart};
use crate::dispatch::Outgoing;
use jiff::Timestamp;

/// Held only by a round's parts (see the module): passed to what moves runs forward. Holds
/// nothing.
#[derive(Debug)]
pub struct InRound(());

impl InRound {
    /// The round's own: made where a round part starts, in this module only (not `core.rs`,
    /// which holds registration and messages too).
    const fn start() -> Self {
        Self(())
    }

    /// For the tests that drive the dispatcher or the execution without a round (this crate's,
    /// and others' through the `testing` feature).
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub const fn for_tests() -> Self {
        Self(())
    }
}

/// The round's ways into the core: each starts an [`InRound`] for what it does.
impl MasterCore {
    /// One pass of the master's loop (tests and the standalone tick): every part in order.
    ///
    /// # Errors
    ///
    /// The first failure; what was done before it stays done, and the next tick goes on.
    pub async fn tick(&mut self, now: Timestamp) -> Result<Vec<Outgoing>, CoreError> {
        self.tick_in(&InRound::start(), now).await
    }

    /// As [`Self::tick`], what was sent before a failure kept.
    pub async fn tick_partial(&mut self, now: Timestamp) -> (Vec<Outgoing>, Result<(), CoreError>) {
        self.tick_partial_in(&InRound::start(), now).await
    }

    /// One part of a round.
    ///
    /// # Errors
    ///
    /// The part's failure.
    pub async fn round_part(
        &mut self,
        part: RoundPart,
        now: Timestamp,
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), CoreError> {
        self.round_part_in(&InRound::start(), part, now, outgoing)
            .await
    }

    /// One command of the round's commands part.
    ///
    /// # Errors
    ///
    /// The command's failure.
    pub async fn apply_command(
        &mut self,
        pending: schedr_db::commands::PendingCommand,
        outgoing: &mut Vec<Outgoing>,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        self.apply_command_in(&InRound::start(), pending, outgoing, now)
            .await
    }

    /// The round's batch of workers' `Started`s and `Finished`s, in one transaction.
    ///
    /// # Errors
    ///
    /// The batch handed back, its memory put back and nothing of it counted: the caller
    /// handles each worker's stretch as a group, then message by message.
    pub async fn handle_results(
        &mut self,
        batch: Vec<(String, Group)>,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, Vec<(String, Group)>> {
        self.handle_results_in(&InRound::start(), batch, now).await
    }

    /// One worker's stretch of a batch that failed, as a group.
    ///
    /// # Errors
    ///
    /// The group handed back, its memory put back and nothing of it counted: the caller
    /// handles its messages one by one.
    pub async fn handle_group(
        &mut self,
        worker_id: &str,
        group: Group,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, Group> {
        self.handle_group_in(&InRound::start(), worker_id, group, now)
            .await
    }

    /// A worker's message the round handles alone: as [`Self::handle`], and what moves runs
    /// forward too (a result, a rejection, a task's end, a deregistration).
    ///
    /// # Errors
    ///
    /// As [`Self::handle`].
    pub async fn handle_in_round(
        &mut self,
        worker_id: &str,
        message: worker_message::Message,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        self.handle_in(Some(&InRound::start()), worker_id, message, now)
            .await
    }
}
