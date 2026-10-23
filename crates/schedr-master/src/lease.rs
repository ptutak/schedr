//! Leading: one master per database (see PROJECT.md, Components and roles: production path).
//!
//! A master takes the database's lease before it starts (a second one waits as a standby),
//! renews it every third of [`Leadership::period`], and leads only while its last renewal is
//! recent enough **by its own monotonic clock**: a renewal begun at `t` makes it lead until
//! `t + period − query limit`, before the lease itself can expire by the database's clock (it
//! was set at or after `t`), so a master cut off from the database pauses before another can
//! take over.
//!
//! What rules out two masters writing at once is the database itself: the master writes
//! through [`Leadership::fenced_db`], whose every write checks the lease in its own
//! transaction (see `schedr_db::Database::fenced`), and is refused once another master took it
//! over, however late it arrives.
//!
//! A renewal that finds the lease no longer its own (another holder, or the takeover counter
//! moved on) means another master led meanwhile: this one's view is stale and it must end.
//! A renewal never takes the lease back.

use std::time::{Duration, Instant};

use schedr_db::{Database, DbError};

#[cfg(test)]
mod tests;

/// The default lease period (`[master] lease_period`).
pub const DEFAULT_LEASE_PERIOD: Duration = Duration::from_secs(30);

/// What a renewal found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Renewal {
    /// Still held: leading for the period less a query limit from the renewal.
    Held,
    /// The database didn't answer: leading only until the last renewal's time is up.
    Failed,
    /// Another master took over (or holds it): this one must end.
    TakenOver,
}

/// This master's hold on the lease.
#[derive(Debug)]
pub struct Leadership {
    db: Database,
    holder: String,
    period: Duration,
    epoch: i64,
    until: Instant,
}

impl Leadership {
    /// Waits until this master (`holder`, its incarnation) takes the lease for `period`,
    /// trying every third of it; `on_wait` is told once when another master holds it (a
    /// standby). The lease's table is created first if needed. Failures (the database not
    /// there yet, two masters creating the table at once) are logged and tried again.
    pub async fn take(
        db: Database,
        holder: String,
        period: Duration,
        on_wait: impl FnOnce(),
    ) -> Self {
        let mut on_wait = Some(on_wait);
        loop {
            let started = Instant::now();
            match Self::try_take_at(&db, &holder, period).await {
                Ok(Some(epoch)) => {
                    return Self {
                        until: started + lead_time(&db, period),
                        db,
                        holder,
                        period,
                        epoch,
                    };
                }
                Ok(None) => {
                    if let Some(on_wait) = on_wait.take() {
                        on_wait();
                    }
                }
                Err(error) => tracing::warn!(%error, "can't take the master's lease; trying again"),
            }
            tokio::time::sleep(period / 3).await;
        }
    }

    /// Takes the lease for `holder` if no other master holds it: `None` if one does.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the table can't be created or the lease read.
    pub async fn try_take(
        db: Database,
        holder: String,
        period: Duration,
    ) -> Result<Option<Self>, DbError> {
        let started = Instant::now();
        let epoch = Self::try_take_at(&db, &holder, period).await?;
        Ok(epoch.map(|epoch| Self {
            until: started + lead_time(&db, period),
            db,
            holder,
            period,
            epoch,
        }))
    }

    /// One try at the lease, the table created first: its takeover counter if taken.
    async fn try_take_at(
        db: &Database,
        holder: &str,
        period: Duration,
    ) -> Result<Option<i64>, DbError> {
        db.create_lease_table().await?;
        db.take_lease(holder, period).await
    }

    /// Renews the lease, begun at `now` (within the query limit).
    pub async fn renew(&mut self, now: Instant) -> Renewal {
        match self
            .db
            .renew_lease(&self.holder, self.epoch, self.period)
            .await
        {
            Ok(true) => {
                self.until = now + lead_time(&self.db, self.period);
                Renewal::Held
            }
            Ok(false) => Renewal::TakenOver,
            Err(error) => {
                tracing::warn!(%error, "the master's lease wasn't renewed");
                Renewal::Failed
            }
        }
    }

    /// Whether this master leads at `now` (its last renewal recent enough).
    #[must_use]
    pub fn leads(&self, now: Instant) -> bool {
        now < self.until
    }

    /// Until when this master leads unless renewed.
    #[must_use]
    pub const fn until(&self) -> Instant {
        self.until
    }

    /// The database for this master's work: its writes are refused once another master took
    /// the lease over.
    #[must_use]
    pub fn fenced_db(&self) -> Database {
        self.db.fenced(&self.holder, self.epoch)
    }

    /// The lease period.
    #[must_use]
    pub const fn period(&self) -> Duration {
        self.period
    }

    /// This master's name in the lease (its incarnation).
    #[must_use]
    pub fn holder(&self) -> &str {
        &self.holder
    }

    /// The takeover counter when this master took the lease.
    #[must_use]
    pub const fn epoch(&self) -> i64 {
        self.epoch
    }

    /// Gives the lease up (a clean shutdown): a standby takes over at once.
    pub async fn release(&self) {
        if let Err(error) = self.db.release_lease(&self.holder).await {
            tracing::warn!(%error, "the master's lease wasn't given up; it expires by itself");
        }
    }
}

/// How long a renewal lets a master lead: the period less the query limit, so a query begun
/// while leading ends before the lease can expire by the database's clock.
fn lead_time(db: &Database, period: Duration) -> Duration {
    period.saturating_sub(db.query_timeout())
}
