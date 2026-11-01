//! This process as a node of its deployment, for the Deployments page (decisions 121-123): the
//! process's ID, its host, and a master's heartbeat of its row (leader or standby).

use std::sync::OnceLock;
use std::time::Duration;

use jiff::Timestamp;
use schedr_db::Database;
use schedr_db::nodes::MasterNode;
use schedr_master::problems::Problems;
use tokio::sync::watch;

use crate::roles::until;

#[cfg(test)]
mod tests;

/// How often a master writes its row.
pub const MASTER_HEARTBEAT: Duration = Duration::from_secs(10);

/// This process's ID: made once, shared by the parts it runs (standalone's master and worker).
#[must_use]
pub fn process_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| uuid::Uuid::now_v7().to_string())
}

/// A new master's ID: the holder it takes the lease as.
#[must_use]
pub fn new_master_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// This host's name (`unknown` if it isn't valid UTF-8 or is empty).
#[must_use]
pub fn host_name() -> String {
    let name = rustix::system::uname();
    let name = name.nodename().to_string_lossy();
    if name.is_empty() {
        "unknown".to_owned()
    } else {
        name.into_owned()
    }
}

/// This process's webserver as a node of its deployment, with `config` (masked).
#[must_use]
pub fn webserver_node(config: serde_json::Value) -> schedr_api::views::NodeView {
    schedr_api::views::NodeView {
        process_id: process_id().to_owned(),
        host: host_name(),
        schedr_version: env!("CARGO_PKG_VERSION").to_owned(),
        config,
    }
}

/// A master's identity as its heartbeat writes it.
#[derive(Debug, Clone)]
pub struct MasterIdentity {
    /// Its ID (its lease holder's).
    pub master_id: String,
    /// Its effective configuration, masked.
    pub config: serde_json::Value,
    /// Its problems now (decision 129), written with each beat.
    pub problems: Problems,
}

/// Writes `master`'s row every [`MASTER_HEARTBEAT`] on `db` (not fenced: a standby writes too)
/// until `stop`. A write that fails is logged once, until one succeeds; not while the schema
/// isn't usable yet (it is being migrated).
pub async fn heartbeat_master(db: Database, master: MasterIdentity, stop: watch::Receiver<u32>) {
    let host = host_name();
    let started_at = Timestamp::now();
    let mut failing = false;
    let beats = async {
        loop {
            let problems = master.problems.now();
            let node = MasterNode {
                master_id: &master.master_id,
                process_id: process_id(),
                host: &host,
                schedr_version: env!("CARGO_PKG_VERSION"),
                config: &master.config,
                started_at,
                problems: &problems,
            };
            match db.heartbeat_master(&node, Timestamp::now()).await {
                Ok(()) => failing = false,
                Err(error) => {
                    let usable = db
                        .schema_status()
                        .await
                        .is_ok_and(|status| status.is_usable());
                    if usable && !failing {
                        tracing::warn!(%error, "can't write this master's row for the Deployments page");
                        failing = true;
                    }
                }
            }
            tokio::time::sleep(MASTER_HEARTBEAT).await;
        }
    };
    tokio::select! {
        () = beats => {}
        () = until(stop) => {}
    }
}
