//! What a schedr process uses (decision 463): its memory and CPU and its container's, and a
//! worker's memory by part. Reported by masters (their row), workers (their heartbeat) and the
//! webserver (its `/deployment` answer).

use serde::{Deserialize, Serialize};

/// A schedr process's memory and CPU, and its container's.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeUsage {
    /// Resident memory, in bytes.
    pub resident_bytes: u64,
    /// CPU time used (user and system) since it started, in seconds.
    pub cpu_seconds: f64,
    /// Its cgroup's memory now (`memory.current`); none outside a cgroup v2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_bytes: Option<u64>,
    /// The smallest `memory.max` on the way up from its cgroup; none when unlimited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_limit_bytes: Option<u64>,
}

/// A worker's memory by part: the agent's own process, then what it started.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerUsage {
    /// The agent's process (in standalone also the master's and the webserver's).
    pub process: NodeUsage,
    /// Its fork servers', together.
    pub fork_servers_bytes: u64,
    /// Its wait step hosts', together.
    pub watcher_hosts_bytes: u64,
    /// Its running tasks', together, as their memory watchdog last measured them.
    pub tasks_bytes: u64,
    /// The largest of them, largest first.
    #[serde(default)]
    pub largest_tasks: Vec<TaskMemory>,
}

/// A running task's memory, named.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskMemory {
    /// The attempt.
    pub attempt_id: String,
    /// Its DAG.
    pub dag_id: String,
    /// Its run.
    pub run_id: String,
    /// Its task.
    pub task_id: String,
    /// Its map index; none for an unmapped task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub map_index: Option<u32>,
    /// Its memory, in bytes.
    pub bytes: u64,
}
