//! The schedr master: scheduling loop, DAG validation, command handling, gRPC server.
//!
//! So far: DAG discovery ([`discovery`]) with its checks of DAG JSON ([`validate`]), parse
//! jobs ([`parse_jobs`]) and the DAG list worked out from the parses ([`catalog`]).

pub mod catalog;
pub mod clock;
pub mod commands;
pub mod core;
pub mod discovery;
pub mod dispatch;
pub mod execution;
#[cfg(test)]
mod faults;
pub mod fire_times;
pub mod lease;
pub mod limits;
pub mod mapping;
pub mod metrics;
pub mod notifications;
pub mod params;
pub mod parse_jobs;
pub mod planning;
pub mod problems;
pub mod progress;
pub mod quiet;
pub mod retention;
pub mod scheduler;
pub mod service;
pub mod validate;
pub mod workers;
