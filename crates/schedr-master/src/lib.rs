//! The schedr master: scheduling loop, DAG validation, command handling, gRPC server.
//!
//! So far: DAG discovery ([`discovery`]) with its checks of DAG JSON ([`validate`]), parse
//! jobs ([`parse_jobs`]) and the DAG list worked out from the parses ([`catalog`]).

pub mod catalog;
pub mod commands;
pub mod core;
pub mod discovery;
pub mod dispatch;
pub mod execution;
pub mod fire_times;
pub mod metrics;
pub mod parse_jobs;
pub mod planning;
pub mod progress;
pub mod scheduler;
pub mod service;
pub mod validate;
pub mod workers;
