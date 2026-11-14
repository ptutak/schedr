//! Database schema, migrations and queries of schedr (SQLite and Postgres).
//!
//! All persistent state lives in the database (see archive/PROJECT.md, State storage). The schema
//! is upgraded by `schedr db migrate`; the roles refuse to start against an outdated schema.

pub mod attempts;
pub mod backend;
pub mod backfills;
pub mod changes;
pub mod commands;
pub mod dags;
pub mod database;
#[cfg(any(test, feature = "testing"))]
mod faults;
pub mod instances;
pub mod lease;
pub mod named_events;
pub mod nodes;
pub mod notifications;
pub mod probe;
mod query;
pub mod reads;
pub mod retention;
pub mod run_of;
pub mod runs;
mod scope;
pub mod secrets;
pub mod status;
pub mod strikes;
pub mod triggers;
pub mod workers;

pub use backend::{Backend, UnsupportedUrl, redact_password};
pub use database::{Database, DbError, Pool};
pub use query::db_time;
pub use status::SchemaStatus;
