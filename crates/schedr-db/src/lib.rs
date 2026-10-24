//! Database schema, migrations and queries of schedr (SQLite and Postgres).
//!
//! All persistent state lives in the database (see PROJECT.md, State storage). The schema is
//! upgraded by `schedr db migrate`; the roles refuse to start against an outdated schema.

pub mod attempts;
pub mod backend;
pub mod commands;
pub mod dags;
pub mod database;
pub mod instances;
pub mod lease;
mod query;
pub mod reads;
pub mod runs;
mod scope;
pub mod status;
pub mod workers;

pub use backend::{Backend, UnsupportedUrl, redact_password};
pub use database::{Database, DbError, Pool};
pub use query::db_time;
pub use status::SchemaStatus;
