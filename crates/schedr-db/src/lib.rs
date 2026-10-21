//! Database schema, migrations and queries of schedr (SQLite and Postgres).
//!
//! All persistent state lives in the database (see PROJECT.md, State storage). The schema is
//! upgraded by `schedr db migrate`; the roles refuse to start against an outdated schema.

pub mod backend;
pub mod dags;
pub mod database;
pub mod status;

pub use backend::{Backend, UnsupportedUrl, redact_password};
pub use database::{Database, DbError};
pub use status::SchemaStatus;
