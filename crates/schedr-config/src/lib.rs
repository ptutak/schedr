//! Configuration loading and validation for schedr (TOML file, environment variables, flags).
//!
//! See PROJECT.md, Configuration: one TOML file and/or `SCHEDR_CONFIG__*` environment
//! variables mirroring its structure, with `SCHEDR_DAG_PATH` / `SCHEDR_SCRIPT_PATH` as short
//! aliases; precedence flags > environment > file. Loading is pure: the caller reads the file
//! and the process environment and passes them in.

pub mod config;
pub mod loader;
pub mod roots;
pub mod sources;
pub mod units;

pub use config::{
    Config, DagsSection, DatabaseSection, Interpreter, LogFormat, LogSection, OpsSection,
    ScriptsSection, TlsMode, TlsSection, config_schema, url_has_password,
};
pub use loader::{ConfigError, load};
pub use roots::{PathList, Root, RootError};
pub use sources::{ConfigFile, FileSource, Override, SourceError, config_file};
pub use units::{UnitError, format_duration, format_size, parse_duration, parse_size};
