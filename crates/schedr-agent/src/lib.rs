//! The schedr worker agent: process supervision, rendering, the runner channel.
//!
//! The agent starts each task attempt as its own process, in its own process group, with a
//! clean environment; it captures the output, enforces the timeout and stops processes with
//! SIGTERM then SIGKILL (see PROJECT.md, Worker environment and Master–worker protocol). The
//! Python runner talks to it over a Unix socket ([`channel`]).

pub mod agent;
pub mod attempt;
pub mod channel;
pub mod drain;
pub mod env;
pub mod journal;
pub mod launch;
pub mod output;
pub mod parse;
pub mod process;
pub mod render;
pub mod scan;
pub mod status;
pub mod supervise;
pub mod watch;
