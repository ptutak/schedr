//! The schedr worker agent: process supervision, rendering, the runner channel.
//!
//! The agent starts each task attempt as its own process, in its own process group, with a
//! clean environment; it captures the output, enforces the timeout and stops processes with
//! SIGTERM then SIGKILL (see PROJECT.md, Worker environment and Master–worker protocol). The
//! Python runner talks to it over a Unix socket ([`channel`]).

pub mod channel;
pub mod env;
pub mod output;
pub mod process;
pub mod render;
pub mod status;
pub mod supervise;
