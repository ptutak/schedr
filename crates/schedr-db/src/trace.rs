//! The round's trace, for profiling (the user, 2026-11-19): with `SCHEDR_HIDDEN__MASTER__ROUND_TRACE`
//! set to a file, each timed point writes a line `<name>\t<nanoseconds>` to it; off, a point
//! costs one check of a flag set once. Names are `<part>/<point>`; a point timed in the database
//! layer (a transaction's start or commit) takes the part it runs under, set by [`in_part`] for a
//! round's part, or `other` (a worker's turn, the trigger loop, the API).
//!
//! The file is buffered and flushed at each round's end ([`flush`]), so a process killed loses at
//! most one round's lines.

use std::io::Write as _;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// The trace's file, if tracing: opened once, from the environment.
fn sink() -> Option<&'static Mutex<std::io::BufWriter<std::fs::File>>> {
    static SINK: OnceLock<Option<Mutex<std::io::BufWriter<std::fs::File>>>> = OnceLock::new();
    SINK.get_or_init(|| {
        let path = std::env::var_os("SCHEDR_HIDDEN__MASTER__ROUND_TRACE")?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok()?;
        Some(Mutex::new(std::io::BufWriter::new(file)))
    })
    .as_ref()
}

/// Whether the round is traced.
#[must_use]
pub fn on() -> bool {
    sink().is_some()
}

tokio::task_local! {
    /// The round's part this task's work is timed under.
    static PART: &'static str;
}

/// Runs `work` as part `part` of a round: the database's points in it are named after it.
pub async fn in_part<F: std::future::Future>(part: &'static str, work: F) -> F::Output {
    if on() {
        PART.scope(part, work).await
    } else {
        work.await
    }
}

/// The part this task's work is timed under: `other` outside a round's part.
#[must_use]
pub fn part() -> &'static str {
    PART.try_with(|part| *part).unwrap_or("other")
}

/// Writes one measurement of `name`.
pub fn record(name: &str, took: Duration) {
    if let Some(sink) = sink() {
        let mut sink = sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = writeln!(sink, "{name}\t{}", took.as_nanos());
    }
}

/// As [`record`], the name `<part>/<point>` with this task's part.
pub fn record_in_part(point: &str, took: Duration) {
    if on() {
        record(&format!("{}/{point}", part()), took);
    }
}

/// Flushes what was written (each round's end).
pub fn flush() {
    if let Some(sink) = sink() {
        let mut sink = sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = sink.flush();
    }
}

/// Times from now until dropped, as `name`.
#[must_use]
pub fn span(name: &'static str) -> Span {
    Span {
        name,
        started: on().then(Instant::now),
    }
}

/// A point being timed ([`span`]).
#[derive(Debug)]
pub struct Span {
    name: &'static str,
    started: Option<Instant>,
}

impl Drop for Span {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            record(self.name, started.elapsed());
        }
    }
}

/// Times `work` as `name`.
pub async fn timed<F: std::future::Future>(name: &'static str, work: F) -> F::Output {
    let _span = span(name);
    work.await
}

/// Times from now until dropped, as `<part>/<point>` with this task's part ([`in_part`]).
#[must_use]
pub fn point(point: &'static str) -> Point {
    Point {
        point,
        started: on().then(Instant::now),
    }
}

/// A point of a part being timed ([`point`]).
#[derive(Debug)]
pub struct Point {
    point: &'static str,
    started: Option<Instant>,
}

impl Drop for Point {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            record_in_part(self.point, started.elapsed());
        }
    }
}

/// Times `work` as `<part>/<point>`.
pub async fn timed_point<F: std::future::Future>(point: &'static str, work: F) -> F::Output {
    let _point = self::point(point);
    work.await
}
