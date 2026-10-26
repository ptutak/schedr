//! Timers on a multi-thread runtime.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Waker};
use std::thread;
use std::time::Duration;

/// Timers keep firing on a busy multi-thread runtime.
///
/// Guards against RUSTSEC-2026-0384 (tokio 1.54.0's `Interval` race, yanked; fixed in 1.54.1).
/// `Interval::poll_tick` could drop a timer entry while the runtime's time driver was still
/// firing it, and the worker aborted inside `AtomicWaker`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timers_keep_firing_on_a_busy_multi_thread_runtime() {
    let handle = tokio::runtime::Handle::current();
    let stop = Arc::new(AtomicBool::new(false));
    // Poll outside the worker set. A worker stays parked and runs the time driver while these
    // threads sit inside `poll_tick`, which is the overlap that aborts on tokio 1.54.0.
    // One spinning thread per core, capped at 4: uncapped, they starve the timing-sensitive
    // integration tests (B4/B6) that run beside this one.
    let pollers = thread::available_parallelism().map_or(4, |cores| cores.get().min(4));
    let threads: Vec<_> = (0..pollers)
        .map(|_| {
            let handle = handle.clone();
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let _enter = handle.enter();
                let waker = Waker::noop();
                let mut cx = Context::from_waker(waker);
                let mut ticks: Vec<_> = (0..16)
                    .map(|_| {
                        let mut tick = tokio::time::interval(Duration::from_millis(1));
                        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        // The first tick is immediate and fires on this thread. Consume it so
                        // the next deadline is in the future and the driver thread fires it.
                        let _ = tick.poll_tick(&mut cx);
                        tick
                    })
                    .collect();
                // Ready ticks after that first one. The immediate tick does not count: a
                // driver that then stops firing must fail the floor below.
                let mut ready = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    for tick in &mut ticks {
                        if tick.poll_tick(&mut cx).is_ready() {
                            ready += 1;
                        }
                    }
                }
                ready
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_secs(2)).await;

    stop.store(true, Ordering::Relaxed);
    let counts: Vec<u64> = threads
        .into_iter()
        .map(|thread| thread.join().expect("a timer thread panicked"))
        .collect();
    // A loose floor, not a rate. Two seconds of 1 ms intervals produce far more when the
    // driver is alive; asserting a rate would flake under load.
    for (index, ready) in counts.iter().enumerate() {
        assert!(
            *ready >= 1,
            "timer thread {index} saw {ready} Ready ticks; the time driver stopped firing"
        );
    }
}
