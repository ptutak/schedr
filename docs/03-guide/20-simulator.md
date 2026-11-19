# The clock simulator

The clock simulator is a deterministic test of the rules that keep a task from running twice
when machines, clocks and networks misbehave (decision 446). Faults that break those rules
need rare timings: a message held up just long enough, a clock drifting the wrong way, a
master replaced at the wrong moment. Real processes almost never hit them, and when they do,
the run can't be repeated. The simulator runs the real code on virtual clocks instead, picks
the faults at random from a seed, and checks the rules at every event. A failure is
reproduced exactly from its scenario, and shrunk to the smallest one that still fails.

The rules it checks are those of [The master clock](17-master-clock.md) and
[Guarantees](18-guarantees.md). The code is in `crates/schedr-agent/tests/clock_sim.rs` (the
worker's side and the tests) and `crates/schedr-agent/tests/clock_sim/master.rs` (the master's
side).

## Two halves

| Half | What runs for real | What is simulated | Cases |
|---|---|---|---|
| The worker's side | the worker's `MasterTime` | a master on a virtual clock; the worker's boot and wall clocks, with drift and stalls | 512 a run |
| The master's side | the master's core (`MasterCore`) over an in-memory SQLite database, and each worker's `MasterTime` | two workers, the streams between them and the master, what the service does on a stream | 12 a push, 600 a night |

**The worker's side** varies rate drift up to 0.1 %, delays each way up to 5 s, answers lost,
and stalls the boot clock doesn't count (a frozen VM). Heartbeats are also lost on their way to
the master while the master's messages still arrive. At every event it checks that a worker not
yet cut off is within a lease of when the master last heard it.

**The master's side** runs one virtual clock for everything. Master time is that clock
exactly; each worker's clocks drift from it. The service's part sits between the core and the
workers as the real one does: each message stamped with master time and the heartbeats heard,
heartbeats answered at once, arrivals noted before the core handles them. The two workers are
models of the agent: they register, report their tasks, keep the master-time cutoff, stop what
they must when cut off, and with a waiting queue (decision 509) hold an assignment past their
slot until it frees. They are not the agent's code.

The DAG has two tasks with retries, run side by side: one with a `cleanup_anywhere` cleanup,
which a worker cut off stops (decision 252), and an ordinary one, which it keeps running. A
scenario's workers have four slots, or one slot and a waiting queue of one.

## The faults

A scenario is a seed, the two workers' drift, a list of timed faults, the timings, and whether
the workers keep a waiting queue. The seed picks every message's delay: mostly under 50 ms,
now and then up to 3 s.

| Fault | What happens |
|---|---|
| Cut | A worker's link holds messages up one way or both, for 0.5 to 40 s |
| Lag | A worker's link slows: each message up and each down takes longer, for 1 to 30 s (a congested link, a reader behind) |
| Drop | A worker's stream breaks, both sides seeing it |
| Replace | A new incarnation of a worker starts; the old one dies, or closes its stream and runs on cut off (a pod replaced while its process still runs) |
| Master | The master is replaced: the deposed one still answers heartbeats for up to 4 s, and the new one leads after a gap (a takeover or a restart) |

It runs with one of two sets of timings, both on a 1 s heartbeat: a lease of ten heartbeats
and a cancel grace of two (what the aimed scenarios are tuned to), or the defaults' ratios (a
lease of six heartbeats, a cancel grace of three).

## The rules

Checked as it runs, and once the faults are over:

1. **No start after expiry.** When a worker starts an attempt, the master hasn't ended it.
2. **No attempt open forever.** Once the faults are over and the workers have been connected a
   while, every attempt and run has ended.
3. **No retry within a replaced incarnation's hold** (decision 443), and
4. **a moved cleanup never beside its old worker:** no two processes of one task instance (its
   attempts, a moved cleanup) run at the same time, on any worker or incarnation. The ordinary
   task may run again only beside an attempt the master ended `lost`.

## Each safeguard is needed

Three safeguards can be switched off for a test (`Weakened` in
`crates/schedr-master/src/dispatch.rs`). For each, scenarios aimed at it must break a rule;
if they don't, the safeguard is either no longer needed or no longer tested.

| Safeguard | Switched off | Test |
|---|---|---|
| The echo (B240) | An attempt whose start wasn't heard counts as never started | `without_the_echo_a_rule_breaks` |
| The hold (decision 443) | A replaced incarnation's attempts end at once | `without_the_hold_a_rule_breaks` |
| The move wait (decisions 256 and 445) | A lost worker's cleanup moves at once | `without_the_move_wait_a_rule_breaks` |

Removing a bound from the worker's side, or its drift allowance, makes that half fail at once
too.

## Running it

Every push runs both halves: `make check` runs them with the other tests, the master's side on
the same twelve scenarios from a fixed seed (446), since each runs the core for minutes of
virtual time. Alone:

```sh
cargo nextest run -p schedr-agent --test clock_sim
```

The Stress workflow runs the master's side every night on 600 fresh scenarios, whether `main`
changed or not:

```sh
PROPTEST_CASES=600 cargo nextest run -p schedr-agent --test clock_sim --no-capture \
  -E 'test(the_master_and_its_workers_keep_the_rules_whatever_the_faults)'
```

A failure prints the smallest scenario it shrank to: the seed, the drift, the faults with their
times, the timings and whether the workers queue. Copied into a test of its own, it replays
exactly. The simulator keeps the runs that found bugs that way
(`an_assignment_started_as_its_worker_was_cut_off_isnt_run_again_beside_it` for B234,
`an_assignment_whose_worker_cant_report_isnt_started_again_beside_it` for B240,
`attempts_end_after_lags_drops_and_a_takeover`), next to a run with no faults
(`without_faults_every_run_ends_and_no_rule_breaks`).

## Known limits

- Master time is the ideal clock: the worker's side checks drift against it.
- On the master's side, workers drift but don't stall. A stall the boot clock misses shows only
  once the wall clock has run 1 s (plus 0.2 % of the time since the last sample) ahead of it,
  so a shorter one leaves the worker that much late; the worker's side checks the rules with
  that allowance.
- Outside a cut, a stream holds a message up at most 3 s (decision 450 counts four heartbeat
  intervals for a `Started` on its way).
- A deposed master only answers heartbeats: its writes would fail on the lease.
- The workers are a model, not the agent. Where they differ, the simulator tests the model:
  B265 records one such difference, the simulated worker closing its stream at the
  master-time cutoff while the agent keeps it.
