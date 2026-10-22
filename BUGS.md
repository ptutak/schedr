# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that would have
caught it; git history keeps the record. Finding a bug doesn't mean fixing it at once: it
is logged here and its fix scheduled with the user. Not part of the documentation site.

## B6: role integration tests fail under disk load inside `make check`

- **Seen:** 2026-10-22, three times (M2.4, M2.5, M2.6): the first `make check` of a step failed 2–7
  `schedr::roles` tests at once, old and new alike ("waited too long for …", a worker found
  `lost` instead of `active`), with Postgres statements taking 3–11 s and SQLite reporting
  "database is locked". `make test-integration` on its own passed right after, twice each
  time, and `make check` passed when run again.
- **Likely cause:** the integration tests run right after the coverage run and share the
  disk with it; the role tests' short timings (a 4 s lease, a 1 s heartbeat) don't tolerate
  multi-second stalls. B4 may be the same thing.
- **Next:** measure the stalls (WSL2 disk), then decide between longer leases in the role
  tests, a retry of `cli_json` reads, or running the integration tests apart from coverage.

## B4: `runs trigger` in a roles integration test got no result once

- **Seen:** 2026-10-22, the first `make test-integration` of M2.3:
  `roles::a_worker_that_never_comes_back_is_lost_and_stays_listed` panicked in
  `Cluster::trigger` (`crates/schedr/tests/roles.rs`): `cli_json(["runs", "trigger", …])` was
  `None`, so the CLI exited non-zero, after 34.8 s; the whole run took 35 s instead of the
  usual 12 s.
- **Not reproduced** in the two runs right after (53/53 passed each time). Postgres had just
  been started by podman; `cli_json` doesn't keep the CLI's stderr, so the reason is unknown.
- **Next:** keep the CLI's stderr in the failure message, then check whether the master
  applied the command (LISTEN/NOTIFY or the polling fallback) under load.
