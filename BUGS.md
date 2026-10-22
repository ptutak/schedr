# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that would have
caught it; git history keeps the record. Finding a bug doesn't mean fixing it at once: it
is logged here and its fix scheduled with the user. Not part of the documentation site.

## B3: the standalone integration tests pick ports that another test can take

- Found: 2026-10-22, in M2.2: `three_processes_on_postgres_run_a_dag` failed once with
  "Address already in use"; its worker then reached another test's server (HTTP 404).
- Cause: `free_port()` binds port 0, reads the port and releases it; a test running in
  parallel can bind it before the process under test does. `tests/roles.rs` now lets each
  role bind port 0 itself and reads the bound address from its log (fixed there in M2.2);
  `crates/schedr/tests/standalone.rs` still uses `free_port()` for standalone's two ports.
- Fix proposed: the same as in `tests/roles.rs`: ports 0 in the config, and the addresses read
  from standalone's banner (`schedr standalone: API on http://..., master on ...`).
- Plan: not scheduled yet (it hasn't failed there yet; more parallel tests make it likelier).
