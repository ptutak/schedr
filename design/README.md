# design/: how schedr works now, by topic

One file per topic, `design/<topic>.md`, each in four parts:

1. **Current:** how it works now, in short statements, rewritten (not appended to) when a decision changes
   it. Each statement names its decisions (`D357`), so the reasons are one lookup away.
2. **Decisions:** the topic's decisions, newest first, one line each (`D357: ...`), as `DECISIONS.md` lists them.
3. **Where in code:** the files and functions that do it; check them before saying how it behaves.
4. **Open:** questions not decided yet, with the current proposal.

The other layers (see `AI_SETUP.md`, Project documents):

| File | Answers | Written |
|---|---|---|
| `STATUS.md` | where things stand, what's next | rewritten each session |
| `design/<topic>.md` | how a part works now | edited in place |
| `decisions/D<n>.toml`, `DECISIONS.md` | what was decided, whether it holds, where its reasons are | one file per decision; `DECISIONS.md` generated |
| `PROJECT.md` | how the project got here, and why | by topic up to D356; then the Journal, dated entries appended |

## Topics

`scripts/decisions.py check` takes a decision's `topic` only from this table.

| Topic | Covers | PROJECT.md sections (up to D356) |
|---|---|---|
| `goal` | what schedr is for, the deployment target | Goal; Deployment target |
| `components` | the roles (master, webserver, worker, standalone), how they fit | Architecture: Components and roles |
| `ui-cli` | the web UI, the user CLI, the REST API | Web UI and CLI |
| `task-logs` | task logs, their endpoint and proxy | Task logs |
| `code-delivery` | DAG roots, code delivery, versions, change detection, ID conflicts | DAG code delivery; DAG roots; DAG ID conflicts; Code versions and change detection |
| `scripts` | plain script delivery and script jobs | Plain script delivery |
| `protocol` | the master–worker protocol | Master–worker protocol |
| `workers` | worker lifecycle, memory protection, environment, drain | Worker lifecycle; Worker memory protection and crash recovery; Worker environment |
| `assignment` | task assignment, rejections, placement, queues | Task assignment; Task rejections |
| `task-processes` | Rust/Python integration, how tasks start (fork server), the runner | Rust/Python integration; How Python tasks are started |
| `task-context` | the in-task CLI, run parameters, `dag_ctx` / `task_ctx` | In-task CLI; Run parameters and the DAG context |
| `deployment` | ports, default size, upgrades and compatibility | Default deployment size; Ports; Upgrades and compatibility; Expected workload |
| `flow` | the end-to-end flow of a run | End-to-end flow |
| `observability` | metrics, health, logging | Observability |
| `configuration` | the configuration file, keys, env, hidden keys, timings | Configuration; Timings: public keys and hidden ones |
| `secrets` | secret storage and masking | Secret storage |
| `connections` | connections and their types | Connections |
| `state` | the database, retention | State storage; Retention |
| `python-api` | the native API's shape, how it maps to the engine | Python API structure; How the Python API works with the engine; Native API |
| `outcomes` | outcomes, failures, joins, task results | Outcomes, failures and joins; Task results and outcomes |
| `runs` | run kinds, manual runs, backfill, pausing, queueing | Runs: manual runs, backfill and pausing |
| `chained-runs` | `TriggerDagOperator`, dependencies between DAGs | Dependencies between DAGs |
| `async-tasks` | async task functions | Async task functions |
| `wait-steps` | sensors, wait steps, the watcher host, `first_of`, `delay` / `until` | Sensors and triggers (wait steps) |
| `triggers` | event triggers, `RunOf`, named events, `schedr.api` | Sensors and triggers (triggers, events) |
| `human-in-the-loop` | approvals (M6) | Human in the loop |
| `cleanup` | cleanup handles, resources | Cleanup and resources |
| `serialization` | output serialization, codecs | Output serialization |
| `testing-dags` | `dags check` / `dags test`, `run_dag`, `task_context` | Testing DAGs |
| `airflow` | the Airflow compatibility layer | Airflow compatibility |
| `development` | repository, tooling, tests, docs, libraries, CI, workflow, releases | Repository and tooling |
