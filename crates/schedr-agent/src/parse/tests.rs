use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use schedr_model::dag::Dag;
use serde_json::json;

use super::*;

const PER_FILE: Duration = Duration::from_secs(10);

/// What a fake runner does next.
#[derive(Debug, Clone)]
enum Step {
    Send(RunnerMessage),
    Hang,
    Close,
    OverMemory,
}

/// Each started session plays the next script; the requests and stops are recorded.
#[derive(Default)]
struct FakeRunner {
    scripts: VecDeque<Vec<Step>>,
    requests: Vec<Vec<String>>,
    stops: Arc<Mutex<usize>>,
    refuse: bool,
}

struct FakeSession {
    steps: VecDeque<Step>,
    stops: Arc<Mutex<usize>>,
}

impl ParseSession for FakeSession {
    async fn next(&mut self) -> Result<RunnerMessage, ChannelError> {
        match self.steps.pop_front() {
            Some(Step::Send(message)) => Ok(message),
            Some(Step::Hang) => std::future::pending().await,
            Some(Step::OverMemory) => Err(ChannelError::OverMemory(
                "exceeded memory budget 100 MiB (130 MiB used)".to_owned(),
            )),
            Some(Step::Close) | None => Err(ChannelError::Closed),
        }
    }

    async fn stop(self) {
        *self.stops.lock().unwrap() += 1;
    }
}

impl ParseRunner for FakeRunner {
    type Session = FakeSession;

    async fn start(&mut self, request: ParseRequest) -> Result<FakeSession, ChannelError> {
        tokio::task::yield_now().await; // a real runner starts a process here
        if self.refuse {
            return Err(ChannelError::Refused("versions differ".to_owned()));
        }
        self.requests.push(request.files);
        Ok(FakeSession {
            steps: self.scripts.pop_front().unwrap_or_default().into(),
            stops: Arc::clone(&self.stops),
        })
    }
}

impl FakeRunner {
    fn with(scripts: Vec<Vec<Step>>) -> Self {
        Self {
            scripts: scripts.into(),
            ..Self::default()
        }
    }

    fn stops(&self) -> usize {
        *self.stops.lock().unwrap()
    }
}

fn dag(id: &str, path: &str) -> Dag {
    serde_json::from_value(json!({
        "produced_by": "0.1",
        "id": id,
        "location": {"root": "etl", "path": path},
        "trigger": {"kind": "none"},
        "initial_since": null,
        "max_active_runs": 1,
        "tasks": [],
        "edges": []
    }))
    .unwrap()
}

fn request(files: &[&str]) -> ParseRequest {
    ParseRequest {
        root: "etl".to_owned(),
        root_dir: "/dags/etl".to_owned(),
        files: files.iter().map(|f| (*f).to_owned()).collect(),
        sys_path: vec!["/dags/etl".to_owned()],
    }
}

fn started(path: &str) -> Step {
    Step::Send(RunnerMessage::FileStarted {
        path: path.to_owned(),
    })
}

fn parsed(path: &str, dags: Vec<Dag>) -> Step {
    Step::Send(RunnerMessage::FileParsed {
        path: path.to_owned(),
        dags,
        error: None,
    })
}

fn finished() -> Step {
    Step::Send(RunnerMessage::ParseFinished)
}

fn ok(path: &str, dags: Vec<Dag>) -> ParsedFile {
    ParsedFile {
        path: path.to_owned(),
        dags,
        error: None,
    }
}

#[tokio::test(start_paused = true)]
async fn one_runner_parses_every_file() {
    let mut runner = FakeRunner::with(vec![vec![
        started("a.py"),
        parsed("a.py", vec![dag("a", "a.py")]),
        started("helpers.py"),
        parsed("helpers.py", vec![]),
        finished(),
    ]]);
    let files = parse_files(&mut runner, request(&["a.py", "helpers.py"]), PER_FILE)
        .await
        .unwrap();
    assert_eq!(
        files,
        [ok("a.py", vec![dag("a", "a.py")]), ok("helpers.py", vec![])]
    );
    assert_eq!(runner.requests, [["a.py", "helpers.py"]]);
    assert_eq!(runner.stops(), 1);
}

#[tokio::test(start_paused = true)]
async fn import_errors_are_kept_per_file() {
    let error = ErrorSummary {
        exception: "ImportError".to_owned(),
        message: "no module".to_owned(),
        traceback: "...".to_owned(),
    };
    let mut runner = FakeRunner::with(vec![vec![
        started("a.py"),
        Step::Send(RunnerMessage::FileParsed {
            path: "a.py".to_owned(),
            dags: vec![],
            error: Some(error.clone()),
        }),
        finished(),
    ]]);
    let files = parse_files(&mut runner, request(&["a.py"]), PER_FILE)
        .await
        .unwrap();
    assert_eq!(files[0].error, Some(error));
}

#[tokio::test(start_paused = true)]
async fn a_file_that_times_out_gets_an_error_and_a_new_runner_continues() {
    let mut runner = FakeRunner::with(vec![
        vec![
            started("a.py"),
            parsed("a.py", vec![]),
            started("slow.py"),
            Step::Hang,
        ],
        vec![started("c.py"), parsed("c.py", vec![]), finished()],
    ]);
    let files = parse_files(&mut runner, request(&["a.py", "slow.py", "c.py"]), PER_FILE)
        .await
        .unwrap();
    assert_eq!(files.len(), 3);
    assert_eq!(files[0], ok("a.py", vec![]));
    let error = files[1].error.as_ref().unwrap();
    assert_eq!(files[1].path, "slow.py");
    assert_eq!(error.exception, TIMEOUT_EXCEPTION);
    assert_eq!(
        error.message,
        "importing the file took longer than the parse timeout of 10 s"
    );
    assert_eq!(files[2], ok("c.py", vec![]));
    assert_eq!(
        runner.requests,
        [vec!["a.py", "slow.py", "c.py"], vec!["c.py"]]
    );
    assert_eq!(runner.stops(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_runner_that_dies_in_a_file_blames_that_file() {
    let mut runner = FakeRunner::with(vec![
        vec![started("crash.py"), Step::Close],
        vec![started("b.py"), parsed("b.py", vec![]), finished()],
    ]);
    let files = parse_files(&mut runner, request(&["crash.py", "b.py"]), PER_FILE)
        .await
        .unwrap();
    let error = files[0].error.as_ref().unwrap();
    assert_eq!(error.exception, RUNNER_DIED_EXCEPTION);
    assert_eq!(
        error.message,
        "the runner stopped while importing the file (the runner closed the connection)"
    );
    assert_eq!(files[1], ok("b.py", vec![]));
}

#[tokio::test(start_paused = true)]
async fn a_file_over_the_memory_budget_after_others_is_tried_alone_before_it_is_blamed() {
    let mut runner = FakeRunner::with(vec![
        // `b.py` crosses the budget with `a.py` still imported...
        vec![
            started("a.py"),
            parsed("a.py", vec![]),
            started("b.py"),
            Step::OverMemory,
        ],
        // ... is fine in a runner of its own, and `c.py` is over the budget by itself.
        vec![
            started("b.py"),
            parsed("b.py", vec![]),
            started("c.py"),
            Step::OverMemory,
        ],
        vec![started("c.py"), Step::OverMemory],
    ]);
    let files = parse_files(&mut runner, request(&["a.py", "b.py", "c.py"]), PER_FILE)
        .await
        .unwrap();
    assert_eq!(files[0], ok("a.py", vec![]));
    assert_eq!(files[1], ok("b.py", vec![]));
    let error = files[2].error.as_ref().unwrap();
    assert_eq!(
        error.message,
        "the runner stopped while importing the file (the runner exceeded memory budget 100 \
         MiB (130 MiB used))"
    );
    assert_eq!(
        runner.requests,
        [
            vec!["a.py", "b.py", "c.py"],
            vec!["b.py", "c.py"],
            vec!["c.py"]
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn the_last_file_timing_out_needs_no_new_runner() {
    let mut runner = FakeRunner::with(vec![vec![started("slow.py"), Step::Hang]]);
    let files = parse_files(&mut runner, request(&["slow.py"]), PER_FILE)
        .await
        .unwrap();
    assert_eq!(
        files[0].error.as_ref().unwrap().exception,
        TIMEOUT_EXCEPTION
    );
    assert_eq!(runner.requests.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn no_files_start_no_runner() {
    let mut runner = FakeRunner::default();
    assert_eq!(
        parse_files(&mut runner, request(&[]), PER_FILE)
            .await
            .unwrap(),
        []
    );
    assert_eq!(runner.requests, Vec::<Vec<String>>::new());
}

#[tokio::test(start_paused = true)]
async fn a_runner_that_cant_start_fails_the_job() {
    let mut runner = FakeRunner {
        refuse: true,
        ..FakeRunner::default()
    };
    let error = parse_files(&mut runner, request(&["a.py"]), PER_FILE)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "parse runner: runner refused: versions differ"
    );
}

#[tokio::test(start_paused = true)]
async fn silence_outside_a_file_fails_the_job() {
    let mut runner = FakeRunner::with(vec![vec![Step::Hang]]);
    let error = parse_files(&mut runner, request(&["a.py"]), PER_FILE)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "the parse runner didn't respond within 10s"
    );
    assert_eq!(runner.stops(), 1);
}

#[tokio::test(start_paused = true)]
async fn closing_outside_a_file_fails_the_job() {
    let mut runner = FakeRunner::with(vec![vec![
        started("a.py"),
        parsed("a.py", vec![]),
        Step::Close,
    ]]);
    let error = parse_files(&mut runner, request(&["a.py", "b.py"]), PER_FILE)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "parse runner: the runner closed the connection"
    );
}

#[tokio::test(start_paused = true)]
async fn messages_out_of_order_fail_the_job() {
    let cases = [
        (
            vec![started("b.py")],
            "expected file_started for the next file, got file_started",
        ),
        (
            vec![started("a.py"), parsed("b.py", vec![])],
            "expected file_parsed for the file started, got file_parsed",
        ),
        (
            vec![started("a.py"), parsed("a.py", vec![]), started("a.py")],
            "expected parse_finished, got file_started",
        ),
        (
            vec![finished()],
            "expected file_started for the next file, got parse_finished",
        ),
    ];
    for (script, message) in cases {
        let mut runner = FakeRunner::with(vec![script]);
        let error = parse_files(&mut runner, request(&["a.py"]), PER_FILE)
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), format!("parse runner: {message}"));
    }
}
