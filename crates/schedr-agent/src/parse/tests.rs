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
    /// Waits this long, then goes on.
    Wait(Duration),
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
    answers: Arc<Mutex<Vec<AgentMessage>>>,
    refuse: bool,
}

struct FakeSession {
    steps: VecDeque<Step>,
    stops: Arc<Mutex<usize>>,
    answers: Arc<Mutex<Vec<AgentMessage>>>,
}

impl ParseSession for FakeSession {
    async fn next(&mut self) -> Result<RunnerMessage, ChannelError> {
        while let Some(Step::Wait(time)) = self.steps.front() {
            tokio::time::sleep(*time).await;
            self.steps.pop_front();
        }
        match self.steps.pop_front() {
            Some(Step::Send(message)) => Ok(message),
            Some(Step::Hang) => std::future::pending().await,
            Some(Step::OverMemory) => Err(ChannelError::OverMemory(
                "exceeded memory budget 100 MiB (130 MiB used)".to_owned(),
            )),
            Some(Step::Close | Step::Wait(_)) | None => Err(ChannelError::Closed),
        }
    }

    async fn send(&mut self, message: AgentMessage) -> Result<(), ChannelError> {
        tokio::task::yield_now().await; // a real session writes to the runner
        self.answers.lock().unwrap().push(message);
        Ok(())
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
            answers: Arc::clone(&self.answers),
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
        connections: Vec::new(),
        error: None,
        warnings: Vec::new(),
    })
}

fn finished() -> Step {
    Step::Send(RunnerMessage::ParseFinished)
}

fn ok(path: &str, dags: Vec<Dag>) -> ParsedFile {
    ParsedFile {
        script_roots: Vec::new(),
        path: path.to_owned(),
        dags,
        connections: Vec::new(),
        error: None,
        warnings: Vec::new(),
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
            connections: Vec::new(),
            error: Some(error.clone()),
            warnings: Vec::new(),
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

fn asks(reference: &str) -> Step {
    Step::Send(RunnerMessage::ScriptDeclaration {
        reference: reference.to_owned(),
    })
}

#[tokio::test(start_paused = true)]
async fn a_script_operator_is_answered_from_the_script_roots_copy_and_its_root_recorded() {
    let copy = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(copy.path().join("sales")).unwrap();
    std::fs::write(copy.path().join("sales/clean.py"), "").unwrap();
    std::fs::write(
        copy.path().join("sales/schedr.toml"),
        "[jobs.clean]\nscript = \"clean.py\"\ninterpreter = \"py\"\nargs = [\"--day\"]\n",
    )
    .unwrap();
    let scripts = BTreeMap::from([(
        "etl-scripts".to_owned(),
        ScriptRoot {
            configured: "/scripts/etl".into(),
            copy: copy.path().to_path_buf(),
        },
    )]);
    let mut runner = FakeRunner::with(vec![vec![
        started("a.py"),
        asks("etl-scripts:sales/schedr.toml#clean"),
        asks("/scripts/etl/sales/schedr.toml#gone"),
        asks("elsewhere:x/schedr.toml#clean"),
        parsed("a.py", vec![]),
        started("b.py"),
        parsed("b.py", vec![]),
        finished(),
    ]]);
    let files = parse_files_with(&mut runner, request(&["a.py", "b.py"]), PER_FILE, &scripts)
        .await
        .unwrap();
    // Every root it named counts, found or not (decision 304); a later file starts afresh.
    assert_eq!(files[0].script_roots, ["elsewhere", "etl-scripts"]);
    assert_eq!(files[1].script_roots, Vec::<String>::new());
    let answers = runner.answers.lock().unwrap().clone();
    let AgentMessage::ScriptDeclaration(declaration) = &answers[0] else {
        panic!("{answers:?}")
    };
    assert_eq!(
        (
            declaration.job.as_str(),
            declaration.script.as_str(),
            declaration.args.as_slice()
        ),
        (
            "etl-scripts:sales/schedr.toml#clean",
            "sales/clean.py",
            [schedr_model::value::Template::text("--day")].as_slice()
        )
    );
    let messages: Vec<String> = answers[1..]
        .iter()
        .map(|answer| match answer {
            AgentMessage::ScriptNotFound { message } => message.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        messages,
        [
            "`etl-scripts:sales/schedr.toml` declares no job `gone`",
            "script root `elsewhere` isn't on this worker (`SCHEDR_SCRIPT_PATH`)",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_files_questions_dont_restart_its_timeout() {
    // Each question comes within the timeout of the last, but the file's import takes longer.
    let mut steps = vec![started("a.py")];
    for _ in 0..4 {
        steps.push(Step::Wait(PER_FILE / 2));
        steps.push(asks("nowhere:x/schedr.toml#job"));
    }
    steps.push(parsed("a.py", vec![]));
    steps.push(finished());
    let mut runner = FakeRunner::with(vec![steps]);
    let files = parse_files(&mut runner, request(&["a.py"]), PER_FILE)
        .await
        .unwrap();
    assert_eq!(
        files[0].error.as_ref().map(|e| e.exception.as_str()),
        Some(TIMEOUT_EXCEPTION)
    );
    assert_eq!(
        files[0].script_roots,
        ["nowhere"],
        "asked before it timed out"
    );
}
