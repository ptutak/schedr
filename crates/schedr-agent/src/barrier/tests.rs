use std::time::Duration;

use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;

use super::*;

const WAIT: Duration = Duration::from_millis(500);

/// A listener on a fresh socket, and its path.
fn listening() -> (UnixListener, PathBuf) {
    let path = socket_path().unwrap();
    (UnixListener::bind(&path).unwrap(), path)
}

/// A process at the barrier: connects, reads the worker's word, replies with `reply` to a `g`.
async fn process(path: PathBuf, reply: u8) -> Option<u8> {
    let mut stream = UnixStream::connect(&path).await.unwrap();
    let mut word = [0_u8; 1];
    match stream.read(&mut word).await {
        Ok(1) => {
            if word[0] == b'g' {
                stream.write_all(&[reply]).await.unwrap();
            }
            Some(word[0])
        }
        _ => None,
    }
}

fn answered(go: bool) -> impl FnOnce() -> oneshot::Receiver<bool> {
    move || {
        let (answer, answered) = oneshot::channel();
        answer.send(go).unwrap();
        answered
    }
}

#[tokio::test]
async fn the_masters_go_lets_the_waiting_process_through() {
    let (listener, path) = listening();
    let process = tokio::spawn(process(path, b'p'));
    assert_eq!(
        exchange(&listener, WAIT, answered(true)).await,
        Word::Passed
    );
    assert_eq!(process.await.unwrap(), Some(b'g'));
}

#[tokio::test]
async fn the_masters_no_keeps_it_shut_and_says_so() {
    let (listener, path) = listening();
    let process = tokio::spawn(process(path, b'p'));
    assert_eq!(
        exchange(&listener, WAIT, answered(false)).await,
        Word::Refused
    );
    // The process read the connection closed: no `g`.
    assert_eq!(process.await.unwrap(), None);
}

#[tokio::test]
async fn no_word_in_time_keeps_it_shut_and_counts_no_try() {
    let (listener, path) = listening();
    let process = tokio::spawn(process(path, b'p'));
    // The agent's loop never answers (no stream): the oneshot is dropped.
    let never = || oneshot::channel::<bool>().1;
    assert_eq!(exchange(&listener, WAIT, never).await, Word::Unanswered);
    assert_eq!(process.await.unwrap(), None);
}

#[tokio::test]
async fn a_go_the_process_got_too_late_isnt_a_pass() {
    // Its own wait over as the `g` came: it replies `l` and runs nothing.
    let (listener, path) = listening();
    let process = tokio::spawn(process(path, b'l'));
    assert_eq!(
        exchange(&listener, WAIT, answered(true)).await,
        Word::Unanswered
    );
    assert_eq!(process.await.unwrap(), Some(b'g'));
}

#[tokio::test]
async fn a_process_that_never_waits_is_a_failed_start() {
    let (listener, _path) = listening();
    let asked = std::sync::atomic::AtomicBool::new(false);
    let word = exchange(&listener, Duration::from_millis(50), || {
        asked.store(true, std::sync::atomic::Ordering::SeqCst);
        oneshot::channel().1
    })
    .await;
    assert_eq!(word, Word::NeverWaited);
    assert!(
        !asked.load(std::sync::atomic::Ordering::SeqCst),
        "the master is asked only once the process waits"
    );
}
