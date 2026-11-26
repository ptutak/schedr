use std::sync::Arc;

use schedr_proto::pb;
use tokio::sync::mpsc;
use tonic::Status;

use super::{Closing, Session, SessionWriter};

/// A session of `worker_id` numbered `id`, and what its stream receives.
fn session(
    worker_id: &str,
    id: u64,
) -> (
    Arc<Session>,
    mpsc::Receiver<Result<pb::MasterMessage, Status>>,
) {
    let (sender, receiver) = mpsc::channel(4);
    (Arc::new(Session::new(worker_id, id, sender)), receiver)
}

fn stamped() -> pb::MasterMessage {
    pb::MasterMessage {
        message: None,
        clock: Some(pb::MasterClock::default()),
    }
}

#[test]
fn an_installed_session_is_current_and_read_without_the_writer() {
    let mut writer = SessionWriter::default();
    let sessions = writer.sessions();
    let (first, _stream) = session("w1", 1);
    assert!(matches!(writer.install(&first), Ok(None)));
    assert!(sessions.has("w1"));
    assert!(sessions.is_current(&first));
    assert_eq!(sessions.current("w1").map(|s| s.id()), Some(1));
    assert!(!sessions.has("w2"));
}

#[test]
fn a_newer_session_replaces_and_ends_the_old_one() {
    let mut writer = SessionWriter::default();
    let sessions = writer.sessions();
    let (old, mut old_stream) = session("w1", 1);
    let (new, _stream) = session("w1", 2);
    writer.install(&old).unwrap();
    let replaced = writer.install(&new).unwrap();
    assert_eq!(replaced.map(|s| s.id()), Some(1));
    assert!(old.is_ended(), "the replaced one ends");
    assert!(!new.is_ended());
    assert!(!sessions.is_current(&old));
    assert!(sessions.is_current(&new));
    let ended = old_stream.try_recv().unwrap();
    assert_eq!(ended.unwrap_err().code(), tonic::Code::Aborted);
}

#[test]
fn a_session_is_taken_out_only_by_its_own_number() {
    let mut writer = SessionWriter::default();
    let sessions = writer.sessions();
    let (current, _stream) = session("w1", 2);
    writer.install(&current).unwrap();
    assert!(
        writer.remove("w1", Some(1)).is_none(),
        "an older stream's end changes nothing"
    );
    assert!(sessions.has("w1"));
    assert_eq!(writer.remove("w1", Some(2)).map(|s| s.id()), Some(2));
    assert!(!sessions.has("w1"));
    assert!(writer.remove("w1", None).is_none(), "none left");
    assert!(
        !current.is_ended(),
        "taken out, not replaced: its own server ends it"
    );
}

#[test]
fn any_session_is_taken_out_without_a_number() {
    let mut writer = SessionWriter::default();
    let (current, _stream) = session("w1", 7);
    writer.install(&current).unwrap();
    assert_eq!(writer.remove("w1", None).map(|s| s.id()), Some(7));
}

#[test]
fn closing_ends_every_session_and_refuses_new_ones() {
    let mut writer = SessionWriter::default();
    let sessions = writer.sessions();
    let (one, mut one_stream) = session("w1", 1);
    let (two, _two_stream) = session("w2", 2);
    writer.install(&one).unwrap();
    writer.install(&two).unwrap();
    sessions.close_all(&Status::unavailable("stopping"));
    assert!(sessions.is_closing());
    assert!(one.is_ended() && two.is_ended());
    assert!(!sessions.has("w1") && !sessions.has("w2"));
    assert_eq!(
        one_stream.try_recv().unwrap().unwrap_err().code(),
        tonic::Code::Unavailable
    );
    let (late, _late_stream) = session("w3", 3);
    assert!(matches!(writer.install(&late), Err(Closing)));
    assert!(!sessions.has("w3"), "nothing put in place once closing");
}

#[test]
fn a_registration_racing_the_close_is_closed_or_refused_never_left_open() {
    for round in 0..200 {
        let mut writer = SessionWriter::default();
        let sessions = writer.sessions();
        let (late, _stream) = session("w1", round);
        let closer = {
            let sessions = sessions.clone();
            std::thread::spawn(move || sessions.close_all(&Status::unavailable("stopping")))
        };
        let installed = writer.install(&late);
        closer.join().unwrap();
        match installed {
            Ok(_) => assert!(late.is_ended(), "installed first, then closed"),
            Err(Closing) => assert!(!sessions.has("w1")),
        }
        assert!(!sessions.has("w1"), "no session left open after closing");
    }
}

#[test]
fn messages_are_numbered_in_order_and_an_unsent_one_uses_no_number() {
    let (sender, mut receiver) = mpsc::channel(2);
    let one = Session::new("w1", 1, sender);
    assert_eq!(one.send(stamped()), Ok(1));
    assert_eq!(one.send(stamped()), Ok(2));
    assert_eq!(
        one.send(stamped()),
        Err(mpsc::error::TrySendError::Full(()))
    );
    let first = receiver.try_recv().unwrap().unwrap();
    assert_eq!(first.clock.unwrap().sequence, 1);
    assert_eq!(one.send(stamped()), Ok(3), "the full one's number not used");
    drop(receiver);
    assert_eq!(
        one.send(stamped()),
        Err(mpsc::error::TrySendError::Closed(()))
    );
}

#[test]
fn the_latest_heartbeat_heard_only_rises() {
    let mut writer = SessionWriter::default();
    let sessions = writer.sessions();
    let (current, _stream) = session("w1", 1);
    writer.install(&current).unwrap();
    assert_eq!(sessions.heard_on("w1"), 0);
    current.hear(5);
    current.hear(3);
    assert_eq!(sessions.heard_on("w1"), 5);
    assert_eq!(sessions.heard_on("w2"), 0, "no session");
}

#[test]
fn a_stuck_session_counts_as_gone_until_the_core_takes_it_out() {
    let mut writer = SessionWriter::default();
    let sessions = writer.sessions();
    let (stuck, _stream) = session("w1", 4);
    writer.install(&stuck).unwrap();
    stuck.hear(9);
    stuck.mark_stuck();
    assert!(!sessions.has("w1"));
    assert!(sessions.current("w1").is_none());
    assert!(!sessions.is_current(&stuck));
    assert_eq!(sessions.heard_on("w1"), 0);
    assert_eq!(
        writer.remove("w1", Some(4)).map(|s| s.id()),
        Some(4),
        "still the core's to take out"
    );
}

#[test]
fn a_session_replaced_out_ends_and_is_gone() {
    let mut writer = SessionWriter::default();
    let sessions = writer.sessions();
    let (old, mut stream) = session("w1", 1);
    writer.install(&old).unwrap();
    assert_eq!(writer.replace_out("w1").map(|s| s.id()), Some(1));
    assert!(old.is_ended());
    assert!(!sessions.has("w1"));
    assert_eq!(
        stream.try_recv().unwrap().unwrap_err().code(),
        tonic::Code::Aborted
    );
    assert!(writer.replace_out("w1").is_none());
}
