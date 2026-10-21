use pretty_assertions::assert_eq;
use schedr_model::runner::ParseRequest;
use tokio::io::{AsyncWriteExt, duplex};

use super::*;

fn hello(version: &str) -> RunnerMessage {
    RunnerMessage::Hello {
        schedr_version: version.into(),
        mode: RunnerMode::Parse,
    }
}

#[tokio::test]
async fn a_message_is_its_length_then_its_json() {
    let (mut a, mut b) = duplex(1024);
    write_message(&mut a, &AgentMessage::Welcome).await.unwrap();
    let mut header = [0; 4];
    tokio::io::AsyncReadExt::read_exact(&mut b, &mut header)
        .await
        .unwrap();
    let body = br#"{"type":"welcome"}"#;
    assert_eq!(
        usize::try_from(u32::from_be_bytes(header)).unwrap(),
        body.len()
    );
    let mut received = vec![0; body.len()];
    tokio::io::AsyncReadExt::read_exact(&mut b, &mut received)
        .await
        .unwrap();
    assert_eq!(&received, body);
}

#[tokio::test]
async fn messages_round_trip() {
    let (mut a, mut b) = duplex(1024);
    let request = AgentMessage::Parse(ParseRequest {
        root: "r".into(),
        root_dir: "/r".into(),
        files: vec!["a.py".into()],
        sys_path: vec![],
    });
    write_message(&mut a, &request).await.unwrap();
    write_message(&mut a, &AgentMessage::Welcome).await.unwrap();
    let first: AgentMessage = read_message(&mut b).await.unwrap();
    let second: AgentMessage = read_message(&mut b).await.unwrap();
    assert_eq!((first, second), (request, AgentMessage::Welcome));
}

#[tokio::test]
async fn reading_a_closed_stream_reports_closed() {
    let (a, mut b) = duplex(64);
    drop(a);
    let err = read_message::<_, RunnerMessage>(&mut b).await.unwrap_err();
    assert!(matches!(err, ChannelError::Closed), "{err:?}");
    assert_eq!(err.to_string(), "the runner closed the connection");
}

#[tokio::test]
async fn a_message_cut_short_reports_closed() {
    let (mut a, mut b) = duplex(64);
    a.write_all(&10u32.to_be_bytes()).await.unwrap();
    a.write_all(b"{\"ty").await.unwrap();
    drop(a);
    let err = read_message::<_, RunnerMessage>(&mut b).await.unwrap_err();
    assert!(matches!(err, ChannelError::Closed), "{err:?}");
}

#[tokio::test]
async fn oversize_and_invalid_messages_are_rejected() {
    let (mut a, mut b) = duplex(64);
    a.write_all(&(MAX_MESSAGE_SIZE + 1).to_be_bytes())
        .await
        .unwrap();
    let err = read_message::<_, RunnerMessage>(&mut b).await.unwrap_err();
    assert!(matches!(err, ChannelError::TooLarge(n) if n == u64::from(MAX_MESSAGE_SIZE) + 1));

    let (mut a, mut b) = duplex(64);
    a.write_all(&5u32.to_be_bytes()).await.unwrap();
    a.write_all(b"{\"x\":").await.unwrap();
    let err = read_message::<_, RunnerMessage>(&mut b).await.unwrap_err();
    assert!(matches!(err, ChannelError::Invalid(_)), "{err:?}");
    assert!(err.to_string().starts_with("invalid message: "));
}

#[tokio::test]
async fn writing_an_oversize_message_is_rejected() {
    let (mut a, _b) = duplex(64);
    let huge = AgentMessage::Refused {
        reason: "x".repeat(MAX_MESSAGE_SIZE as usize),
    };
    let err = write_message(&mut a, &huge).await.unwrap_err();
    assert!(matches!(err, ChannelError::TooLarge(_)), "{err:?}");
}

#[tokio::test]
async fn a_runner_with_the_same_version_is_welcomed() {
    let (mut agent, mut runner) = duplex(1024);
    write_message(&mut runner, &hello("0.1.0")).await.unwrap();
    let mode = accept_runner(&mut agent, "0.1.0").await.unwrap();
    assert_eq!(mode, RunnerMode::Parse);
    let reply: AgentMessage = read_message(&mut runner).await.unwrap();
    assert_eq!(reply, AgentMessage::Welcome);
}

#[tokio::test]
async fn a_runner_with_another_version_is_refused_with_the_reason() {
    let (mut agent, mut runner) = duplex(1024);
    write_message(&mut runner, &hello("0.2.0")).await.unwrap();
    let err = accept_runner(&mut agent, "0.1.0").await.unwrap_err();
    let ChannelError::Refused(reason) = &err else {
        panic!("expected Refused, got {err:?}")
    };
    assert!(reason.contains("install schedr==0.1.0"), "{reason}");
    let reply: AgentMessage = read_message(&mut runner).await.unwrap();
    assert_eq!(
        reply,
        AgentMessage::Refused {
            reason: reason.clone()
        }
    );
}

#[tokio::test]
async fn the_first_message_must_be_hello() {
    let (mut agent, mut runner) = duplex(1024);
    write_message(&mut runner, &RunnerMessage::ParseFinished)
        .await
        .unwrap();
    let err = accept_runner(&mut agent, "0.1.0").await.unwrap_err();
    assert_eq!(err.to_string(), "expected hello, got parse_finished");
}

#[test]
fn io_errors_are_wrapped() {
    let err = ChannelError::from(std::io::Error::other("broken"));
    assert_eq!(err.to_string(), "runner channel: broken");
}
