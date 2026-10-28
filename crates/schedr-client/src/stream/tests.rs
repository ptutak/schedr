use pretty_assertions::assert_eq;

use super::*;

fn event(name: &str, data: &str) -> ServerEvent {
    ServerEvent {
        name: name.to_owned(),
        data: data.to_owned(),
    }
}

/// The events read from `chunks`, sent in that order.
async fn read(chunks: &[&[u8]]) -> Vec<ServerEvent> {
    let (sender, chunks_in) = Chunks::channel();
    let owned: Vec<Vec<u8>> = chunks.iter().map(|c| c.to_vec()).collect();
    tokio::spawn(async move {
        for chunk in owned {
            sender.send(Ok(chunk)).await.unwrap();
        }
    });
    let mut reader = EventReader::new(chunks_in);
    let mut events = Vec::new();
    while let Some(event) = reader.next().await {
        events.push(event.unwrap());
    }
    events
}

#[tokio::test]
async fn events_are_read_across_chunks_with_comments_skipped() {
    let events = read(&[
        b": keep-alive\n\nevent: run\nda",
        b"ta: {\"run_id\": \"r1\"}\n\n",
        b"event:dag\r\ndata:{}\r\n\r\ndata: one\ndata: two\n\n",
        // A character cut between chunks (`é` is bytes 15 and 16).
        "event: n\ndata: \u{e9}\n\n".as_bytes().split_at(16).0,
        "event: n\ndata: \u{e9}\n\n".as_bytes().split_at(16).1,
        // Unfinished at the end: never an event.
        b"event: half",
    ])
    .await;
    assert_eq!(
        events,
        [
            event("run", "{\"run_id\": \"r1\"}"),
            event("dag", "{}"),
            event("message", "one\ntwo"),
            event("n", "\u{e9}"),
        ]
    );
}

#[tokio::test]
async fn a_failed_stream_is_an_error_then_its_end() {
    let (sender, chunks) = Chunks::channel();
    sender
        .send(Ok(b"event: a\ndata: 1\n\n".to_vec()))
        .await
        .unwrap();
    sender
        .send(Err(ClientError::Unexpected("cut".to_owned())))
        .await
        .unwrap();
    drop(sender);
    let mut reader = EventReader::new(chunks);
    assert_eq!(reader.next().await.unwrap().unwrap(), event("a", "1"));
    assert!(reader.next().await.unwrap().is_err());
    assert!(reader.next().await.is_none());
}
