use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

#[test]
fn the_process_id_is_made_once() {
    assert_eq!(process_id(), process_id());
    assert_ne!(new_master_id(), new_master_id());
    assert_ne!(host_name(), "");
}

#[tokio::test]
async fn a_master_heartbeats_its_row_until_stopped() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let (stop, stopped) = watch::channel(0);
    let config = json!({"database": {"url": "postgres://schedr:****@db/s"}});
    let beating = tokio::spawn(heartbeat_master(
        db.clone(),
        MasterIdentity {
            master_id: "m-1".to_owned(),
            config: config.clone(),
        },
        stopped,
    ));
    let read = loop {
        let masters = db.masters(Timestamp::UNIX_EPOCH).await.unwrap();
        if !masters.is_empty() {
            break masters;
        }
        tokio::task::yield_now().await;
    };
    assert_eq!(read[0].master_id, "m-1");
    assert_eq!(read[0].process_id, process_id());
    assert_eq!(read[0].config, config);
    assert_eq!(read[0].host, host_name());
    stop.send(1).unwrap();
    beating.await.unwrap();
}
