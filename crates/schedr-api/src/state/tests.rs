use pretty_assertions::assert_eq;

use super::*;

async fn state(identity_header: Option<&str>) -> ApiState {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    ApiState::new(db, identity_header.map(str::to_owned))
}

#[tokio::test]
async fn the_state_carries_this_builds_version() {
    let state = state(None).await;
    assert_eq!(state.version, SchedrVersion::current());
    assert_eq!(state.version_text, env!("CARGO_PKG_VERSION"));
}
