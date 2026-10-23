use std::time::{Duration, Instant};

use super::*;

#[test]
fn a_beat_says_how_long_ago_the_loop_turned() {
    let beat = Beat::default();
    let now = Instant::now();
    assert_eq!(beat.since(now), None);
    beat.beat_at(now);
    assert_eq!(
        beat.since(now + Duration::from_secs(3)),
        Some(Duration::from_secs(3))
    );
    // Clones share it.
    let other = beat.clone();
    other.beat_at(now + Duration::from_secs(5));
    assert_eq!(
        beat.since(now + Duration::from_secs(5)),
        Some(Duration::ZERO)
    );
    // A time before the beat counts as now.
    assert_eq!(beat.since(now), Some(Duration::ZERO));
    beat.beat();
    assert!(beat.since(Instant::now()).unwrap() < Duration::from_secs(1));
}
