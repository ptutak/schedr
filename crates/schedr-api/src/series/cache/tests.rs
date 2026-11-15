use super::*;

fn key(process: usize) -> Key {
    Key {
        totals: false,
        range: Range::Day,
        process: Some(format!("p{process}")),
    }
}

#[tokio::test]
async fn the_least_recently_used_key_goes_and_a_clock_gone_back_reads_again() {
    let cache = SeriesCache::default();
    let now: Timestamp = "2026-11-14T10:30:20Z".parse().unwrap();
    let range = (now, now);
    for p in 0..KEYS {
        cache.entry(key(p), range).await.final_to = Some(now);
    }
    // `p0` used again: `p1` is the least recently used when one more comes.
    assert_eq!(cache.entry(key(0), range).await.final_to, Some(now));
    drop(cache.entry(key(KEYS), range).await);
    assert_eq!(cache.entry(key(1), range).await.final_to, None);
    assert_eq!(cache.entry(key(0), range).await.final_to, Some(now));
    let before = now.checked_sub(SignedDuration::from_mins(1)).unwrap();
    assert_eq!(cache.entry(key(0), (before, before)).await.final_to, None);
}

#[tokio::test]
async fn a_key_a_request_holds_isnt_evicted() {
    // Codex's review of M5.1j: evicted while held, a second request for it made a second lock
    // beside the first, and read alongside it.
    let cache = SeriesCache::default();
    let now: Timestamp = "2026-11-14T10:30:20Z".parse().unwrap();
    let range = (now, now);
    let held = cache.entry(key(0), range).await;
    for p in 1..=KEYS {
        drop(cache.entry(key(p), range).await);
    }
    let second = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        cache.entry(key(0), range),
    )
    .await;
    assert!(second.is_err(), "the key's lock is still the held one");
    drop(held);
    drop(cache.entry(key(0), range).await);
}

#[test]
fn a_key_of_too_many_buckets_isnt_kept() {
    let now: Timestamp = "2026-11-14T10:30:20Z".parse().unwrap();
    let bucket = Bucket {
        process_id: "p".to_owned(),
        at: now,
        minutes: 1,
        resident_avg: 1,
        resident_peak: 1,
        memory_avg: 1,
        memory_peak: 1,
        cpu_avg: None,
        cpu_peak: None,
        container_avg: None,
        container_peak: None,
        container_limit: None,
        parts: None,
        covered: 1,
    };
    let mut entry = Entry::default();
    keep_buckets(&mut entry, &vec![bucket; BUCKETS + 1], (now, now));
    assert_eq!((entry.final_to, entry.buckets.len()), (None, 0));
    assert_eq!(entry.read_from(now, now), now);
}
