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
    for p in 0..KEYS {
        cache.entry(key(p), now).await.final_to = Some(now);
    }
    // `p0` used again: `p1` is the least recently used when one more comes.
    assert_eq!(cache.entry(key(0), now).await.final_to, Some(now));
    drop(cache.entry(key(KEYS), now).await);
    assert_eq!(cache.entry(key(1), now).await.final_to, None);
    assert_eq!(cache.entry(key(0), now).await.final_to, Some(now));
    let before = now.checked_sub(SignedDuration::from_mins(1)).unwrap();
    assert_eq!(cache.entry(key(0), before).await.final_to, None);
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
    keep_buckets(&mut entry, &vec![bucket; BUCKETS + 1], now);
    assert_eq!((entry.final_to, entry.buckets.len()), (None, 0));
    assert_eq!(entry.read_from(now, now), now);
}
