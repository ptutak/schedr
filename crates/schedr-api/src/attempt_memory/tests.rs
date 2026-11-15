use pretty_assertions::assert_eq;

use super::*;

fn peaks(task: &str, peaks: &[u64]) -> TaskPeaks {
    TaskPeaks {
        task_id: task.to_owned(),
        peaks: peaks.to_vec(),
        oom_killed: 0,
    }
}

#[test]
fn the_p95_is_the_nearest_rank() {
    assert_eq!(p95(&mut []), None);
    assert_eq!(p95(&mut [7]), Some(7));
    // 20 peaks: the 19th smallest.
    let mut twenty: Vec<u64> = (1..=20).rev().collect();
    assert_eq!(p95(&mut twenty), Some(19));
    // 21: rank 20 (19.95 up).
    let mut more: Vec<u64> = (1..=21).collect();
    assert_eq!(p95(&mut more), Some(20));
}

#[test]
fn tasks_closest_to_their_limit_come_first_then_the_rest_by_p95() {
    let limits = [
        ("far".to_owned(), Some(1000)),
        ("near".to_owned(), Some(100)),
        ("small".to_owned(), None),
        ("big".to_owned(), None),
    ];
    let ranked = ranked(
        vec![
            peaks("big", &[500, 400]),
            peaks("far", &[100]),
            peaks("gone", &[50]),
            peaks("near", &[90, 10]),
            peaks("small", &[5]),
        ],
        &limits,
    );
    let order: Vec<_> = ranked
        .iter()
        .map(|t| (t.task_id.as_str(), t.p95_bytes, t.max_bytes, t.of_limit))
        .collect();
    assert_eq!(
        order,
        [
            ("near", 90, 90, Some(0.9)),
            ("far", 100, 100, Some(0.1)),
            ("big", 500, 500, None),
            // A task no longer in the DAG: no limit.
            ("gone", 50, 50, None),
            ("small", 5, 5, None),
        ]
    );
    assert_eq!(ranked[0].attempts, 2);
}
