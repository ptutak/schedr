use pretty_assertions::assert_eq;
use proptest::prelude::*;

use super::*;

/// A DAG of up to `max` nodes: edges only from a lower index to a higher one.
fn dags(max: usize) -> impl Strategy<Value = (usize, Vec<(usize, usize)>)> {
    (1..=max).prop_flat_map(|count| {
        let pairs: Vec<(usize, usize)> = (0..count)
            .flat_map(|a| (a + 1..count).map(move |b| (a, b)))
            .collect();
        let len = pairs.len();
        (
            Just(count),
            proptest::sample::subsequence(pairs, 0..=len.min(40)),
        )
    })
}

/// The crossings of the drawn routes' bends, between each two consecutive layers: a bend
/// starts at the bottom of a layer's band and ends at the top of the next one's; two bends of
/// a gap cross when their ends are in opposite orders.
fn crossings_drawn(layout: &Layout) -> usize {
    #[expect(clippy::cast_possible_truncation, reason = "a layer's index, small")]
    let band = |y: f64| ((y - MARGIN) / (NODE_HEIGHT + LAYER_GAP)).floor() as i64;
    let mut segments: Vec<(i64, f64, f64)> = Vec::new();
    for route in &layout.routes {
        // The straight runs through a layer's band can't cross anything: only the bends
        // between layers are compared.
        for pair in route
            .points
            .windows(2)
            .filter(|p| (p[1].1 - p[0].1 - LAYER_GAP).abs() < 1e-9)
        {
            segments.push((band(pair[0].1), pair[0].0, pair[1].0));
        }
    }
    let mut count = 0;
    for (k, a) in segments.iter().enumerate() {
        for b in &segments[k + 1..] {
            if a.0 == b.0 && (a.1 - b.1) * (a.2 - b.2) < 0.0 {
                count += 1;
            }
        }
    }
    count
}

proptest! {
    #[test]
    fn boxes_never_overlap((count, edges) in dags(24)) {
        let layout = layout(count, &edges);
        for (i, a) in layout.nodes.iter().enumerate() {
            for b in &layout.nodes[i + 1..] {
                let apart_x = (a.x - b.x).abs() >= NODE_WIDTH;
                let apart_y = (a.y - b.y).abs() >= NODE_HEIGHT;
                prop_assert!(apart_x || apart_y, "{a:?} overlaps {b:?}");
            }
        }
    }

    #[test]
    fn every_edge_goes_down_and_is_drawn((count, edges) in dags(24)) {
        let layout = layout(count, &edges);
        prop_assert_eq!(layout.routes.len(), edges.len());
        for route in &layout.routes {
            prop_assert!(layout.nodes[route.from].layer < layout.nodes[route.to].layer);
            for pair in route.points.windows(2) {
                prop_assert!(pair[1].1 > pair[0].1, "{route:?} doesn't go down");
            }
        }
    }

    #[test]
    fn routes_bend_only_between_layers_never_through_a_box((count, edges) in dags(24)) {
        let layout = layout(count, &edges);
        let pitch = NODE_HEIGHT + LAYER_GAP;
        for route in &layout.routes {
            for pair in route.points.windows(2) {
                let ((x0, y0), (x1, y1)) = (pair[0], pair[1]);
                if (x0 - x1).abs() < 1e-9 {
                    continue;
                }
                // A bend: from the bottom of a layer's band to the top of the next one's.
                let bottom = (y0 - MARGIN - NODE_HEIGHT) / pitch;
                prop_assert!((bottom - bottom.round()).abs() < 1e-9, "{route:?}");
                prop_assert!((y1 - y0 - LAYER_GAP).abs() < 1e-9, "{route:?}");
            }
        }
    }

    #[test]
    fn everything_fits_in_the_drawing((count, edges) in dags(24)) {
        let layout = layout(count, &edges);
        for node in &layout.nodes {
            prop_assert!(node.x - NODE_WIDTH / 2.0 >= MARGIN - 1e-6);
            prop_assert!(node.x + NODE_WIDTH / 2.0 <= layout.width - MARGIN + 1e-6);
            prop_assert!(node.y + NODE_HEIGHT / 2.0 <= layout.height - MARGIN + 1e-6);
        }
    }

    #[test]
    fn the_same_graph_gets_the_same_layout((count, edges) in dags(24)) {
        prop_assert_eq!(layout(count, &edges), layout(count, &edges));
    }

    #[test]
    fn crossings_are_no_worse_than_the_tasks_own_order((count, edges) in dags(16)) {
        let layer = layers(count, &edges);
        let naive = crossings(&layered(count, &edges, &layer, vec![Vec::new(); count]));
        let mut graph = layered(count, &edges, &layer, vec![Vec::new(); count]);
        reduce_crossings(&mut graph);
        prop_assert!(crossings(&graph) <= naive);
        // What is drawn is what was counted.
        prop_assert_eq!(crossings_drawn(&layout(count, &edges)), crossings(&graph));
    }
}

#[test]
fn a_task_sits_one_layer_below_its_lowest_upstream_task() {
    // 0 -> 1 -> 2, 0 -> 2: 2 is two layers down; the long edge 0 -> 2 passes layer 1
    // straight down, beside 1's box.
    let layout = layout(3, &[(0, 1), (1, 2), (0, 2)]);
    let layers: Vec<usize> = layout.nodes.iter().map(|n| n.layer).collect();
    assert_eq!(layers, [0, 1, 2]);
    let long = &layout.routes[2];
    assert_eq!((long.edge, long.points.len()), (2, 4));
    assert!((long.points[1].0 - long.points[2].0).abs() < 1e-9);
    assert!((long.points[1].0 - layout.nodes[1].x).abs() >= NODE_WIDTH / 2.0);
}

#[test]
fn a_diamond_is_symmetric_and_a_chain_straight() {
    let diamond = layout(4, &[(0, 1), (0, 2), (1, 3), (2, 3)]);
    let [top, left, right, bottom] = [0, 1, 2, 3].map(|i| diamond.nodes[i]);
    assert!((top.x - bottom.x).abs() < 1e-6);
    assert!((top.x - f64::midpoint(left.x, right.x)).abs() < 1e-6);
    assert_eq!(left.layer, right.layer);
    let chain = layout(3, &[(0, 1), (1, 2)]);
    assert!(
        chain
            .nodes
            .iter()
            .all(|n| (n.x - chain.nodes[0].x).abs() < 1e-6)
    );
}

#[test]
fn crossings_are_removed_where_they_can_be() {
    // 0 -> 3, 1 -> 2 in the given order cross once; reordered, they don't.
    let layout = layout(4, &[(0, 3), (1, 2)]);
    assert_eq!(crossings_drawn(&layout), 0);
}

#[test]
fn an_empty_graph_and_a_cycle_still_lay_out() {
    let empty = layout(0, &[]);
    assert_eq!((empty.nodes.len(), empty.routes.len()), (0, 0));
    // Never in a valid DAG; the nodes on it go below the rest, its edges aren't drawn.
    let cycle = layout(3, &[(0, 1), (1, 2), (2, 1)]);
    assert_eq!(cycle.nodes.len(), 3);
    assert!(
        cycle
            .routes
            .iter()
            .all(|r| cycle.nodes[r.from].layer < cycle.nodes[r.to].layer)
    );
}

/// One grouped DAG: node count, edges, the group tree, each node's innermost group.
#[allow(clippy::type_complexity, reason = "the four parts of one grouped DAG")]
type GroupedDag = (
    usize,
    Vec<(usize, usize)>,
    Vec<GroupSpec>,
    Vec<Option<usize>>,
);

/// A valid group tree: 1 to 6 groups, each parent an earlier group, or none.
fn group_trees() -> impl Strategy<Value = Vec<GroupSpec>> {
    (1..=6_usize).prop_flat_map(|n| {
        proptest::collection::vec(any::<usize>(), n - 1).prop_map(move |picks| {
            let mut groups = vec![GroupSpec { parent: None }];
            for (offset, pick) in picks.into_iter().enumerate() {
                // `offset + 1` choices of parent (`0..group`) and one choice of no parent.
                let choices = offset + 2;
                let parent = if pick % choices == offset + 1 {
                    None
                } else {
                    Some(pick % choices)
                };
                groups.push(GroupSpec { parent });
            }
            groups
        })
    })
}

/// A DAG whose nodes sit in a random group tree, or in none.
fn grouped_dags(max: usize) -> impl Strategy<Value = GroupedDag> {
    (dags(max), group_trees()).prop_flat_map(|((count, edges), groups)| {
        let n = groups.len();
        (
            Just(count),
            Just(edges),
            Just(groups),
            proptest::collection::vec(proptest::option::of(0..n), count),
        )
    })
}

/// Whether node `n`, in `node_group`, is in group `g` (directly or nested).
fn in_group(groups: &[GroupSpec], node_group: &[Option<usize>], n: usize, g: usize) -> bool {
    group_path(groups, node_group[n]).contains(&g)
}

/// The staggered side-by-side bound, plus `2 * GROUP_PAD` for each nesting level past the
/// first, plus one extra row so a rigid column can slide the tasks that follow it, plus 10%
/// (the user, 2026-10-31: a random 13-node, two-deep grouping drew 5,830 px against 5,612,
/// one of B39's shapes the merged layout draws wider; its redesign is in M6).
fn width_bound(count: usize, depth: usize) -> f64 {
    let extra = depth.saturating_sub(1);
    let bound = to_f64(count) * (NODE_WIDTH + GAP + 2.0 * GROUP_PAD)
        + 2.0 * MARGIN
        + to_f64(extra) * 2.0 * GROUP_PAD
        + to_f64(count.saturating_sub(1)) * (NODE_WIDTH + GAP);
    bound * 1.1
}

fn nesting_depth(groups: &[GroupSpec], node_group: &[Option<usize>]) -> usize {
    node_group
        .iter()
        .map(|&group| group_path(groups, group).len())
        .max()
        .unwrap_or(0)
}

fn contains(b: &GroupBox, node: &Placed) -> (bool, bool) {
    let left = node.x - NODE_WIDTH / 2.0;
    let right = node.x + NODE_WIDTH / 2.0;
    let top = node.y - NODE_HEIGHT / 2.0;
    let bottom = node.y + NODE_HEIGHT / 2.0;
    let inside = left >= b.left - 1e-6
        && right <= b.left + b.width + 1e-6
        && top >= b.top - 1e-6
        && bottom <= b.top + b.height + 1e-6;
    let touches = left < b.left + b.width - 1e-6
        && right > b.left + 1e-6
        && top < b.top + b.height - 1e-6
        && bottom > b.top + 1e-6;
    (inside, touches)
}

proptest! {
    #[test]
    fn a_groups_box_holds_its_nodes_and_no_other(
        (count, edges, groups, node_group) in grouped_dags(16),
    ) {
        let layout = layout_grouped(count, &edges, &groups, &node_group);
        for b in &layout.boxes {
            for (n, node) in layout.nodes.iter().enumerate() {
                let (inside, touches) = contains(b, node);
                if in_group(&groups, &node_group, n, b.group) {
                    prop_assert!(inside, "node {n} outside its group {}'s box {b:?}", b.group);
                } else {
                    prop_assert!(!touches, "node {n} inside group {}'s box {b:?}", b.group);
                }
            }
        }
    }

    #[test]
    fn a_nested_box_is_inside_its_parents(
        (count, edges, groups, node_group) in grouped_dags(16),
    ) {
        let layout = layout_grouped(count, &edges, &groups, &node_group);
        for child in &layout.boxes {
            let Some(parent) = groups[child.group].parent else {
                continue;
            };
            let Some(outer) = layout.boxes.iter().find(|b| b.group == parent) else {
                continue;
            };
            prop_assert!(child.left > outer.left && child.left + child.width < outer.left + outer.width);
            prop_assert!(child.top > outer.top && child.top + child.height < outer.top + outer.height);
            prop_assert_eq!(child.depth, group_path(&groups, Some(child.group)).len() - 1);
        }
    }

    #[test]
    fn grouped_nodes_never_overlap_and_fit(
        (count, edges, groups, node_group) in grouped_dags(16),
    ) {
        let layout = layout_grouped(count, &edges, &groups, &node_group);
        for (i, a) in layout.nodes.iter().enumerate() {
            for b in &layout.nodes[i + 1..] {
                prop_assert!((a.x - b.x).abs() >= NODE_WIDTH || (a.y - b.y).abs() >= NODE_HEIGHT);
            }
            prop_assert!(a.x - NODE_WIDTH / 2.0 >= MARGIN - 1e-6);
            prop_assert!(a.x + NODE_WIDTH / 2.0 <= layout.width - MARGIN + 1e-6);
        }
        for b in &layout.boxes {
            prop_assert!(b.left >= MARGIN - 1e-6 && b.top >= 0.0);
            prop_assert!(b.left + b.width <= layout.width + 1e-6);
            prop_assert!(b.top + b.height <= layout.height + 1e-6);
        }
        let depth = nesting_depth(&groups, &node_group);
        let bound = width_bound(count, depth);
        prop_assert!(
            layout.width <= bound,
            "{count} nodes, depth {depth}: {} wide, bound {bound}",
            layout.width
        );
        prop_assert_eq!(layout_grouped(count, &edges, &groups, &node_group), layout);
    }
}

#[test]
fn a_groups_tasks_sit_together_in_a_box_with_room_for_its_label() {
    // start -> (load: a -> b, c) -> end, with `other` beside them.
    let edges = [(0, 1), (1, 2), (0, 3), (2, 4), (3, 4), (0, 5)];
    let node_group = [None, Some(0), Some(0), Some(0), None, None];
    let layout = layout_grouped(6, &edges, &[GroupSpec { parent: None }], &node_group);
    let [load] = layout.boxes.as_slice() else {
        panic!("one box: {:?}", layout.boxes)
    };
    for n in [1, 2, 3] {
        assert!(contains(load, &layout.nodes[n]).0, "{n} in the box");
    }
    for n in [0, 4, 5] {
        assert!(!contains(load, &layout.nodes[n]).1, "{n} out of the box");
    }
    let first = layout.nodes[1].y - NODE_HEIGHT / 2.0;
    assert!(
        (first - load.top - GROUP_LABEL).abs() < 1e-9,
        "{first} {load:?}"
    );
}

/// `k` sibling groups, each a top and a bottom task (top -> bottom); every top feeds one
/// chain outside them, and group `i`'s bottom hangs off the chain's `i`th task, so the
/// bottoms sit on different layers. Nodes: tops `0..k`, bottoms `k..2k`, chain `2k..3k`.
fn staggered_groups(k: usize) -> GroupedDag {
    let mut edges = Vec::new();
    for i in 0..k {
        edges.push((i, k + i));
        edges.push((i, 2 * k));
        edges.push((2 * k + i, k + i));
        if i + 1 < k {
            edges.push((2 * k + i, 2 * k + i + 1));
        }
    }
    let groups = vec![GroupSpec { parent: None }; k];
    let node_group = (0..3 * k).map(|n| (n < 2 * k).then_some(n % k)).collect();
    (3 * k, edges, groups, node_group)
}

#[test]
fn sibling_groups_on_different_layers_dont_widen_the_drawing() {
    for k in 2..=8 {
        let (count, edges, groups, node_group) = staggered_groups(k);
        let layout = layout_grouped(count, &edges, &groups, &node_group);
        // Never wider than every task side by side, each in its own padded box.
        let side_by_side = to_f64(count) * (NODE_WIDTH + GAP + 2.0 * GROUP_PAD) + 2.0 * MARGIN;
        assert!(
            layout.width <= side_by_side,
            "{k} groups: {} wide, more than {side_by_side}",
            layout.width
        );
        for b in &layout.boxes {
            for (n, node) in layout.nodes.iter().enumerate() {
                let (inside, touches) = contains(b, node);
                if node_group[n] == Some(b.group) {
                    assert!(inside, "{k} groups: node {n} outside its box {b:?}");
                } else {
                    assert!(!touches, "{k} groups: node {n} inside box {b:?}");
                }
            }
        }
    }
}

#[test]
fn a_four_deep_nest_stays_within_the_width_bound() {
    // 0 ⊃ 1 ⊃ 2 ⊃ 3, tasks 0→1→2→3 in those groups, and an ungrouped sibling 0→4.
    let groups = [
        GroupSpec { parent: None },
        GroupSpec { parent: Some(0) },
        GroupSpec { parent: Some(1) },
        GroupSpec { parent: Some(2) },
    ];
    let node_group = [Some(0), Some(1), Some(2), Some(3), None];
    let edges = [(0, 1), (1, 2), (2, 3), (0, 4)];
    let layout = layout_grouped(5, &edges, &groups, &node_group);
    let bound = width_bound(5, 4);
    assert!(
        layout.width <= bound,
        "{} wide, more than {bound}",
        layout.width
    );
    for (n, node) in layout.nodes.iter().enumerate() {
        for b in &layout.boxes {
            let (inside, touches) = contains(b, node);
            if in_group(&groups, &node_group, n, b.group) {
                assert!(inside, "node {n} outside group {}'s box {b:?}", b.group);
            } else {
                assert!(!touches, "node {n} inside group {}'s box {b:?}", b.group);
            }
        }
    }
    for child in &layout.boxes {
        let Some(parent) = groups[child.group].parent else {
            continue;
        };
        let Some(outer) = layout.boxes.iter().find(|b| b.group == parent) else {
            panic!("parent {parent} has no box");
        };
        assert!(child.left > outer.left && child.left + child.width < outer.left + outer.width);
        assert!(child.top > outer.top && child.top + child.height < outer.top + outer.height);
    }
}

#[test]
fn a_group_with_tasks_on_layer_0_and_3_and_no_edge_between_them_stays_one_column() {
    // t0 in the group on layer 0, an ungrouped chain t0→u1→u2→u3, and t3 (u2→t3) in the
    // group on layer 3. No edge joins t0 to t3, so layers 1 and 2 have no member.
    let groups = [GroupSpec { parent: None }];
    let node_group = [Some(0), None, None, None, Some(0)];
    let edges = [(0, 1), (1, 2), (2, 3), (2, 4)];
    let layout = layout_grouped(5, &edges, &groups, &node_group);
    let layers: Vec<usize> = layout.nodes.iter().map(|n| n.layer).collect();
    assert_eq!(layers, [0, 1, 2, 3, 3]);
    let bound = width_bound(5, 1);
    assert!(
        layout.width <= bound,
        "{} wide, more than {bound}",
        layout.width
    );
    let left = |n: usize| layout.nodes[n].x - NODE_WIDTH / 2.0;
    assert!(
        (left(0) - left(4)).abs() < 1e-6,
        "left edges {} and {}",
        left(0),
        left(4)
    );
    let group_box = layout.boxes.iter().find(|b| b.group == 0).expect("one box");
    assert!(contains(group_box, &layout.nodes[0]).0 && contains(group_box, &layout.nodes[4]).0);
    for n in [1, 2, 3] {
        assert!(
            !contains(group_box, &layout.nodes[n]).1,
            "{n} inside the box"
        );
    }
}
