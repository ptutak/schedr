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
        let naive = crossings(&layered(count, &edges, &layer));
        let mut graph = layered(count, &edges, &layer);
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
