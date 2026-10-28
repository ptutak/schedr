//! The run graph's layout: a layered (Sugiyama) layout of our own (decision 22A), top to
//! bottom.
//!
//! 1. **Layers:** each task one layer below its lowest upstream task (the longest path from a
//!    task without upstream ones).
//! 2. **Long edges** get a point in every layer they cross (dummy nodes), so they are routed
//!    between the tasks rather than through them.
//! 3. **Crossings:** layers are reordered by the mean position of their neighbours
//!    (barycenters), sweeping down and up; the order with the fewest crossings seen is kept,
//!    the tasks' own order first, so the result is never worse than that.
//! 4. **x:** each node as near its neighbours' mean as the order and the gaps allow (a
//!    least-squares fit under those constraints, by pool-adjacent-violators), sweeping down
//!    and up; then shifted so the leftmost node starts at the margin.
//!
//! Deterministic: the same graph always gets the same layout.

#[cfg(test)]
mod tests;

/// A task's box: width.
pub const NODE_WIDTH: f64 = 180.0;
/// A task's box: height.
pub const NODE_HEIGHT: f64 = 44.0;
/// The space between two boxes side by side.
const GAP: f64 = 32.0;
/// The space next to a long edge passing a layer.
const DUMMY_GAP: f64 = 16.0;
/// The space between layers.
const LAYER_GAP: f64 = 56.0;
/// The space around the drawing.
pub const MARGIN: f64 = 16.0;
/// Reordering sweeps (each one down and one up).
const SWEEPS: usize = 12;
/// Placement sweeps.
const PLACEMENTS: usize = 8;

/// A node placed: its box's centre.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Placed {
    /// The centre's x.
    pub x: f64,
    /// The centre's y.
    pub y: f64,
    /// Its layer (0 at the top).
    pub layer: usize,
}

/// An edge's route: from the bottom of `from`'s box, straight down through each layer it
/// crosses (beside that layer's boxes, from the top of their band to its bottom), to the top
/// of `to`'s box. Between layers it bends; within a layer it never does, so it can't cut a
/// box.
#[derive(Debug, Clone, PartialEq)]
pub struct Route {
    /// The edge's index in the edges given.
    pub edge: usize,
    /// The upstream node's index.
    pub from: usize,
    /// The downstream node's index.
    pub to: usize,
    /// The points, first to last.
    pub points: Vec<(f64, f64)>,
}

/// A graph laid out.
#[derive(Debug, Clone, PartialEq)]
pub struct Layout {
    /// The nodes, in the order given.
    pub nodes: Vec<Placed>,
    /// The edges, in the order given (an edge that doesn't go down, which a DAG never has, is
    /// left out).
    pub routes: Vec<Route>,
    /// The drawing's width.
    pub width: f64,
    /// The drawing's height.
    pub height: f64,
}

/// A node of the layered graph: a task, or a point of a long edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Item {
    Node(usize),
    Dummy,
}

impl Item {
    const fn width(self) -> f64 {
        match self {
            Self::Node(_) => NODE_WIDTH,
            Self::Dummy => 0.0,
        }
    }
}

/// The layered graph: items per layer and edges between consecutive layers (by item index).
struct Layered {
    items: Vec<Item>,
    layer_of: Vec<usize>,
    /// `layers[l]`: the items of layer `l`, in order.
    layers: Vec<Vec<usize>>,
    /// Each item's neighbours in the layer above and below.
    up: Vec<Vec<usize>>,
    down: Vec<Vec<usize>>,
    /// Each kept edge: its index, and its chain of items, `from` first.
    chains: Vec<(usize, Vec<usize>)>,
}

/// Lays out `count` nodes joined by `edges` (`(from, to)` indices; `to` runs after `from`).
#[must_use]
pub fn layout(count: usize, edges: &[(usize, usize)]) -> Layout {
    let layer = layers(count, edges);
    let mut graph = layered(count, edges, &layer);
    reduce_crossings(&mut graph);
    let x = place(&graph);
    let y = |l: usize| {
        MARGIN
            + NODE_HEIGHT / 2.0
            + f64::from(u32::try_from(l).unwrap_or(u32::MAX)) * (NODE_HEIGHT + LAYER_GAP)
    };
    let nodes = (0..count)
        .map(|n| Placed {
            x: x[n],
            y: y(layer[n]),
            layer: layer[n],
        })
        .collect::<Vec<_>>();
    let routes = graph
        .chains
        .iter()
        .map(|(edge, chain)| {
            let (from, to) = (&chain[0], &chain[chain.len() - 1]);
            let mut points = vec![(x[*from], y(layer[*from]) + NODE_HEIGHT / 2.0)];
            for &i in &chain[1..chain.len() - 1] {
                let middle = y(graph.layer_of[i]);
                points.push((x[i], middle - NODE_HEIGHT / 2.0));
                points.push((x[i], middle + NODE_HEIGHT / 2.0));
            }
            points.push((x[*to], y(layer[*to]) - NODE_HEIGHT / 2.0));
            Route {
                edge: *edge,
                from: *from,
                to: *to,
                points,
            }
        })
        .collect();
    let right = graph
        .items
        .iter()
        .enumerate()
        .map(|(i, item)| x[i] + item.width() / 2.0)
        .fold(0.0_f64, f64::max);
    let depth = layer.iter().copied().max().map_or(0, |l| l + 1);
    Layout {
        nodes,
        routes,
        width: right + MARGIN,
        height: if depth == 0 {
            2.0 * MARGIN
        } else {
            y(depth - 1) + NODE_HEIGHT / 2.0 + MARGIN
        },
    }
}

/// Each node's layer: one below its lowest upstream node. Nodes on a cycle (never in a valid
/// DAG) go below everything else.
fn layers(count: usize, edges: &[(usize, usize)]) -> Vec<usize> {
    let mut upstream = vec![0_usize; count];
    let mut downstream = vec![Vec::new(); count];
    for &(from, to) in edges
        .iter()
        .filter(|(f, t)| f != t && *f < count && *t < count)
    {
        upstream[to] += 1;
        downstream[from].push(to);
    }
    let mut layer = vec![0_usize; count];
    let mut done = vec![false; count];
    let mut ready: Vec<usize> = (0..count).filter(|&n| upstream[n] == 0).collect();
    while let Some(n) = ready.pop() {
        done[n] = true;
        for &d in &downstream[n] {
            layer[d] = layer[d].max(layer[n] + 1);
            upstream[d] -= 1;
            if upstream[d] == 0 {
                ready.push(d);
            }
        }
    }
    let below = layer.iter().copied().max().unwrap_or(0) + 1;
    for n in (0..count).filter(|&n| !done[n]) {
        layer[n] = below;
    }
    layer
}

fn layered(count: usize, edges: &[(usize, usize)], layer: &[usize]) -> Layered {
    let mut items: Vec<Item> = (0..count).map(Item::Node).collect();
    let mut layer_of = layer.to_vec();
    let mut chains = Vec::new();
    for (edge, &(from, to)) in edges.iter().enumerate() {
        if from >= count || to >= count || layer[from] >= layer[to] {
            continue;
        }
        let mut chain = vec![from];
        for l in layer[from] + 1..layer[to] {
            items.push(Item::Dummy);
            layer_of.push(l);
            chain.push(items.len() - 1);
        }
        chain.push(to);
        chains.push((edge, chain));
    }
    let depth = layer_of.iter().copied().max().map_or(0, |l| l + 1);
    let mut layers = vec![Vec::new(); depth];
    for (i, &l) in layer_of.iter().enumerate() {
        layers[l].push(i);
    }
    let mut up = vec![Vec::new(); items.len()];
    let mut down = vec![Vec::new(); items.len()];
    for (_, chain) in &chains {
        for pair in chain.windows(2) {
            down[pair[0]].push(pair[1]);
            up[pair[1]].push(pair[0]);
        }
    }
    Layered {
        items,
        layer_of,
        layers,
        up,
        down,
        chains,
    }
}

/// Reorders the layers by barycenters, keeping the order with the fewest crossings seen.
fn reduce_crossings(graph: &mut Layered) {
    let mut best = graph.layers.clone();
    let mut fewest = crossings(graph);
    for _ in 0..SWEEPS {
        for l in 1..graph.layers.len() {
            reorder(graph, l, true);
        }
        for l in (0..graph.layers.len().saturating_sub(1)).rev() {
            reorder(graph, l, false);
        }
        let now = crossings(graph);
        if now < fewest {
            fewest = now;
            best.clone_from(&graph.layers);
        }
        if fewest == 0 {
            break;
        }
    }
    graph.layers = best;
}

/// Sorts layer `l` by its items' mean position among their neighbours above (`by_up`) or
/// below; an item without such neighbours keeps its place.
fn reorder(graph: &mut Layered, l: usize, by_up: bool) {
    let position = positions(graph);
    let keys: Vec<(f64, usize)> = graph.layers[l]
        .iter()
        .enumerate()
        .map(|(place, &i)| {
            let near = if by_up { &graph.up[i] } else { &graph.down[i] };
            let key = if near.is_empty() {
                to_f64(place)
            } else {
                near.iter().map(|&n| to_f64(position[n])).sum::<f64>() / to_f64(near.len())
            };
            (key, i)
        })
        .collect();
    let mut sorted = keys;
    // Stable: ties keep their order.
    sorted.sort_by(|a, b| a.0.total_cmp(&b.0));
    graph.layers[l] = sorted.into_iter().map(|(_, i)| i).collect();
}

/// Each item's place in its layer.
fn positions(graph: &Layered) -> Vec<usize> {
    let mut position = vec![0; graph.items.len()];
    for layer in &graph.layers {
        for (place, &i) in layer.iter().enumerate() {
            position[i] = place;
        }
    }
    position
}

/// The crossings between every two consecutive layers.
fn crossings(graph: &Layered) -> usize {
    let position = positions(graph);
    let mut total = 0;
    for layer in &graph.layers {
        let edges: Vec<(usize, usize)> = layer
            .iter()
            .flat_map(|&i| graph.down[i].iter().map(move |&d| (i, d)))
            .map(|(a, b)| (position[a], position[b]))
            .collect();
        for (k, a) in edges.iter().enumerate() {
            total += edges[k + 1..]
                .iter()
                .filter(|b| (a.0 < b.0 && a.1 > b.1) || (a.0 > b.0 && a.1 < b.1))
                .count();
        }
    }
    total
}

/// Each item's x (its centre).
fn place(graph: &Layered) -> Vec<f64> {
    let mut x = vec![0.0; graph.items.len()];
    for layer in &graph.layers {
        let mut at = 0.0;
        for &i in layer {
            x[i] = at + graph.items[i].width() / 2.0;
            at += graph.items[i].width() + gap(graph.items[i]);
        }
    }
    for _ in 0..PLACEMENTS {
        for l in 1..graph.layers.len() {
            fit(graph, l, &graph.up, &mut x);
        }
        for l in (0..graph.layers.len().saturating_sub(1)).rev() {
            fit(graph, l, &graph.down, &mut x);
        }
    }
    let left = graph
        .items
        .iter()
        .enumerate()
        .map(|(i, item)| x[i] - item.width() / 2.0)
        .fold(f64::INFINITY, f64::min);
    if left.is_finite() {
        for value in &mut x {
            *value += MARGIN - left;
        }
    }
    x
}

const fn gap(item: Item) -> f64 {
    match item {
        Item::Node(_) => GAP,
        Item::Dummy => DUMMY_GAP,
    }
}

/// Moves layer `l`'s items as near their neighbours' (`near`) mean x as their order and the
/// gaps allow: the least-squares fit of `x[k] = offset[k] + y[k]` with `y` non-decreasing,
/// by pool-adjacent-violators. An item without neighbours wants to stay where it is.
fn fit(graph: &Layered, l: usize, near: &[Vec<usize>], x: &mut [f64]) {
    let layer = &graph.layers[l];
    let mut offset = Vec::with_capacity(layer.len());
    let mut at = 0.0;
    for (k, &i) in layer.iter().enumerate() {
        if k > 0 {
            let before = graph.items[layer[k - 1]];
            at += before.width() / 2.0 + gap(before) + graph.items[i].width() / 2.0;
        }
        offset.push(at);
    }
    // Blocks of (sum of wanted y, count), merged while out of order.
    let mut blocks: Vec<(f64, f64, usize)> = Vec::new();
    for (k, &i) in layer.iter().enumerate() {
        let wanted = if near[i].is_empty() {
            x[i]
        } else {
            near[i].iter().map(|&n| x[n]).sum::<f64>() / to_f64(near[i].len())
        };
        blocks.push((wanted - offset[k], 1.0, 1));
        while blocks.len() > 1 {
            let (sum, n, _) = blocks[blocks.len() - 1];
            let (before_sum, before_n, _) = blocks[blocks.len() - 2];
            if before_sum / before_n <= sum / n {
                break;
            }
            let last = blocks.pop().expect("two blocks");
            let previous = blocks.last_mut().expect("two blocks");
            previous.0 += last.0;
            previous.1 += last.1;
            previous.2 += last.2;
        }
    }
    let mut k = 0;
    for (sum, n, items) in blocks {
        for _ in 0..items {
            x[layer[k]] = offset[k] + sum / n;
            k += 1;
        }
    }
}

fn to_f64(n: usize) -> f64 {
    f64::from(u32::try_from(n).unwrap_or(u32::MAX))
}
