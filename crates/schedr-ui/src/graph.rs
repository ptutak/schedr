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
//! **Groups** (decision 65): a group's tasks stay together. Every layer is ordered group by
//! group (a group's place is its members' mean; nested groups inside their parent), a long
//! edge's points belong to the innermost group both its ends are in, and after the x fit any
//! other task is pushed clear of a group's box in every layer the box spans. A box is its
//! members' extent and its nested boxes', with padding and a band for its label on top; the
//! space between layers grows with the deepest nesting, so labels never meet.
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
/// A group box's padding around its contents (sides and bottom).
pub const GROUP_PAD: f64 = 10.0;
/// A group box's band for its label, above its contents.
pub const GROUP_LABEL: f64 = 22.0;
/// Rounds of pushing other tasks clear of the group boxes, at most.
const SEPARATIONS: usize = 24;

/// A group of nodes, for [`layout_grouped`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupSpec {
    /// The group it is nested in (an index into the same list, before this one).
    pub parent: Option<usize>,
}

/// A group's box, drawn around its nodes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GroupBox {
    /// The group's index in the groups given.
    pub group: usize,
    /// The box's left edge.
    pub left: f64,
    /// The box's top edge.
    pub top: f64,
    /// The box's width.
    pub width: f64,
    /// The box's height.
    pub height: f64,
    /// How deeply it is nested (0 for a top-level group).
    pub depth: usize,
}

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
    /// The groups' boxes, outer before inner (a group without nodes has none).
    pub boxes: Vec<GroupBox>,
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
    /// Each item's groups, outermost first (empty outside any group).
    paths: Vec<Vec<usize>>,
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
    layout_grouped(count, edges, &[], &vec![None; count])
}

/// [`layout`], with `groups` and each node's innermost group (`node_group`, by index into
/// `groups`) kept together and boxed.
#[must_use]
pub fn layout_grouped(
    count: usize,
    edges: &[(usize, usize)],
    groups: &[GroupSpec],
    node_group: &[Option<usize>],
) -> Layout {
    let paths: Vec<Vec<usize>> = (0..count)
        .map(|n| group_path(groups, node_group.get(n).copied().flatten()))
        .collect();
    let deepest = paths.iter().map(Vec::len).max().unwrap_or(0);
    // Room between layers for the deepest boxes' padding and labels.
    let layer_gap = LAYER_GAP + to_f64(deepest.saturating_sub(1)) * (GROUP_PAD + GROUP_LABEL);
    let top = MARGIN + to_f64(deepest) * GROUP_LABEL;
    let layer = layers(count, edges);
    let mut graph = layered(count, edges, &layer, paths);
    for l in 0..graph.layers.len() {
        let keys: Vec<f64> = (0..graph.layers[l].len()).map(to_f64).collect();
        arrange(&mut graph, l, &keys);
    }
    reduce_crossings(&mut graph);
    let mut x = place(&graph);
    separate(&graph, groups, &mut x);
    let y = |l: usize| top + NODE_HEIGHT / 2.0 + to_f64(l) * (NODE_HEIGHT + layer_gap);
    let mut boxes = group_boxes(&graph, groups, &x, &y);
    // Everything shifted so the leftmost box or node starts at the margin.
    let left = boxes
        .iter()
        .map(|b| b.left)
        .chain(
            graph
                .items
                .iter()
                .enumerate()
                .map(|(i, item)| x[i] - item.width() / 2.0),
        )
        .fold(f64::INFINITY, f64::min);
    if left.is_finite() {
        let shift = MARGIN - left;
        for value in &mut x {
            *value += shift;
        }
        for group_box in &mut boxes {
            group_box.left += shift;
        }
    }
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
        .chain(boxes.iter().map(|b| b.left + b.width))
        .fold(0.0_f64, f64::max);
    let bottom = boxes
        .iter()
        .map(|b| b.top + b.height)
        .fold(0.0_f64, f64::max);
    let depth = layer.iter().copied().max().map_or(0, |l| l + 1);
    Layout {
        nodes,
        routes,
        boxes,
        width: right + MARGIN,
        height: if depth == 0 {
            2.0 * MARGIN
        } else {
            (y(depth - 1) + NODE_HEIGHT / 2.0).max(bottom) + MARGIN
        },
    }
}

/// A group and its ancestors, outermost first.
fn group_path(groups: &[GroupSpec], innermost: Option<usize>) -> Vec<usize> {
    let mut path = Vec::new();
    let mut at = innermost.filter(|&g| g < groups.len());
    while let Some(g) = at {
        if path.contains(&g) || path.len() > groups.len() {
            break; // a cycle, which validation refuses: stop rather than loop
        }
        path.push(g);
        at = groups[g].parent.filter(|&p| p < groups.len());
    }
    path.reverse();
    path
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

fn layered(
    count: usize,
    edges: &[(usize, usize)],
    layer: &[usize],
    mut paths: Vec<Vec<usize>>,
) -> Layered {
    let mut items: Vec<Item> = (0..count).map(Item::Node).collect();
    let mut layer_of = layer.to_vec();
    let mut chains = Vec::new();
    for (edge, &(from, to)) in edges.iter().enumerate() {
        if from >= count || to >= count || layer[from] >= layer[to] {
            continue;
        }
        // A long edge's points are in the groups both its ends are in.
        let shared: Vec<usize> = paths[from]
            .iter()
            .zip(&paths[to])
            .take_while(|(a, b)| a == b)
            .map(|(a, _)| *a)
            .collect();
        let mut chain = vec![from];
        for l in layer[from] + 1..layer[to] {
            items.push(Item::Dummy);
            layer_of.push(l);
            paths.push(shared.clone());
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
        paths,
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
    let mut wanted = vec![0.0; graph.items.len()];
    for (key, i) in &keys {
        wanted[*i] = *key;
    }
    let keys: Vec<f64> = graph.layers[l].iter().map(|&i| wanted[i]).collect();
    arrange(graph, l, &keys);
}

/// Orders layer `l` by `keys` (one per item, in the layer's current order), group by group:
/// a group's members stay together, placed by their mean key; nested groups likewise inside.
/// Stable: ties keep their order.
fn arrange(graph: &mut Layered, l: usize, keys: &[f64]) {
    let members: Vec<(usize, f64)> = graph.layers[l]
        .iter()
        .copied()
        .zip(keys.iter().copied())
        .collect();
    graph.layers[l] = arrange_level(&graph.paths, &members, 0);
}

/// One thing [`arrange_level`] places: its key, its first place in the layer, the group it
/// stands for (`None` for a single item) and its items with their keys.
type Unit = (f64, usize, Option<usize>, Vec<(usize, f64)>);

fn arrange_level(paths: &[Vec<usize>], members: &[(usize, f64)], depth: usize) -> Vec<usize> {
    // Units: an item not deeper in a group, or a whole group at this depth.
    let mut units: Vec<Unit> = Vec::new();
    for (place, &(item, key)) in members.iter().enumerate() {
        match paths[item].get(depth) {
            None => units.push((key, place, None, vec![(item, key)])),
            Some(&group) => match units.iter_mut().find(|u| u.2 == Some(group)) {
                Some(unit) => unit.3.push((item, key)),
                None => units.push((key, place, Some(group), vec![(item, key)])),
            },
        }
    }
    for unit in &mut units {
        if unit.2.is_some() {
            unit.0 = unit.3.iter().map(|(_, k)| k).sum::<f64>() / to_f64(unit.3.len());
        }
    }
    // Sibling groups keep one order in every layer (by index), so their boxes never swap
    // sides: each group's key is raised to at least the one before it; tasks outside groups
    // then fall between them by their own keys.
    let mut grouped: Vec<usize> = (0..units.len()).filter(|&u| units[u].2.is_some()).collect();
    grouped.sort_by_key(|&u| units[u].2);
    let mut floor = f64::NEG_INFINITY;
    for &u in &grouped {
        floor = floor.max(units[u].0);
        units[u].0 = floor;
    }
    units.sort_by(|a, b| {
        a.0.total_cmp(&b.0).then_with(|| match (a.2, b.2) {
            (Some(x), Some(y)) => x.cmp(&y),
            _ => a.1.cmp(&b.1),
        })
    });
    units
        .into_iter()
        .flat_map(|(_, _, group, inside)| match group {
            None => inside.into_iter().map(|(i, _)| i).collect::<Vec<_>>(),
            Some(_) => arrange_level(paths, &inside, depth + 1),
        })
        .collect()
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
    x
}

/// Pushes every item clear of the boxes of the groups it isn't in, in each layer those
/// boxes span, keeping each layer's order and gaps. Everything only moves right, so the
/// rounds settle: an item before a group's members that reaches into its box moves the whole
/// group right (in every layer, with what follows it); an item after them, or in a layer
/// without them, that reaches into the box moves right of it (with what follows it).
fn separate(graph: &Layered, groups: &[GroupSpec], x: &mut [f64]) {
    let paths: Vec<Vec<usize>> = (0..groups.len())
        .map(|g| group_path(groups, Some(g)))
        .collect();
    for _ in 0..SEPARATIONS * groups.len() {
        let mut moved = false;
        for (g, path) in paths.iter().enumerate() {
            moved |= clear(graph, g, path, x);
        }
        if !moved {
            break;
        }
    }
}

/// Whether an item with groups `item` belongs left of a group with path `group`: when it is in
/// a sibling group ordered before the group's branch, as the layers are ordered.
fn belongs_left(item: &[usize], group: &[usize]) -> bool {
    item.iter()
        .zip(group)
        .find(|(a, b)| a != b)
        .is_some_and(|(a, b)| a < b)
}

/// One pass of [`separate`] for group `g` (its groups `path`); whether anything moved.
fn clear(graph: &Layered, g: usize, path: &[usize], x: &mut [f64]) -> bool {
    let inside = |i: usize| graph.paths[i].contains(&g);
    let mut moved = false;
    for layer in &graph.layers {
        // The span as it is now: each move below changes it.
        let Some((left, right, first, last)) = group_spans_of(graph, g, x) else {
            return moved;
        };
        let Some(&head) = layer.first() else {
            continue;
        };
        if !(first..=last).contains(&graph.layer_of[head]) {
            continue;
        }
        let block = layer.iter().position(|&i| inside(i));
        if let Some(start) = block
            && start > 0
        {
            let before = layer[start - 1];
            let reach = x[before] + graph.items[before].width() / 2.0 + gap(graph.items[before]);
            if reach > left + 1e-9 {
                shift_group(graph, g, reach - left, x);
                moved = true;
                continue;
            }
        }
        // The left side: before the group's members, or (in a layer without them) up to the
        // last item of a group ordered before this one, as the layers are ordered.
        let split = block.unwrap_or_else(|| {
            layer
                .iter()
                .rposition(|&i| belongs_left(&graph.paths[i], path))
                .map_or(0, |k| k + 1)
        });
        if block.is_none() && split > 0 {
            let reach = layer[..split]
                .iter()
                .map(|&i| x[i] + graph.items[i].width() / 2.0 + gap(graph.items[i]))
                .fold(f64::NEG_INFINITY, f64::max);
            if reach > left + 1e-9 {
                // The group goes right of the items that belong left of it.
                shift_group(graph, g, reach - left, x);
                moved = true;
                continue;
            }
        }
        let outside = layer.iter().enumerate().skip(split).find(|&(_, &i)| {
            !inside(i)
                && x[i] + graph.items[i].width() / 2.0 > left + 1e-9
                && x[i] - graph.items[i].width() / 2.0 < right - 1e-9
        });
        if let Some((k, &i)) = outside {
            let wanted = right + gap(graph.items[i]).min(GAP);
            let edge = x[i] - graph.items[i].width() / 2.0;
            shift_from(layer, k, wanted - edge, x);
            moved = true;
        }
    }
    moved
}

/// Moves the items of `layer` from position `k` on right by `by`.
fn shift_from(layer: &[usize], k: usize, by: f64, x: &mut [f64]) {
    for &i in &layer[k..] {
        x[i] += by;
    }
}

/// Moves group `g` right by `by`: in each layer, its first member and everything after it.
fn shift_group(graph: &Layered, g: usize, by: f64, x: &mut [f64]) {
    for layer in &graph.layers {
        if let Some(k) = layer.iter().position(|&i| graph.paths[i].contains(&g)) {
            shift_from(layer, k, by, x);
        }
    }
}

/// [`group_spans`] for one group.
fn group_spans_of(graph: &Layered, group: usize, x: &[f64]) -> Option<(f64, f64, usize, usize)> {
    let mut span: Option<(f64, f64, usize, usize)> = None;
    for (item, path) in graph.paths.iter().enumerate() {
        let Some(depth) = path.iter().position(|&h| h == group) else {
            continue;
        };
        let half = graph.items[item].width() / 2.0;
        let pad = to_f64(path.len() - depth) * GROUP_PAD;
        let left = x[item] - half - pad;
        let right = x[item] + half + pad;
        let layer = graph.layer_of[item];
        span = Some(match span {
            None => (left, right, layer, layer),
            Some((lo, hi, first, last)) => (
                lo.min(left),
                hi.max(right),
                first.min(layer),
                last.max(layer),
            ),
        });
    }
    span
}

/// Each group's box across x (with its padding and its nested groups' padding) and the
/// layers it spans, or `None` for a group without items.
fn group_spans(
    graph: &Layered,
    group_count: usize,
    x: &[f64],
) -> Vec<Option<(f64, f64, usize, usize)>> {
    (0..group_count)
        .map(|g| group_spans_of(graph, g, x))
        .collect()
}

/// The groups' boxes, outer before inner.
fn group_boxes(
    graph: &Layered,
    groups: &[GroupSpec],
    x: &[f64],
    y: &impl Fn(usize) -> f64,
) -> Vec<GroupBox> {
    let spans = group_spans(graph, groups.len(), x);
    let mut boxes: Vec<GroupBox> = Vec::new();
    for (g, span) in spans.iter().enumerate() {
        let Some((left, right, first, last)) = *span else {
            continue;
        };
        let depth = group_path(groups, Some(g)).len() - 1;
        // The deepest nesting under this group, per layer edge, sets its vertical padding.
        let below = graph
            .paths
            .iter()
            .filter_map(|p| p.iter().position(|&h| h == g).map(|at| p.len() - at))
            .max()
            .unwrap_or(1);
        let top = y(first) - NODE_HEIGHT / 2.0 - to_f64(below) * GROUP_LABEL;
        let bottom = y(last) + NODE_HEIGHT / 2.0 + to_f64(below) * GROUP_PAD;
        boxes.push(GroupBox {
            group: g,
            left,
            top,
            width: right - left,
            height: bottom - top,
            depth,
        });
    }
    boxes.sort_by_key(|b| (b.depth, b.group));
    boxes
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
