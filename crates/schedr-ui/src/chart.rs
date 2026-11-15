//! Charts drawn by the server as SVG (decision 466): bars (a task's attempts, decision 467), lines over time, an "others" band, dashed
//! limits, axes, and a hover strip per point whose `<title>` gives every value there. No
//! JavaScript; the colours are the stylesheet's series tokens (`--series-1` to `--series-8`),
//! light and dark.

use std::fmt::Write as _;

use jiff::Timestamp;
use jiff::tz::TimeZone;

/// A chart's drawing width (the SVG scales to its box).
pub const WIDTH: f64 = 720.0;
/// A chart's in a panel: its text stays the same size.
pub const NARROW: f64 = 420.0;
/// Its drawing height.
pub const HEIGHT: f64 = 220.0;
/// Room left of the plot (the y labels).
const LEFT: f64 = 64.0;
/// Room right of it.
const RIGHT: f64 = 12.0;
/// Room above it.
const TOP: f64 = 10.0;
/// Room below it (the x labels).
const BOTTOM: f64 = 24.0;
/// The series a chart draws at most; the rest go into the band (decision 465).
pub const SERIES: usize = 8;

/// What the values are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    /// Bytes, shown as KiB, MiB, GiB.
    Bytes,
    /// CPU cores.
    Cores,
}

impl Unit {
    /// `value` as people read it.
    #[must_use]
    pub fn show(self, value: f64) -> String {
        match self {
            Self::Bytes => bytes(value),
            Self::Cores => format!("{value:.2} cores"),
        }
    }
}

/// `n` bytes as people read it: `512 B`, `1.5 KiB`, `20.0 MiB`.
#[must_use]
pub fn bytes(n: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = n.max(0.0);
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value:.0} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// One line: a label, its colour slot (1 to 8), and its points (seconds, value), oldest first.
#[derive(Debug, Clone, PartialEq)]
pub struct Line {
    /// What it is.
    pub label: String,
    /// Its colour, `--series-<slot>`.
    pub slot: usize,
    /// Its points.
    pub points: Vec<(i64, f64)>,
}

/// A band: its label, and each point's time, lowest and highest.
pub type Band = (String, Vec<(i64, f64, f64)>);

/// A chart's content.
#[derive(Debug, Clone, PartialEq)]
pub struct Spec {
    /// What it shows, for screen readers.
    pub title: String,
    /// What the values are.
    pub unit: Unit,
    /// Its drawing width: [`WIDTH`], or [`NARROW`] in a panel.
    pub width: f64,
    /// Its start and end, in seconds.
    pub from: i64,
    /// Its end.
    pub to: i64,
    /// A point's width, in seconds: points further apart leave a gap.
    pub step: i64,
    /// The lines, at most [`SERIES`].
    pub lines: Vec<Line>,
    /// Layers stacked on each other, the first at the bottom (memory by kind); a layer
    /// without a point at a time counts 0 there.
    pub stacked: Vec<Line>,
    /// The rest as one band, if any.
    pub band: Option<Band>,
    /// Dashed limits: a label, the slot of the line it belongs to, the value.
    pub limits: Vec<(String, usize, f64)>,
    /// The axis's zone.
    pub zone: TimeZone,
}

/// `text` safe in SVG text and attributes.
fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

/// A round step for about `ticks` ticks up to `max`: 1, 2 or 5 times a power of `base`'s
/// unit. Returns the step.
fn nice_step(max: f64, ticks: f64) -> f64 {
    let raw = (max / ticks).max(f64::MIN_POSITIVE);
    let power = 10_f64.powf(raw.log10().floor());
    let fraction = raw / power;
    let nice = if fraction <= 1.0 {
        1.0
    } else if fraction <= 2.0 {
        2.0
    } else if fraction <= 5.0 {
        5.0
    } else {
        10.0
    };
    nice * power
}

/// The y axis's top and ticks for values up to `max` in `unit`.
fn y_axis(max: f64, unit: Unit) -> (f64, Vec<f64>) {
    let (scale, max) = match unit {
        // Ticks in the unit the top is shown in: 512 MiB, 1 GiB, 1.5 GiB.
        Unit::Bytes => {
            let mut scale = 1.0;
            while max / scale >= 1024.0 {
                scale *= 1024.0;
            }
            (scale, max / scale)
        }
        Unit::Cores => (1.0, max),
    };
    let max = if max > 0.0 { max } else { 1.0 };
    let step = nice_step(max, 4.0);
    let top = (max / step).ceil() * step;
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "a handful of ticks"
    )]
    let count = (top / step).round() as usize;
    let ticks = (0..=count)
        .map(|i| {
            #[expect(clippy::cast_precision_loss, reason = "a handful of ticks")]
            let i = i as f64;
            i * step * scale
        })
        .collect();
    (top * scale, ticks)
}

/// The x axis's ticks for `from..to` seconds: a round interval for the span.
fn x_ticks(from: i64, to: i64) -> (i64, &'static str) {
    let span = to - from;
    if span <= 2 * 3600 {
        (600, "%H:%M")
    } else if span <= 2 * 86_400 {
        (3 * 3600, "%H:%M")
    } else if span <= 10 * 86_400 {
        (86_400, "%b %d")
    } else {
        (5 * 86_400, "%b %d")
    }
}

/// A chart drawn: its SVG.
#[must_use]
pub fn draw(spec: &Spec) -> String {
    let plot_w = spec.width - LEFT - RIGHT;
    let plot_h = HEIGHT - TOP - BOTTOM;
    let span = (spec.to - spec.from).max(1);
    #[expect(clippy::cast_precision_loss, reason = "screen coordinates")]
    let x = |t: i64| LEFT + plot_w * ((t - spec.from) as f64 / span as f64);
    let data_max = spec
        .lines
        .iter()
        .flat_map(|l| l.points.iter().map(|p| p.1))
        .chain(
            spec.band
                .iter()
                .flat_map(|(_, points)| points.iter().map(|p| p.2)),
        )
        .chain(
            stack(&spec.stacked)
                .into_iter()
                .filter_map(|(_, tops)| tops.last().copied()),
        )
        .fold(0.0_f64, f64::max);
    // A limit far above the lines would flatten them: shown only within 4 times their top.
    let limits: Vec<_> = spec
        .limits
        .iter()
        .filter(|(_, _, value)| data_max == 0.0 || *value <= data_max * 4.0)
        .collect();
    let max = limits.iter().map(|l| l.2).fold(data_max, f64::max);
    let (top, y_ticks) = y_axis(max, spec.unit);
    let y = |v: f64| TOP + plot_h * (1.0 - v / top);

    let mut svg = String::new();
    let _ = write!(
        svg,
        r#"<svg class="chart" viewBox="0 0 {} {HEIGHT}" role="img" aria-label="{}">"#,
        spec.width,
        escape(&spec.title)
    );
    axes(&mut svg, spec, &y_ticks, &x, &y);
    if let Some((label, points)) = &spec.band {
        for run in runs(points, spec.step, |p| p.0) {
            let d = area(
                run.iter().map(|p| (x(p.0), y(p.2))),
                run.iter().rev().map(|p| (x(p.0), y(p.1))),
            );
            let _ = write!(
                svg,
                r#"<path class="band" d="{d}Z"><title>{}</title></path>"#,
                escape(label)
            );
        }
    }
    let layers = stack(&spec.stacked);
    for (layer, line) in spec.stacked.iter().enumerate() {
        for run in runs(&layers, spec.step, |p| p.0) {
            let below = |p: &(i64, Vec<f64>)| if layer == 0 { 0.0 } else { p.1[layer - 1] };
            let d = area(
                run.iter().map(|p| (x(p.0), y(p.1[layer]))),
                run.iter().rev().map(|p| (x(p.0), y(below(p)))),
            );
            let _ = write!(
                svg,
                r#"<path class="area s{}" d="{d}Z"><title>{}</title></path>"#,
                line.slot,
                escape(&line.label)
            );
        }
    }
    for (label, slot, value) in &limits {
        let _ = write!(
            svg,
            r#"<line class="limit s{slot}" x1="{LEFT}" x2="{}" y1="{y:.1}" y2="{y:.1}"><title>{}: {}</title></line>"#,
            spec.width - RIGHT,
            escape(label),
            escape(&spec.unit.show(*value)),
            y = y(*value),
        );
    }
    for line in &spec.lines {
        for run in runs(&line.points, spec.step, |p| p.0) {
            let d = path(run.iter().map(|p| (x(p.0), y(p.1))));
            // A point alone: a dot, so it shows.
            if run.len() == 1 {
                let _ = write!(
                    svg,
                    r#"<circle class="dot s{}" cx="{:.1}" cy="{:.1}" r="3"></circle>"#,
                    line.slot,
                    x(run[0].0),
                    y(run[0].1)
                );
            } else {
                let _ = write!(svg, r#"<path class="line s{}" d="{d}"></path>"#, line.slot);
            }
        }
    }
    hover_strips(&mut svg, spec, &x, plot_h);
    svg.push_str("</svg>");
    svg
}

/// One bar: its value (none: not read, drawn as a gap with its label), its class (how it
/// ended: `ok`, `failed`, `other`), whether it is marked (killed for its memory), where it
/// links, and its hover text.
#[derive(Debug, Clone, PartialEq)]
pub struct Bar {
    /// Its value; none when there is none (a gap).
    pub value: Option<f64>,
    /// Its colour class.
    pub class: &'static str,
    /// Marked with a cross above it.
    pub marked: bool,
    /// Where it links.
    pub href: String,
    /// Its hover text.
    pub title: String,
}

/// A bar chart's content: bars side by side, oldest first, a dashed limit, a label under
/// the first and the last.
#[derive(Debug, Clone, PartialEq)]
pub struct Bars {
    /// What it shows, for screen readers.
    pub title: String,
    /// What the values are.
    pub unit: Unit,
    /// Its drawing width.
    pub width: f64,
    /// The bars.
    pub bars: Vec<Bar>,
    /// A dashed limit: its label and value.
    pub limit: Option<(String, f64)>,
    /// The labels under the first bar and the last.
    pub ends: (String, String),
}

/// A bar chart drawn: its SVG.
#[must_use]
pub fn draw_bars(spec: &Bars) -> String {
    let plot_w = spec.width - LEFT - RIGHT;
    let plot_h = HEIGHT - TOP - BOTTOM;
    let data_max = spec
        .bars
        .iter()
        .filter_map(|b| b.value)
        .fold(0.0_f64, f64::max);
    let limit = spec
        .limit
        .as_ref()
        .filter(|(_, value)| data_max == 0.0 || *value <= data_max * 4.0);
    let max = limit.map_or(data_max, |l| l.1.max(data_max));
    let (top, y_ticks) = y_axis(max, spec.unit);
    let y = |v: f64| TOP + plot_h * (1.0 - v / top);
    #[expect(clippy::cast_precision_loss, reason = "a few dozen bars")]
    let slot = plot_w / spec.bars.len().max(1) as f64;
    // Thin bars, a gap between: at most 14 wide.
    let bar_w = (slot * 0.6).clamp(2.0, 14.0);
    #[expect(clippy::cast_precision_loss, reason = "a few dozen bars")]
    let center = |i: usize| LEFT + slot * (i as f64 + 0.5);

    let mut svg = String::new();
    let _ = write!(
        svg,
        r#"<svg class="chart bars" viewBox="0 0 {} {HEIGHT}" role="img" aria-label="{}">"#,
        spec.width,
        escape(&spec.title)
    );
    y_grid(&mut svg, spec.width, &y_ticks, spec.unit, &y);
    if let Some(last) = spec.bars.len().checked_sub(1) {
        let first = center(0) - bar_w / 2.0;
        end_labels(
            &mut svg,
            &spec.ends,
            first,
            (last > 0).then(|| center(last) + bar_w / 2.0),
        );
    }
    let base = y(0.0);
    for (i, bar) in spec.bars.iter().enumerate() {
        let left = center(i) - bar_w / 2.0;
        let _ = write!(
            svg,
            r#"<a href="{}"><title>{}</title>"#,
            escape(&bar.href),
            escape(&bar.title)
        );
        // The whole slot's height takes the hover and the click, wider than the bar.
        let _ = write!(
            svg,
            r#"<rect class="hover" x="{:.1}" y="{TOP}" width="{slot:.1}" height="{plot_h:.1}"></rect>"#,
            center(i) - slot / 2.0
        );
        if let Some(value) = bar.value {
            let top_y = y(value);
            let _ = write!(
                svg,
                r#"<path class="bar {}" d="{}"></path>"#,
                bar.class,
                bar_outline(left, bar_w, base, top_y)
            );
            if bar.marked {
                let _ = write!(
                    svg,
                    r#"<text class="mark" x="{:.1}" y="{:.1}" text-anchor="middle">✕</text>"#,
                    center(i),
                    (top_y - 4.0).max(TOP + 8.0)
                );
            }
        }
        svg.push_str("</a>");
    }
    if let Some((label, value)) = limit {
        let _ = write!(
            svg,
            r#"<line class="limit" x1="{LEFT}" x2="{}" y1="{y:.1}" y2="{y:.1}"><title>{}: {}</title></line>"#,
            spec.width - RIGHT,
            escape(label),
            escape(&spec.unit.show(*value)),
            y = y(*value),
        );
    }
    let _ = write!(
        svg,
        r#"<line class="baseline" x1="{LEFT}" x2="{}" y1="{base:.1}" y2="{base:.1}"></line>"#,
        spec.width - RIGHT,
    );
    svg.push_str("</svg>");
    svg
}

/// The labels under a bar chart's first bar (from `first`) and its last (to `last`; none for
/// one bar).
fn end_labels(svg: &mut String, (start, end): &(String, String), first: f64, last: Option<f64>) {
    let mut label = |x: f64, anchor: &str, text: &str| {
        let _ = write!(
            svg,
            r#"<text class="axis" x="{x:.1}" y="{}" text-anchor="{anchor}">{}</text>"#,
            HEIGHT - 6.0,
            escape(text)
        );
    };
    label(first, "start", start);
    if let Some(last) = last {
        label(last, "end", end);
    }
}

/// The y grid lines and their labels, across a chart `width` wide.
fn y_grid(svg: &mut String, width: f64, y_ticks: &[f64], unit: Unit, y: &impl Fn(f64) -> f64) {
    for tick in y_ticks {
        let _ = write!(
            svg,
            r#"<line class="grid" x1="{LEFT}" x2="{}" y1="{y:.1}" y2="{y:.1}"></line><text class="axis" x="{}" y="{:.1}" text-anchor="end">{}</text>"#,
            width - RIGHT,
            LEFT - 6.0,
            y(*tick) + 4.0,
            escape(&axis_value(*tick, unit)),
            y = y(*tick),
        );
    }
}

/// A bar's outline from `left`, `width` wide, from the baseline `base` up to `top`: rounded at
/// the top only, flat on the baseline.
fn bar_outline(left: f64, width: f64, base: f64, top: f64) -> String {
    let r = (width / 2.0).min(4.0).min((base - top).max(0.0));
    let right = left + width;
    format!(
        "M{left:.1},{base:.1}V{:.1}Q{left:.1},{top:.1} {:.1},{top:.1}H{:.1}Q{right:.1},{top:.1} {right:.1},{:.1}V{base:.1}Z",
        top + r,
        left + r,
        right - r,
        top + r,
    )
}

/// The grid, the y labels, the time labels and the baseline.
fn axes(
    svg: &mut String,
    spec: &Spec,
    y_ticks: &[f64],
    x: &impl Fn(i64) -> f64,
    y: &impl Fn(f64) -> f64,
) {
    y_grid(svg, spec.width, y_ticks, spec.unit, y);
    let (every, format) = x_ticks(spec.from, spec.to);
    let mut tick = (spec.from + every - 1).div_euclid(every) * every;
    while tick <= spec.to {
        if let Ok(at) = Timestamp::from_second(tick) {
            let label = at.to_zoned(spec.zone.clone()).strftime(format).to_string();
            let _ = write!(
                svg,
                r#"<text class="axis" x="{:.1}" y="{}" text-anchor="middle">{}</text>"#,
                x(tick),
                HEIGHT - 6.0,
                escape(&label)
            );
        }
        tick += every;
    }
    let _ = write!(
        svg,
        r#"<line class="baseline" x1="{LEFT}" x2="{}" y1="{:.1}" y2="{:.1}"></line>"#,
        spec.width - RIGHT,
        y(0.0),
        y(0.0)
    );
}

/// A line through `points` (SVG path data).
fn path(points: impl Iterator<Item = (f64, f64)>) -> String {
    let mut d = String::new();
    for (i, (px, py)) in points.enumerate() {
        let _ = write!(d, "{}{px:.1},{py:.1}", if i == 0 { "M" } else { "L" });
    }
    d
}

/// An area along `top`, then back along `bottom` (given back to front).
fn area(top: impl Iterator<Item = (f64, f64)>, bottom: impl Iterator<Item = (f64, f64)>) -> String {
    let mut d = path(top);
    for (px, py) in bottom {
        let _ = write!(d, "L{px:.1},{py:.1}");
    }
    d
}

/// A tick's label: in the unit the chart's top is shown in.
fn axis_value(value: f64, unit: Unit) -> String {
    match unit {
        Unit::Bytes if value == 0.0 => "0".to_owned(),
        Unit::Bytes => {
            let text = bytes(value);
            text.replace(".0 ", " ")
        }
        Unit::Cores => {
            let text = format!("{value:.2}");
            text.trim_end_matches('0').trim_end_matches('.').to_owned()
        }
    }
}

/// `points` split where two are further apart than one and a half steps (no samples there).
fn runs<P>(points: &[P], step: i64, at: impl Fn(&P) -> i64) -> Vec<&[P]> {
    let gap = step + step / 2;
    points.chunk_by(|a, b| at(b) - at(a) <= gap).collect()
}

/// The stacked layers' tops at each time any has a point: the running sums, bottom first.
fn stack(layers: &[Line]) -> Vec<(i64, Vec<f64>)> {
    let mut times: Vec<i64> = layers
        .iter()
        .flat_map(|l| l.points.iter().map(|p| p.0))
        .collect();
    times.sort_unstable();
    times.dedup();
    times
        .into_iter()
        .map(|at| {
            let mut sum = 0.0;
            let tops = layers
                .iter()
                .map(|l| {
                    sum += l.points.iter().find(|p| p.0 == at).map_or(0.0, |p| p.1);
                    sum
                })
                .collect();
            (at, tops)
        })
        .collect()
}

/// A transparent strip per point in time, its `<title>` every value there.
fn hover_strips(svg: &mut String, spec: &Spec, x: &impl Fn(i64) -> f64, plot_h: f64) {
    let mut times: Vec<i64> = spec
        .lines
        .iter()
        .flat_map(|l| l.points.iter().map(|p| p.0))
        .chain(
            spec.band
                .iter()
                .flat_map(|(_, points)| points.iter().map(|p| p.0)),
        )
        .collect();
    times.extend(
        spec.stacked
            .iter()
            .flat_map(|l| l.points.iter().map(|p| p.0)),
    );
    times.sort_unstable();
    times.dedup();
    let width = (x(spec.from + spec.step) - x(spec.from)).max(1.0);
    for at in times {
        let Ok(when) = Timestamp::from_second(at) else {
            continue;
        };
        let mut title = when
            .to_zoned(spec.zone.clone())
            .strftime("%Y-%m-%d %H:%M %Z")
            .to_string();
        for line in &spec.lines {
            if let Some(p) = line.points.iter().find(|p| p.0 == at) {
                let _ = write!(title, "\n{}: {}", line.label, spec.unit.show(p.1));
            }
        }
        let mut total = 0.0;
        for line in spec.stacked.iter().rev() {
            if let Some(p) = line.points.iter().find(|p| p.0 == at) {
                total += p.1;
                let _ = write!(title, "\n{}: {}", line.label, spec.unit.show(p.1));
            }
        }
        if !spec.stacked.is_empty() {
            let _ = write!(title, "\ntogether: {}", spec.unit.show(total));
        }
        if let Some((label, points)) = &spec.band
            && let Some(p) = points.iter().find(|p| p.0 == at)
        {
            let _ = write!(
                title,
                "\n{label}: {} to {}",
                spec.unit.show(p.1),
                spec.unit.show(p.2)
            );
        }
        let _ = write!(
            svg,
            r#"<rect class="hover" x="{:.1}" y="{TOP}" width="{width:.1}" height="{plot_h:.1}"><title>{}</title></rect>"#,
            x(at),
            escape(&title)
        );
    }
}

/// A sparkline of `points` (seconds, value) over `from..to`: a small line, no axes.
#[must_use]
pub fn sparkline(points: &[(i64, f64)], from: i64, to: i64, step: i64, title: &str) -> String {
    const W: f64 = 120.0;
    const H: f64 = 24.0;
    let span = (to - from).max(1);
    let max = points.iter().map(|p| p.1).fold(0.0_f64, f64::max);
    let max = if max > 0.0 { max } else { 1.0 };
    #[expect(clippy::cast_precision_loss, reason = "screen coordinates")]
    let x = |t: i64| W * ((t - from) as f64 / span as f64);
    let y = |v: f64| 2.0 + (H - 4.0) * (1.0 - v / max);
    let mut svg = format!(
        r#"<svg class="sparkline" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" aria-label="{}"><title>{}</title>"#,
        escape(title),
        escape(title)
    );
    for run in runs(points, step, |p| p.0) {
        let mut d = String::new();
        for (i, p) in run.iter().enumerate() {
            let _ = write!(
                d,
                "{}{:.1},{:.1}",
                if i == 0 { "M" } else { "L" },
                x(p.0),
                y(p.1)
            );
        }
        let _ = write!(svg, r#"<path class="line s1" d="{d}"></path>"#);
    }
    svg.push_str("</svg>");
    svg
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn spec() -> Spec {
        Spec {
            title: "Memory <prod>".to_owned(),
            unit: Unit::Bytes,
            width: WIDTH,
            from: 0,
            to: 3600,
            step: 60,
            lines: vec![Line {
                label: "worker \"a\"".to_owned(),
                slot: 1,
                // A gap between 120 and 600: two runs.
                points: vec![(0, 1024.0), (60, 2048.0), (120, 3072.0), (600, 1024.0)],
            }],
            band: Some((
                "3 others".to_owned(),
                vec![(0, 10.0, 500.0), (60, 20.0, 600.0)],
            )),
            stacked: Vec::new(),
            limits: vec![("worker a's limit".to_owned(), 1, 4096.0)],
            zone: TimeZone::UTC,
        }
    }

    #[test]
    fn a_chart_draws_its_lines_band_limits_axes_and_hover_titles_escaped() {
        let svg = draw(&spec());
        assert!(svg.starts_with(r#"<svg class="chart""#), "{svg}");
        assert!(svg.contains(r#"aria-label="Memory &lt;prod&gt;""#));
        assert_eq!(
            svg.matches(r#"class="line s1""#).count(),
            1,
            "the run of three"
        );
        assert_eq!(
            svg.matches(r#"class="dot s1""#).count(),
            1,
            "the point alone"
        );
        assert_eq!(svg.matches(r#"class="band""#).count(), 1);
        assert!(svg.contains(r#"class="limit s1""#));
        assert!(svg.contains(">4 KiB</text>"), "the top tick: {svg}");
        assert!(svg.contains(">00:10</text>"), "a time tick");
        assert_eq!(svg.matches(r#"class="hover""#).count(), 4);
        assert!(svg.contains("worker &quot;a&quot;: 2.0 KiB"));
        assert!(svg.contains("3 others: 20 B to 600 B"));
    }

    #[test]
    fn stacked_layers_are_drawn_on_each_other_with_their_sum_on_hover() {
        let layer = |label: &str, slot, points: Vec<(i64, f64)>| Line {
            label: label.to_owned(),
            slot,
            points,
        };
        let svg = draw(&Spec {
            lines: Vec::new(),
            band: None,
            limits: Vec::new(),
            stacked: vec![
                layer("workers", 1, vec![(0, 1024.0), (60, 1024.0)]),
                layer("tasks", 2, vec![(60, 3072.0)]),
            ],
            ..spec()
        });
        assert_eq!(svg.matches(r#"class="area s1""#).count(), 1);
        assert_eq!(svg.matches(r#"class="area s2""#).count(), 1);
        assert!(svg.contains(">4 KiB</text>"), "the top is the sum: {svg}");
        assert!(
            svg.contains("tasks: 3.0 KiB\nworkers: 1.0 KiB\ntogether: 4.0 KiB"),
            "{svg}"
        );
    }

    #[test]
    fn a_limit_far_above_the_lines_is_left_off() {
        let svg = draw(&Spec {
            limits: vec![("limit".to_owned(), 1, 1e12)],
            ..spec()
        });
        assert!(!svg.contains("class=\"limit"));
    }

    #[test]
    fn the_y_axis_has_round_ticks_in_the_unit_of_its_top() {
        let (top, ticks) = y_axis(3.3 * 1024.0 * 1024.0 * 1024.0, Unit::Bytes);
        assert_eq!(
            top.to_bits(),
            (4.0_f64 * 1024.0 * 1024.0 * 1024.0).to_bits()
        );
        assert_eq!(ticks.len(), 5);
        let (top, ticks) = y_axis(0.0, Unit::Cores);
        let bits = |v: &[f64]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
        assert_eq!(
            (top.to_bits(), bits(&ticks)),
            (1.0_f64.to_bits(), bits(&[0.0, 0.5, 1.0]))
        );
        assert_eq!(axis_value(1.5 * 1024.0 * 1024.0, Unit::Bytes), "1.5 MiB");
        assert_eq!(axis_value(0.25, Unit::Cores), "0.25");
    }

    #[test]
    fn a_sparkline_is_a_small_line() {
        let svg = sparkline(&[(0, 1.0), (60, 2.0)], 0, 120, 60, "Memory");
        assert!(svg.contains(r#"class="line s1""#) && svg.contains("<title>Memory</title>"));
    }

    fn bar(value: Option<f64>, marked: bool) -> Bar {
        Bar {
            value,
            class: "ok",
            marked,
            href: "/r?a=1&b=2".to_owned(),
            title: "try <1>".to_owned(),
        }
    }

    #[test]
    fn bars_stand_on_the_baseline_rounded_at_the_top_and_link_to_their_attempts() {
        let spec = Bars {
            title: "Peaks".to_owned(),
            unit: Unit::Bytes,
            width: NARROW,
            bars: vec![
                bar(Some(512.0), false),
                bar(None, false),
                bar(Some(1024.0), true),
            ],
            limit: Some(("its memory limit".to_owned(), 2048.0)),
            ends: ("Nov 13 10:00".to_owned(), "Nov 14 10:00".to_owned()),
        };
        let svg = draw_bars(&spec);
        // A bar for each value; the one without stays an empty link with its title.
        assert_eq!(svg.matches(r#"<path class="bar ok""#).count(), 2);
        assert_eq!(svg.matches("<a href=").count(), 3);
        assert!(svg.contains(r#"<a href="/r?a=1&amp;b=2"><title>try &lt;1&gt;</title>"#));
        assert_eq!(svg.matches(r#"<text class="mark""#).count(), 1);
        assert!(svg.contains("<title>its memory limit: 2.0 KiB</title>"));
        assert!(svg.contains(">Nov 13 10:00</text>") && svg.contains(">Nov 14 10:00</text>"));
        // A limit far above the bars would flatten them: left out.
        let far = Bars {
            limit: Some(("its memory limit".to_owned(), 1e12)),
            ..spec.clone()
        };
        assert!(!draw_bars(&far).contains("class=\"limit\""));
        // One bar: one label.
        let one = Bars {
            bars: vec![bar(Some(1.0), false)],
            ..spec
        };
        let svg = draw_bars(&one);
        assert!(svg.contains(">Nov 13 10:00</text>") && !svg.contains(">Nov 14 10:00</text>"));
    }

    #[test]
    fn a_bars_outline_is_flat_on_the_baseline() {
        assert_eq!(
            bar_outline(10.0, 8.0, 100.0, 50.0),
            "M10.0,100.0V54.0Q10.0,50.0 14.0,50.0H14.0Q18.0,50.0 18.0,54.0V100.0Z"
        );
    }
}
