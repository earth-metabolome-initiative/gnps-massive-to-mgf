//! Renderer for the download running screen.

use std::time::{Duration, Instant};

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{BarChart, Block, Borders, Clear, Gauge, List, ListItem, Paragraph, Wrap};

use crate::aggregator::{AggregateSnapshot, Histogram};
use crate::tui::{App, InputMode, RunningTab};

/// Stall threshold after which the gauge label gains a `STALLED Xs` suffix.
const STALL_THRESHOLD: Duration = Duration::from_secs(5);
/// Hard cap on ETA display. Anything beyond this renders as `??`.
const ETA_DISPLAY_CAP: Duration = Duration::from_hours(999 * 24);

/// Renders the running screen onto `frame`.
pub fn render(frame: &mut Frame, app: &mut App) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(0)])
        .split(area);
    render_header(frame, chunks[0], app);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(chunks[1]);
    render_left(frame, body[0], app);
    render_right(frame, body[1], app);

    if matches!(app.input_mode, InputMode::DumpPathPrompt) {
        render_dump_prompt(frame, app);
    }
}

/// Renders the modal "dump log to file" popup over the running screen.
fn render_dump_prompt(frame: &mut Frame, app: &App) {
    let area = centered_rect(60, 5, frame.area());
    frame.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title("dump log to file  (Enter to write, Esc to cancel)")
        .border_style(Style::default().fg(Color::Yellow));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let prompt = Paragraph::new(Line::from(vec![
        Span::styled("path: ", Style::default().fg(Color::DarkGray)),
        Span::raw(app.dump_path_input.as_str()),
        Span::styled("_", Style::default().add_modifier(Modifier::SLOW_BLINK)),
    ]));
    frame.render_widget(prompt, inner);
}

/// Renders the right-pane tab header with equal-width cells and records each
/// cell's `Rect` into `app.tab_rects` so [`App::on_mouse`] can map clicks to
/// tabs. The active tab is filled with the highlight style.
fn render_tab_header(frame: &mut Frame, inner: Rect, app: &mut App, tabs: &[RunningTab]) {
    app.tab_rects.clear();
    if inner.width == 0 || inner.height == 0 || tabs.is_empty() {
        return;
    }
    let denominator = u32::try_from(tabs.len()).unwrap_or(1).max(1);
    let constraints: Vec<Constraint> = tabs
        .iter()
        .map(|_| Constraint::Ratio(1, denominator))
        .collect();
    let cells = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(constraints)
        .split(inner);
    for (cell, tab) in cells.iter().zip(tabs.iter()) {
        let is_active = *tab == app.tab;
        let style = if is_active {
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Gray)
        };
        let label = Paragraph::new(Line::from(Span::styled(tab.title(), style)))
            .alignment(Alignment::Center)
            .style(style);
        frame.render_widget(label, *cell);
        app.tab_rects.push((*tab, *cell));
    }
}

/// Centers `width_percent` x `height_rows` inside `area`.
fn centered_rect(width_percent: u16, height_rows: u16, area: Rect) -> Rect {
    let vert = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0),
            Constraint::Length(height_rows),
            Constraint::Min(0),
        ])
        .split(area);
    let horiz = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - width_percent) / 2),
            Constraint::Percentage(width_percent),
            Constraint::Percentage((100 - width_percent) / 2),
        ])
        .split(vert[1]);
    horiz[1]
}

/// Renders the top status line: title + key hints.
fn render_header(frame: &mut Frame, area: Rect, app: &App) {
    let hint = if app.download_done {
        "download complete, press q to exit (d dump log to file)"
    } else {
        "Tab/Shift+Tab switch right pane | d dump log | q quit (aborts in-flight transfer)"
    };
    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            "massive | download",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(hint, Style::default().fg(Color::DarkGray)),
    ]));
    frame.render_widget(header, area);
}

/// Renders the left pane: current file metadata + progress gauges.
fn render_left(frame: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(10), Constraint::Length(8)])
        .split(area);
    render_current_file(frame, chunks[0], app);
    render_progress(frame, chunks[1], app);
}

/// Renders the metadata block for the in-flight file.
fn render_current_file(frame: &mut Frame, area: Rect, app: &App) {
    let block = Block::default().borders(Borders::ALL).title("Current file");
    let lines = app.current.as_ref().map_or_else(
        || {
            vec![Line::from(Span::styled(
                "(idle, waiting for next file)",
                Style::default().fg(Color::DarkGray),
            ))]
        },
        |row| {
            let mut out = vec![
                kv("Filepath", row.filepath.clone()),
                kv("Dataset", row.dataset.clone()),
                kv("Collection", row.collection.clone()),
                kv("Vendor", row.instrument_vendor.clone()),
                kv("Model", row.instrument_model.clone()),
                kv("Extension", row.extension.clone()),
                kv("Size", format_bytes(app.current_expected_bytes)),
                kv(
                    "Volume",
                    row.volume.clone().unwrap_or_else(|| "(missing)".to_owned()),
                ),
                kv("Status", row.status.clone()),
                kv(
                    "Attempt",
                    if app.current_attempt == 0 {
                        "queued".to_owned()
                    } else {
                        app.current_attempt.to_string()
                    },
                ),
            ];
            if let Some(ms2) = row.spectra_ms2 {
                out.push(kv("Indexed MS2", format!("{ms2:.0}")));
            }
            out
        },
    );
    let paragraph = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}

/// Renders the per-file gauge, the aggregate gauge, and the counter lines.
fn render_progress(frame: &mut Frame, area: Rect, app: &App) {
    let block = Block::default().borders(Borders::ALL).title("Progress");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .split(inner);

    let totals = app.totals;
    let planned_files = totals.map_or(0, |totals| totals.planned_files);
    let planned_bytes = totals.map_or(0, |totals| totals.planned_bytes);

    let now = Instant::now();
    let file_ratio = app
        .current_delivered
        .saturating_mul(100)
        .checked_div(app.current_expected_bytes)
        .map_or(0_u16, |pct| u16::try_from(pct.min(100)).unwrap_or(0));
    let file_remaining = app
        .current_expected_bytes
        .saturating_sub(app.current_delivered);
    let file_stall = stall_suffix(now, app.last_file_advance_at);
    let file_gauge = Gauge::default()
        .percent(file_ratio)
        .label(format!(
            "file  {} / {}  {}  eta {}{}",
            format_bytes(app.current_delivered),
            format_bytes(app.current_expected_bytes),
            format_rate(app.file_rate_bps),
            format_eta(app.file_rate_bps, file_remaining),
            file_stall,
        ))
        .gauge_style(Style::default().fg(if file_stall.is_empty() {
            Color::Cyan
        } else {
            Color::Red
        }));
    frame.render_widget(file_gauge, rows[0]);

    let total_ratio = app
        .aggregate_bytes
        .saturating_mul(100)
        .checked_div(planned_bytes)
        .map_or(0_u16, |pct| u16::try_from(pct.min(100)).unwrap_or(0));
    let total_remaining = planned_bytes.saturating_sub(app.aggregate_bytes);
    let total_stall = stall_suffix(now, app.last_total_advance_at);
    let total_gauge = Gauge::default()
        .percent(total_ratio)
        .label(format!(
            "total {} / {}  {}  eta {}{}",
            format_bytes(app.aggregate_bytes),
            format_bytes(planned_bytes),
            format_rate(app.total_rate_bps),
            format_eta(app.total_rate_bps, total_remaining),
            total_stall,
        ))
        .gauge_style(Style::default().fg(if total_stall.is_empty() {
            Color::Green
        } else {
            Color::Red
        }));
    frame.render_widget(total_gauge, rows[1]);

    let files_line = format!(
        "files  : {downloaded} downloaded, {failed} failed, {remaining} remaining of {planned_files} planned",
        downloaded = app.downloaded,
        failed = app.failed,
        remaining = app.remaining,
    );
    let attempt_line = format!(
        "attempt: {}",
        if app.current_attempt == 0 {
            "(none)".to_owned()
        } else {
            app.current_attempt.to_string()
        },
    );

    frame.render_widget(Paragraph::new(files_line), rows[2]);
    frame.render_widget(Paragraph::new(attempt_line), rows[3]);
}

/// Renders the right pane: tab header + the body of the selected tab.
fn render_right(frame: &mut Frame, area: Rect, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    let tabs = RunningTab::all();
    let block = Block::default().borders(Borders::ALL).title(format!(
        "Aggregates (files parsed: {})",
        app.aggregates.files_parsed
    ));
    let inner = block.inner(chunks[0]);
    frame.render_widget(block, chunks[0]);
    render_tab_header(frame, inner, app, &tabs);

    match app.tab {
        RunningTab::Peaks => render_histogram_tab(
            frame,
            chunks[1],
            "peak count per spectrum",
            "peaks",
            &app.aggregates.peaks,
        ),
        RunningTab::Ms2PerFile => render_histogram_tab(
            frame,
            chunks[1],
            "MS2 spectra per file",
            "ms2",
            &app.aggregates.ms2_per_file,
        ),
        RunningTab::Pepmass => render_histogram_tab(
            frame,
            chunks[1],
            "precursor m/z (MS2)",
            "m/z",
            &app.aggregates.pepmass,
        ),
        RunningTab::Charge => render_charge_tab(frame, chunks[1], &app.aggregates),
        RunningTab::Polarity => render_label_counts(
            frame,
            chunks[1],
            "scan polarity",
            &app.aggregates.polarities,
            "(waiting for first parsed spectrum)",
        ),
        RunningTab::Activation => render_label_counts(
            frame,
            chunks[1],
            "MS2 dissociation methods",
            &app.aggregates.activations,
            "(waiting for first MS2 with an activation method)",
        ),
        RunningTab::Instrument => render_instrument_tab(frame, chunks[1], &app.aggregates),
        RunningTab::Log => render_log(frame, chunks[1], app),
    }
}

/// Renders a histogram tab: summary line on top, bar chart underneath.
fn render_histogram_tab(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    unit_label: &str,
    histogram: &Histogram,
) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title.to_owned());
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if histogram.count == 0 {
        let paragraph = Paragraph::new(Span::styled(
            "(waiting for first parsed spectrum)",
            Style::default().fg(Color::DarkGray),
        ));
        frame.render_widget(paragraph, inner);
        return;
    }

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(inner);

    let mean = histogram.mean;
    let p25 = histogram.quantile(0.25).unwrap_or(0.0);
    let median = histogram.quantile(0.5).unwrap_or(0.0);
    let p75 = histogram.quantile(0.75).unwrap_or(0.0);
    let p95 = histogram.quantile(0.95).unwrap_or(0.0);
    let summary = vec![
        Line::from(format!(
            "count = {count}    mean = {mean:.2} {unit_label}    min = {min:.2}    max = {max:.2}",
            count = histogram.count,
            min = histogram.min,
            max = histogram.max,
        )),
        Line::from(format!(
            "p25 = {p25:.2}    median = {median:.2}    p75 = {p75:.2}    p95 = {p95:.2}"
        )),
    ];
    let paragraph = Paragraph::new(summary).wrap(Wrap { trim: false });
    frame.render_widget(paragraph, rows[0]);

    render_bar_chart(frame, rows[1], histogram);
}

/// Renders a `ratatui::widgets::BarChart` from a histogram.
fn render_bar_chart(frame: &mut Frame, area: Rect, histogram: &Histogram) {
    let labels: Vec<String> = bucket_labels(histogram);
    let data: Vec<(&str, u64)> = labels
        .iter()
        .zip(histogram.buckets.iter())
        .map(|(label, value)| (label.as_str(), *value))
        .collect();
    let chart = BarChart::default()
        .block(Block::default().borders(Borders::TOP).title("distribution"))
        .bar_width(4)
        .bar_gap(1)
        .data(&data)
        .bar_style(Style::default().fg(Color::Cyan))
        .value_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(chart, area);
}

/// Bucket midpoint labels for the bar chart.
fn bucket_labels(histogram: &Histogram) -> Vec<String> {
    let n = histogram.buckets.len();
    if n == 0 {
        return Vec::new();
    }
    #[allow(
        clippy::as_conversions,
        clippy::cast_precision_loss,
        reason = "labels are display-only; precision loss is fine"
    )]
    let span = (histogram_high(histogram) - histogram_low(histogram)) / n as f64;
    #[allow(
        clippy::as_conversions,
        clippy::cast_precision_loss,
        reason = "labels are display-only, precision loss is fine"
    )]
    (0..n)
        .map(|i| {
            format!(
                "{:.0}",
                (i as f64 + 0.5).mul_add(span, histogram_low(histogram))
            )
        })
        .collect()
}

/// Histogram lower bound (accessor wrapper to keep callers tidy).
const fn histogram_low(_histogram: &Histogram) -> f64 {
    0.0
}

/// Histogram upper bound (accessor wrapper to keep callers tidy).
#[allow(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "labels are display-only, precision loss is fine"
)]
const fn histogram_high(histogram: &Histogram) -> f64 {
    // The histogram never exposes its bucket bounds publicly to keep the
    // surface small, so we reconstruct an upper bound from `max`.
    histogram.max.max(histogram.buckets.len() as f64).max(1.0)
}

/// Renders the charge-state count tab.
fn render_charge_tab(frame: &mut Frame, area: Rect, snapshot: &AggregateSnapshot) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title("precursor charge counts (MS2)");
    if snapshot.charges.is_empty() {
        let paragraph = Paragraph::new(Span::styled(
            "(waiting for first MS2 precursor)",
            Style::default().fg(Color::DarkGray),
        ))
        .block(block);
        frame.render_widget(paragraph, area);
        return;
    }
    let items: Vec<ListItem<'_>> = snapshot
        .charges
        .iter()
        .map(|(charge, count)| {
            let sign = if *charge >= 0 { "+" } else { "" };
            ListItem::new(Line::from(format!("{sign}{charge:>3}    {count}")))
        })
        .collect();
    let list = List::new(items).block(block);
    frame.render_widget(list, area);
}

/// Renders a generic "label -> count" tab as a list sorted by count desc.
fn render_label_counts(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    counts: &std::collections::BTreeMap<String, u64>,
    empty_message: &str,
) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title.to_owned());
    if counts.is_empty() {
        let paragraph = Paragraph::new(Span::styled(
            empty_message.to_owned(),
            Style::default().fg(Color::DarkGray),
        ))
        .block(block);
        frame.render_widget(paragraph, area);
        return;
    }
    let mut rows: Vec<(&String, &u64)> = counts.iter().collect();
    rows.sort_by(|a, b| b.1.cmp(a.1));
    let items: Vec<ListItem<'_>> = rows
        .into_iter()
        .map(|(label, count)| ListItem::new(Line::from(format!("{count:>8}  {label}"))))
        .collect();
    let list = List::new(items).block(block);
    frame.render_widget(list, area);
}

/// Renders the instrument file-count tab.
fn render_instrument_tab(frame: &mut Frame, area: Rect, snapshot: &AggregateSnapshot) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title("file counts per (vendor, model)");
    if snapshot.instruments.is_empty() {
        let paragraph = Paragraph::new(Span::styled(
            "(waiting for first parsed file)",
            Style::default().fg(Color::DarkGray),
        ))
        .block(block);
        frame.render_widget(paragraph, area);
        return;
    }
    let mut rows: Vec<(&(String, String), &u64)> = snapshot.instruments.iter().collect();
    rows.sort_by(|a, b| b.1.cmp(a.1));
    let items: Vec<ListItem<'_>> = rows
        .into_iter()
        .map(|((vendor, model), count)| {
            ListItem::new(Line::from(format!("{count:>6}  {vendor} / {model}")))
        })
        .collect();
    let list = List::new(items).block(block);
    frame.render_widget(list, area);
}

/// Renders the rolling log lines, newest at the bottom. Honors
/// `app.log_scroll_offset` so the user can scroll back into older entries
/// with `Up` / `Down` / `PgUp` / `PgDn` / `Home` / `End` or the mouse wheel.
/// Lines are tinted by [`log_severity`] so failures pop visually.
fn render_log(frame: &mut Frame, area: Rect, app: &App) {
    let total = app.logs.len();
    let scroll = app.log_scroll_offset.min(total);
    let title = if scroll == 0 {
        format!("log ({total} lines, bottom | Up/Down/PgUp/PgDn/Home/End/wheel)")
    } else {
        format!("log ({total} lines, +{scroll} from bottom | Up/Down/PgUp/PgDn/Home/End/wheel)")
    };
    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    let visible_rows = usize::from(inner.height);
    let end = total.saturating_sub(scroll);
    let start = end.saturating_sub(visible_rows);
    let items: Vec<ListItem<'_>> = app
        .logs
        .iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .map(|line| {
            ListItem::new(Line::from(Span::styled(
                line.as_str(),
                log_severity(line).style(),
            )))
        })
        .collect();
    let list = List::new(items).block(block);
    frame.render_widget(list, area);
}

/// Severity bucket assigned to a log line by [`log_severity`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogSeverity {
    /// Routine status (default color).
    Info,
    /// Successful action (green).
    Success,
    /// Recoverable issue, retry, or corrected data (yellow).
    Warning,
    /// Failure, refusal, or timeout (red).
    Error,
}

impl LogSeverity {
    /// Maps the severity to a [`Style`] for terminal rendering.
    const fn style(self) -> Style {
        match self {
            Self::Info => Style::new(),
            Self::Success => Style::new().fg(Color::Green),
            Self::Warning => Style::new().fg(Color::Yellow),
            Self::Error => Style::new().fg(Color::Red),
        }
    }
}

/// Classifies a log line by simple substring matching on the formats emitted
/// by `src/massive.rs` and the TUI itself.
fn log_severity(line: &str) -> LogSeverity {
    let lower = line.to_ascii_lowercase();
    let attempt_failed = lower.starts_with("attempt ") && lower.contains(" failed");
    if lower.starts_with("download failed:")
        || lower.starts_with("failed to ")
        || lower.contains("refused")
        || lower.contains("did not respond")
        || lower.contains("delivered no bytes")
        || lower.contains("did not accept")
        || attempt_failed
    {
        LogSeverity::Error
    } else if lower.starts_with("warning:")
        || lower.starts_with("sleeping ")
        || lower.starts_with("retrying ")
        || lower.starts_with("correcting indexed size")
        || lower.contains("restarting from zero")
        || lower.contains("is larger than")
        || lower.contains("unavailable")
        || lower.contains("dump cancelled")
        || (lower.contains("reports") && lower.contains("(index expected"))
    {
        LogSeverity::Warning
    } else if lower.starts_with("downloaded:")
        || lower.starts_with("already downloaded:")
        || lower.starts_with("wrote ")
        || lower.starts_with("copied ")
    {
        LogSeverity::Success
    } else {
        LogSeverity::Info
    }
}

/// Single key/value row.
fn kv(label: &str, value: String) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("{label:>11}: "),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw(value),
    ])
}

/// Formats a byte count as a human-readable string.
#[allow(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "human-readable display only, the u64-to-f64 cast loses precision well outside any byte counter the TUI will see in practice"
)]
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes == 0 {
        return "0 B".to_owned();
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.2} {}", UNITS[unit])
}

/// Formats a throughput in human-readable form, decimal units for familiarity.
fn format_rate(bps: Option<f64>) -> String {
    match bps {
        None => "(measuring)".to_owned(),
        Some(rate) if !rate.is_finite() || rate < 0.0 => "(measuring)".to_owned(),
        Some(rate) if rate >= 1_000_000.0 => format!("{:.2} MB/s", rate / 1_000_000.0),
        Some(rate) if rate >= 1_000.0 => format!("{:.1} kB/s", rate / 1_000.0),
        Some(rate) => format!("{rate:.0} B/s"),
    }
}

/// Formats an ETA computed from a rate and a remaining byte count.
///
/// Treats rates below 1 kB/s as "effectively zero" so the EMA's slow decay
/// after a stall cannot produce nonsensical 10^14-day ETAs.
#[allow(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "ETA is display-only seconds; truncating from f64 to u64 is intentional"
)]
fn format_eta(rate: Option<f64>, remaining_bytes: u64) -> String {
    let Some(rate) = rate else {
        return "??".to_owned();
    };
    if !rate.is_finite() || rate < 1_000.0 {
        return "??".to_owned();
    }
    let seconds = remaining_bytes as f64 / rate;
    if !seconds.is_finite() || seconds < 0.0 {
        return "??".to_owned();
    }
    let total = seconds as u64;
    if total >= ETA_DISPLAY_CAP.as_secs() {
        return "??".to_owned();
    }
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    if h >= 24 {
        let d = h / 24;
        let hh = h % 24;
        format!("{d}d {hh}h")
    } else if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}

/// Builds the `STALLED Xs` suffix when the last byte advance is too old.
fn stall_suffix(now: Instant, last_advance: Option<Instant>) -> String {
    let Some(at) = last_advance else {
        return String::new();
    };
    let elapsed = now.saturating_duration_since(at);
    if elapsed < STALL_THRESHOLD {
        return String::new();
    }
    let s = elapsed.as_secs();
    if s >= 3600 {
        format!("  STALLED {}h {}m", s / 3600, (s % 3600) / 60)
    } else if s >= 60 {
        format!("  STALLED {}m {}s", s / 60, s % 60)
    } else {
        format!("  STALLED {s}s")
    }
}
