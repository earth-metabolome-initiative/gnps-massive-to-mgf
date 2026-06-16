//! Renderer for the download running screen.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{BarChart, Block, Borders, Gauge, List, ListItem, Paragraph, Tabs, Wrap};

use crate::aggregator::{AggregateSnapshot, Histogram};
use crate::tui::{App, RunningTab};

/// Renders the running screen onto `frame`.
pub fn render(frame: &mut Frame, app: &App) {
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
}

/// Renders the top status line: title + key hints.
fn render_header(frame: &mut Frame, area: Rect, app: &App) {
    let hint = if app.download_done {
        "download complete, press q to exit"
    } else {
        "Tab/Shift+Tab switch right pane | q quit (aborts in-flight transfer)"
    };
    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            "gnps-massive-to-mgf | download",
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

/// Renders the per-file gauge and aggregate counters.
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

    let ratio = app
        .current_delivered
        .saturating_mul(100)
        .checked_div(app.current_expected_bytes)
        .map_or(0_u16, |pct| u16::try_from(pct.min(100)).unwrap_or(0));
    let gauge = Gauge::default()
        .percent(ratio)
        .label(format!(
            "{} / {}",
            format_bytes(app.current_delivered),
            format_bytes(app.current_expected_bytes)
        ))
        .gauge_style(Style::default().fg(Color::Cyan));
    frame.render_widget(gauge, rows[0]);

    let totals = app.totals;
    let planned_files = totals.map_or(0, |totals| totals.planned_files);
    let planned_bytes = totals.map_or(0, |totals| totals.planned_bytes);
    let files_line = format!(
        "files  : {downloaded} downloaded, {failed} failed, {remaining} remaining of {planned_files} planned",
        downloaded = app.downloaded,
        failed = app.failed,
        remaining = app.remaining,
    );
    let bytes_line = format!(
        "bytes  : {} / {}",
        format_bytes(app.aggregate_bytes),
        format_bytes(planned_bytes),
    );
    let attempt_line = format!(
        "attempt: {}",
        if app.current_attempt == 0 {
            "(none)".to_owned()
        } else {
            app.current_attempt.to_string()
        },
    );

    frame.render_widget(Paragraph::new(files_line), rows[1]);
    frame.render_widget(Paragraph::new(bytes_line), rows[2]);
    frame.render_widget(Paragraph::new(attempt_line), rows[3]);
}

/// Renders the right pane: tab header + the body of the selected tab.
fn render_right(frame: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    let tabs = RunningTab::all();
    let titles = tabs
        .iter()
        .map(|tab| Line::from(tab.title()))
        .collect::<Vec<_>>();
    let selected = tabs.iter().position(|tab| *tab == app.tab).unwrap_or(0);
    let header = Tabs::new(titles)
        .block(Block::default().borders(Borders::ALL).title(format!(
            "Aggregates (files parsed: {})",
            app.aggregates.files_parsed
        )))
        .select(selected)
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(header, chunks[0]);

    match app.tab {
        RunningTab::Peaks => render_histogram_tab(
            frame,
            chunks[1],
            "peaks per spectrum",
            "peaks",
            &app.aggregates.peaks,
        ),
        RunningTab::Pepmass => render_histogram_tab(
            frame,
            chunks[1],
            "precursor m/z (MS2)",
            "m/z",
            &app.aggregates.pepmass,
        ),
        RunningTab::Charge => render_charge_tab(frame, chunks[1], &app.aggregates),
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

/// Renders the rolling log lines, newest at the bottom.
fn render_log(frame: &mut Frame, area: Rect, app: &App) {
    let block = Block::default().borders(Borders::ALL).title("log");
    let items: Vec<ListItem<'_>> = app
        .logs
        .iter()
        .map(|line| ListItem::new(Line::from(line.as_str())))
        .collect();
    let list = List::new(items).block(block);
    frame.render_widget(list, area);
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
    reason = "human-readable display only; the u64-to-f64 cast loses precision well outside any byte counter the TUI will see in practice"
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
