//! Renderer for the download running screen.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Gauge, List, ListItem, Paragraph, Tabs, Wrap};

use crate::tui::{App, FileEntry, FileOutcome, RunningTab};

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
        .block(Block::default().borders(Borders::ALL).title("Details"))
        .select(selected)
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(header, chunks[0]);

    match app.tab {
        RunningTab::InFlight => render_file_list(
            frame,
            chunks[1],
            app.files
                .iter()
                .filter(|entry| entry.outcome == FileOutcome::Pending),
            "queued / in flight",
        ),
        RunningTab::Completed => render_file_list(
            frame,
            chunks[1],
            app.files
                .iter()
                .filter(|entry| entry.outcome == FileOutcome::Downloaded),
            "completed",
        ),
        RunningTab::Failed => render_file_list(
            frame,
            chunks[1],
            app.files
                .iter()
                .filter(|entry| entry.outcome == FileOutcome::Failed),
            "failed",
        ),
        RunningTab::Log => render_log(frame, chunks[1], app),
    }
}

/// Renders a vertical list of file entries.
fn render_file_list<'a, I>(frame: &mut Frame, area: Rect, entries: I, title: &str)
where
    I: Iterator<Item = &'a FileEntry>,
{
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title.to_owned());
    let items: Vec<ListItem<'_>> = entries
        .map(|entry| {
            let (marker, style) = match entry.outcome {
                FileOutcome::Pending => ("…", Style::default().fg(Color::Yellow)),
                FileOutcome::Downloaded => ("✓", Style::default().fg(Color::Green)),
                FileOutcome::Failed => ("✗", Style::default().fg(Color::Red)),
            };
            let suffix = entry.error.as_deref().map_or_else(
                || format_bytes(entry.bytes),
                |error| format!("{} ({error})", format_bytes(entry.bytes)),
            );
            ListItem::new(Line::from(vec![
                Span::styled(format!("{marker} "), style),
                Span::raw(format!("{} ", entry.source.filepath)),
                Span::styled(suffix, Style::default().fg(Color::DarkGray)),
            ]))
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
