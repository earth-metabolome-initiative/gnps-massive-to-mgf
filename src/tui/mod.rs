//! `ratatui` terminal UI for the `download` subcommand.
//!
//! The TUI runs on the existing `current_thread` tokio runtime alongside the
//! single in-flight FTPS download. `tokio::select!` interleaves five sources:
//! a fixed-cadence render tick, the [`ProgressEvent`] channel from the
//! download path, snapshot updates from the spectrum aggregator, the
//! `crossterm` event stream, and the download future itself. Pressing `q`,
//! `Esc`, or `Ctrl+C` exits the loop. If the download is still in flight,
//! dropping its pinned future cancels the FTPS transfer (the partial `.part`
//! file persists for FTP `REST` resume on the next run).

use std::collections::VecDeque;
use std::fs;
use std::io::Stdout;
use std::io::stdout;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::execute;
use futures_util::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::Rect;

use crate::aggregator::{self, AggregateEvent, AggregateSnapshot, AggregatorJob};
use crate::config::Config;
use crate::db::{DownloadFilter, DownloadProgressTotals, SourceFileRow, StateDb};
use crate::massive::download_pending_mzml;
use crate::progress_sink::{ProgressEvent, ProgressSink};

pub mod running;

/// Maximum log lines retained in the running screen's `Log` tab.
const MAX_LOG_LINES: usize = 256;
/// Render tick cadence. ~12 fps is plenty for FTPS throughput.
const TICK_RATE: Duration = Duration::from_millis(80);
/// Number of log lines scrolled by one mouse-wheel turn (terminal convention).
const MOUSE_WHEEL_STEP: usize = 3;

/// Returns whether `rect` covers the given `(column, row)` cell.
const fn rect_contains(rect: Rect, column: u16, row: u16) -> bool {
    column >= rect.x && column < rect.x + rect.width && row >= rect.y && row < rect.y + rect.height
}

/// Right-pane tab the user is currently viewing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RunningTab {
    /// Peak-count distribution across every parsed spectrum.
    #[default]
    Peaks,
    /// Distribution of MS2-spectrum count per file.
    Ms2PerFile,
    /// Precursor m/z distribution across MS2 spectra.
    Pepmass,
    /// Spectrum counts per precursor charge state.
    Charge,
    /// Spectrum counts per scan polarity.
    Polarity,
    /// MS2 spectrum counts per dissociation method.
    Activation,
    /// File counts per `(vendor, model)` pair.
    Instrument,
    /// Log scrollback (status messages, file failures).
    Log,
}

/// Input handling mode.
///
/// The [`App`] is in [`Self::Normal`] while the user is browsing the running
/// screen. Entering a popup such as the dump-log prompt switches to
/// [`Self::DumpPathPrompt`] which redirects key presses into the buffer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum InputMode {
    /// Normal navigation: tab switching, quit, copy, dump.
    #[default]
    Normal,
    /// Modal text input for the "dump log to file" path prompt.
    DumpPathPrompt,
}

impl RunningTab {
    /// Ordered list of tabs cycled by `Tab` / `Shift+Tab`.
    #[must_use]
    pub const fn all() -> [Self; 8] {
        [
            Self::Peaks,
            Self::Ms2PerFile,
            Self::Pepmass,
            Self::Charge,
            Self::Polarity,
            Self::Activation,
            Self::Instrument,
            Self::Log,
        ]
    }

    /// Title shown in the [`ratatui::widgets::Tabs`] header.
    #[must_use]
    pub const fn title(self) -> &'static str {
        match self {
            Self::Peaks => "#Peaks",
            Self::Ms2PerFile => "MS2/file",
            Self::Pepmass => "Pepmass",
            Self::Charge => "Charge",
            Self::Polarity => "Polarity",
            Self::Activation => "Activation",
            Self::Instrument => "Instrument",
            Self::Log => "Log",
        }
    }
}

/// Application state shared between the event handler and the renderer.
#[derive(Debug, Default)]
pub struct App {
    /// Cumulative workload totals from [`ProgressEvent::DownloadStarted`].
    pub totals: Option<DownloadProgressTotals>,
    /// Files still in `indexed` status.
    pub remaining: u64,
    /// Cumulative downloaded count (includes already-present files).
    pub downloaded: u64,
    /// Cumulative failure count.
    pub failed: u64,
    /// In-flight file's metadata.
    pub current: Option<SourceFileRow>,
    /// In-flight file's current attempt number.
    pub current_attempt: usize,
    /// In-flight file's expected total bytes.
    pub current_expected_bytes: u64,
    /// In-flight file's bytes delivered so far in this attempt.
    pub current_delivered: u64,
    /// Cumulative bytes credited to the aggregate-bytes total.
    pub aggregate_bytes: u64,
    /// Latest snapshot from the spectrum aggregator.
    pub aggregates: AggregateSnapshot,
    /// Log scrollback.
    pub logs: VecDeque<String>,
    /// Currently selected right-pane tab.
    pub tab: RunningTab,
    /// Set by `q` / `Esc` / `Ctrl+C`.
    pub quit_requested: bool,
    /// Set when [`ProgressEvent::DownloadFinished`] is received.
    pub download_done: bool,
    /// High-water mark of bytes credited for the in-flight file.
    in_flight_high_water: u64,
    /// Last (instant, bytes) sample used to compute the per-file rate EMA.
    file_rate_sample: Option<(Instant, u64)>,
    /// Last (instant, bytes) sample used to compute the aggregate rate EMA.
    total_rate_sample: Option<(Instant, u64)>,
    /// EMA of per-file bytes per second (None until two samples have arrived).
    pub file_rate_bps: Option<f64>,
    /// EMA of aggregate bytes per second (None until two samples have arrived).
    pub total_rate_bps: Option<f64>,
    /// Wall-clock instant of the last per-file byte advance.
    pub last_file_advance_at: Option<Instant>,
    /// Wall-clock instant of the last aggregate byte advance.
    pub last_total_advance_at: Option<Instant>,
    /// Current input mode; controls how key presses are dispatched.
    pub input_mode: InputMode,
    /// Buffer fed by [`InputMode::DumpPathPrompt`] characters.
    pub dump_path_input: String,
    /// Number of log lines hidden below the bottom of the visible window when
    /// the user has scrolled up. `0` means "stay pinned to the newest line".
    pub log_scroll_offset: usize,
    /// Clickable rectangle for each right-pane tab header, populated by the
    /// renderer each frame. Consumed by the mouse handler to map a click
    /// position to a tab.
    pub tab_rects: Vec<(RunningTab, Rect)>,
}

impl App {
    /// Builds a fresh application with the default tab selection.
    fn new() -> Self {
        Self {
            tab: RunningTab::Peaks,
            ..Self::default()
        }
    }

    /// Handles one terminal event.
    pub fn on_terminal_event(&mut self, event: &Event) {
        match event {
            Event::Key(KeyEvent {
                code,
                modifiers,
                kind,
                ..
            }) => {
                if *kind != KeyEventKind::Press {
                    return;
                }
                self.on_key(*code, *modifiers);
            }
            Event::Mouse(mouse_event) => self.on_mouse(*mouse_event),
            _ => {}
        }
    }

    /// Handles one mouse event. Wired interactions:
    ///
    /// - Left click on a right-pane tab header switches to that tab.
    /// - Wheel over the `Log` tab scrolls [`MOUSE_WHEEL_STEP`] lines per turn.
    fn on_mouse(&mut self, event: MouseEvent) {
        match event.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                for (tab, rect) in &self.tab_rects {
                    if rect_contains(*rect, event.column, event.row) {
                        self.tab = *tab;
                        return;
                    }
                }
            }
            MouseEventKind::ScrollUp if self.tab == RunningTab::Log => {
                self.log_scroll_offset = self.log_scroll_offset.saturating_add(MOUSE_WHEEL_STEP);
                if self.log_scroll_offset > self.logs.len() {
                    self.log_scroll_offset = self.logs.len();
                }
            }
            MouseEventKind::ScrollDown if self.tab == RunningTab::Log => {
                self.log_scroll_offset = self.log_scroll_offset.saturating_sub(MOUSE_WHEEL_STEP);
            }
            _ => {}
        }
    }

    /// Dispatches a key press. The dispatch path depends on the current
    /// [`InputMode`]: text-input modes consume the keystroke for the buffer,
    /// while [`InputMode::Normal`] runs the global shortcut table.
    fn on_key(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        match self.input_mode {
            InputMode::DumpPathPrompt => self.on_key_dump_prompt(code),
            InputMode::Normal => self.on_key_normal(code, modifiers),
        }
    }

    /// Global shortcut handler used in [`InputMode::Normal`].
    fn on_key_normal(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        if self.tab == RunningTab::Log && self.on_key_log_scroll(code) {
            return;
        }
        match code {
            KeyCode::Char('q') | KeyCode::Esc => self.quit_requested = true,
            KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => {
                self.quit_requested = true;
            }
            KeyCode::Char('d') => {
                self.dump_path_input.clear();
                self.input_mode = InputMode::DumpPathPrompt;
            }
            KeyCode::Tab | KeyCode::Right => self.advance_tab(1),
            KeyCode::BackTab | KeyCode::Left => self.advance_tab(-1),
            _ => {}
        }
    }

    /// Handles scroll keys while the `Log` tab is active. Returns `true` when
    /// the key was a scroll action so the global shortcut table is skipped.
    fn on_key_log_scroll(&mut self, code: KeyCode) -> bool {
        const PAGE: usize = 10;
        match code {
            KeyCode::Up => self.log_scroll_offset = self.log_scroll_offset.saturating_add(1),
            KeyCode::Down => self.log_scroll_offset = self.log_scroll_offset.saturating_sub(1),
            KeyCode::PageUp => self.log_scroll_offset = self.log_scroll_offset.saturating_add(PAGE),
            KeyCode::PageDown => {
                self.log_scroll_offset = self.log_scroll_offset.saturating_sub(PAGE);
            }
            KeyCode::Home => self.log_scroll_offset = self.logs.len(),
            KeyCode::End => self.log_scroll_offset = 0,
            _ => return false,
        }
        if self.log_scroll_offset > self.logs.len() {
            self.log_scroll_offset = self.logs.len();
        }
        true
    }

    /// Key handler used while the dump-path prompt is open.
    fn on_key_dump_prompt(&mut self, code: KeyCode) {
        match code {
            KeyCode::Esc => {
                self.dump_path_input.clear();
                self.input_mode = InputMode::Normal;
            }
            KeyCode::Enter => {
                let path = std::mem::take(&mut self.dump_path_input);
                self.input_mode = InputMode::Normal;
                self.dump_log_to_file(path.trim());
            }
            KeyCode::Backspace => {
                self.dump_path_input.pop();
            }
            KeyCode::Char(character) => {
                self.dump_path_input.push(character);
            }
            _ => {}
        }
    }

    /// Writes the in-memory log to `path` (one line per record, no trailing
    /// newline). Reports the outcome back to the user via [`Self::push_log`].
    fn dump_log_to_file(&mut self, path: &str) {
        if path.is_empty() {
            self.push_log("dump cancelled: empty path".to_owned());
            return;
        }
        let path = PathBuf::from(path);
        let payload = self.logs.iter().cloned().collect::<Vec<_>>().join("\n");
        let line_count = self.logs.len();
        match fs::write(&path, &payload) {
            Ok(()) => self.push_log(format!(
                "wrote {line_count} log line(s) to {}",
                path.display(),
            )),
            Err(error) => self.push_log(format!("failed to write {}: {error}", path.display())),
        }
    }

    /// Cycles the selected tab by `step` (negative = backwards).
    fn advance_tab(&mut self, step: i32) {
        let tabs = RunningTab::all();
        let len = tabs.len();
        let current = tabs.iter().position(|t| *t == self.tab).unwrap_or(0);
        let length = i32::try_from(len).unwrap_or(1).max(1);
        let raw = i32::try_from(current).unwrap_or(0) + step;
        let normalised = raw.rem_euclid(length);
        let next = usize::try_from(normalised).unwrap_or(0);
        self.tab = tabs[next];
    }

    /// Handles one progress event from the download path. When a file
    /// completes, a job is sent to the spectrum aggregator over `aggregator`.
    pub fn on_progress_event(&mut self, event: ProgressEvent, aggregator: &Sender<AggregatorJob>) {
        match event {
            ProgressEvent::Log(message) => self.push_log(message),
            ProgressEvent::DownloadStarted {
                totals,
                remaining_indexed,
            } => {
                self.totals = Some(totals);
                self.remaining = remaining_indexed;
                self.downloaded = totals.completed_files;
                self.aggregate_bytes = totals.completed_bytes;
            }
            ProgressEvent::FileStarted { source } => {
                let row = *source;
                self.current_attempt = 0;
                self.current_expected_bytes = u64::try_from(row.size_bytes).unwrap_or(0);
                self.current_delivered = 0;
                self.in_flight_high_water = 0;
                self.file_rate_sample = None;
                self.file_rate_bps = None;
                self.last_file_advance_at = Some(Instant::now());
                self.current = Some(row);
            }
            ProgressEvent::FileAlreadyPresent { bytes } => {
                self.current_delivered = bytes;
                self.credit_in_flight(bytes);
            }
            ProgressEvent::AttemptStarted {
                attempt,
                expected_bytes,
                resume_offset,
            } => {
                self.current_attempt = attempt;
                self.current_expected_bytes = expected_bytes;
                self.current_delivered = resume_offset;
                self.credit_in_flight(resume_offset);
                // A new attempt restarts from `resume_offset`, so the rate
                // sample taken before the restart is no longer comparable.
                self.file_rate_sample = None;
                self.last_file_advance_at = Some(Instant::now());
            }
            ProgressEvent::FileBytes { delivered } => {
                let advanced = delivered > self.current_delivered;
                if advanced {
                    self.current_delivered = delivered;
                }
                let prior_total = self.aggregate_bytes;
                self.credit_in_flight(delivered);
                let now = Instant::now();
                if advanced {
                    self.last_file_advance_at = Some(now);
                }
                if self.aggregate_bytes > prior_total {
                    self.last_total_advance_at = Some(now);
                }
            }
            ProgressEvent::AttemptFinished => {}
            ProgressEvent::FileFinished { local_path, bytes } => {
                self.current_delivered = bytes;
                self.credit_in_flight(bytes);
                self.downloaded = self.downloaded.saturating_add(1);
                self.remaining = self.remaining.saturating_sub(1);
                if let Some(row) = self.current.take() {
                    self.push_log(format!("downloaded: {}", row.filepath));
                    drop(aggregator.send(AggregatorJob {
                        local_path,
                        instrument_vendor: row.instrument_vendor,
                        instrument_model: row.instrument_model,
                    }));
                }
            }
            ProgressEvent::FileFailed { filepath, error } => {
                self.failed = self.failed.saturating_add(1);
                self.remaining = self.remaining.saturating_sub(1);
                self.push_log(format!("download failed: {filepath} | {error}"));
                self.current = None;
            }
            ProgressEvent::DownloadFinished { downloaded, failed } => {
                self.downloaded = downloaded;
                self.failed = failed;
                self.download_done = true;
            }
        }
    }

    /// Handles one update from the spectrum aggregator.
    pub fn on_aggregate_event(&mut self, event: AggregateEvent) {
        match event {
            AggregateEvent::Snapshot(snapshot) => self.aggregates = *snapshot,
            AggregateEvent::Error(message) => self.push_log(message),
        }
    }

    /// Refreshes the per-file and aggregate throughput EMAs from the current
    /// byte counters. Intended to be called once per render tick.
    #[allow(
        clippy::as_conversions,
        clippy::cast_precision_loss,
        reason = "bytes/sec math is descriptive; lossy u64 to f64 conversions are fine here"
    )]
    pub fn update_rates(&mut self, now: Instant) {
        const ALPHA: f64 = 0.3;
        const MIN_INTERVAL: Duration = Duration::from_millis(250);
        if let Some((then, bytes)) = self.file_rate_sample {
            if now.duration_since(then) >= MIN_INTERVAL {
                let dt = now.duration_since(then).as_secs_f64();
                let delta = self.current_delivered.saturating_sub(bytes) as f64;
                let instant = if dt > 0.0 { delta / dt } else { 0.0 };
                self.file_rate_bps = Some(
                    self.file_rate_bps
                        .map_or(instant, |prev| ALPHA.mul_add(instant, (1.0 - ALPHA) * prev)),
                );
                self.file_rate_sample = Some((now, self.current_delivered));
            }
        } else {
            self.file_rate_sample = Some((now, self.current_delivered));
        }
        if let Some((then, bytes)) = self.total_rate_sample {
            if now.duration_since(then) >= MIN_INTERVAL {
                let dt = now.duration_since(then).as_secs_f64();
                let delta = self.aggregate_bytes.saturating_sub(bytes) as f64;
                let instant = if dt > 0.0 { delta / dt } else { 0.0 };
                self.total_rate_bps = Some(
                    self.total_rate_bps
                        .map_or(instant, |prev| ALPHA.mul_add(instant, (1.0 - ALPHA) * prev)),
                );
                self.total_rate_sample = Some((now, self.aggregate_bytes));
            }
        } else {
            self.total_rate_sample = Some((now, self.aggregate_bytes));
        }
    }

    /// Advances the aggregate-bytes counter without double-counting retries.
    const fn credit_in_flight(&mut self, cumulative: u64) {
        if cumulative <= self.in_flight_high_water {
            return;
        }
        let delta = cumulative - self.in_flight_high_water;
        self.in_flight_high_water = cumulative;
        self.aggregate_bytes = self.aggregate_bytes.saturating_add(delta);
    }

    /// Appends a log line, evicting the oldest when at capacity.
    fn push_log(&mut self, line: String) {
        if self.logs.len() >= MAX_LOG_LINES {
            self.logs.pop_front();
        }
        self.logs.push_back(line);
    }
}

/// Initialises the terminal, runs the download TUI, then restores it.
///
/// # Errors
///
/// Returns an error from terminal setup, render, or the underlying download.
pub async fn run_download_tui(
    db: &mut StateDb,
    config: &Config,
    filter: &DownloadFilter,
) -> Result<()> {
    let mut terminal = ratatui::init();
    execute!(stdout(), EnableMouseCapture).context("failed to enable mouse capture")?;
    let result = run_loop(&mut terminal, db, config, filter).await;
    execute!(stdout(), DisableMouseCapture).ok();
    ratatui::restore();
    result
}

/// Inner event loop split out so the terminal can always be restored on error.
async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    db: &mut StateDb,
    config: &Config,
    filter: &DownloadFilter,
) -> Result<()> {
    let (sink, mut rx) = ProgressSink::channel();
    let mut aggregator = aggregator::spawn().context("failed to spawn spectrum aggregator")?;
    let download = async move { download_pending_mzml(db, config, &sink, filter).await };
    let mut download = pin!(download);

    let mut events = EventStream::new();
    let mut ticker = tokio::time::interval(TICK_RATE);
    let mut app = App::new();
    let mut download_done = false;
    let mut download_result: Result<()> = Ok(());

    loop {
        if app.quit_requested {
            break;
        }
        if download_done && app.download_done {
            break;
        }
        tokio::select! {
            biased;
            _ = ticker.tick() => {
                app.update_rates(Instant::now());
                terminal.draw(|frame| running::render(frame, &mut app))?;
            }
            Some(event) = rx.recv() => {
                app.on_progress_event(event, &aggregator.tx_jobs);
            }
            Some(event) = aggregator.rx_updates.recv() => {
                app.on_aggregate_event(event);
            }
            Some(Ok(event)) = events.next() => {
                app.on_terminal_event(&event);
            }
            result = &mut download, if !download_done => {
                download_done = true;
                download_result = result.context("failed to download pending mzML files");
            }
        }
    }

    app.update_rates(Instant::now());
    terminal.draw(|frame| running::render(frame, &mut app))?;
    download_result
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::{App, RunningTab};
    use crate::db::{DownloadProgressTotals, SourceFileRow};
    use crate::progress_sink::ProgressEvent;

    fn dummy_row(path: &str, size: i64) -> SourceFileRow {
        SourceFileRow {
            filepath: path.to_owned(),
            dataset: "MSV000000001".to_owned(),
            collection: "peak".to_owned(),
            create_time: "2021".to_owned(),
            size_bytes: size,
            spectra_ms1: Some(1.0),
            spectra_ms2: Some(2.0),
            instrument_vendor: "Thermo".to_owned(),
            instrument_model: "Q Exactive".to_owned(),
            extension: ".mzML".to_owned(),
            local_path: "/tmp/x".to_owned(),
            status: "indexed".to_owned(),
            volume: Some("v01".to_owned()),
        }
    }

    /// Confirms the tab cycler moves forwards and wraps around.
    #[test]
    fn tab_cycles_forwards_and_backwards() {
        let mut app = App::new();
        let expected = RunningTab::all();
        assert_eq!(app.tab, expected[0]);
        for tab in &expected[1..] {
            app.advance_tab(1);
            assert_eq!(app.tab, *tab);
        }
        app.advance_tab(1);
        assert_eq!(app.tab, expected[0]);
        app.advance_tab(-1);
        assert_eq!(app.tab, *expected.last().unwrap_or(&expected[0]));
    }

    /// Confirms aggregate bytes only advance by per-file deltas across retries.
    #[test]
    fn aggregate_bytes_dedup_across_retries() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new();
        app.on_progress_event(
            ProgressEvent::DownloadStarted {
                totals: DownloadProgressTotals {
                    planned_files: 1,
                    planned_bytes: 1_000,
                    completed_files: 0,
                    completed_bytes: 0,
                },
                remaining_indexed: 1,
            },
            &tx,
        );
        app.on_progress_event(
            ProgressEvent::FileStarted {
                source: Box::new(dummy_row("a.mzML", 1_000)),
            },
            &tx,
        );
        app.on_progress_event(
            ProgressEvent::AttemptStarted {
                attempt: 1,
                expected_bytes: 1_000,
                resume_offset: 0,
            },
            &tx,
        );
        app.on_progress_event(ProgressEvent::FileBytes { delivered: 400 }, &tx);
        assert_eq!(app.aggregate_bytes, 400);
        // attempt 1 fails halfway, attempt 2 starts from 0
        app.on_progress_event(
            ProgressEvent::AttemptStarted {
                attempt: 2,
                expected_bytes: 1_000,
                resume_offset: 0,
            },
            &tx,
        );
        app.on_progress_event(ProgressEvent::FileBytes { delivered: 200 }, &tx);
        // 200 is below the 400 high water, so aggregate stays put
        assert_eq!(app.aggregate_bytes, 400);
        app.on_progress_event(ProgressEvent::FileBytes { delivered: 1_000 }, &tx);
        assert_eq!(app.aggregate_bytes, 1_000);
        app.on_progress_event(
            ProgressEvent::FileFinished {
                local_path: std::path::PathBuf::from("/tmp/a"),
                bytes: 1_000,
            },
            &tx,
        );
        assert_eq!(app.aggregate_bytes, 1_000);
        assert_eq!(app.downloaded, 1);
    }
}
