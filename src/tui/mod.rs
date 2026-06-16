//! `ratatui` terminal UI for the `download` subcommand.
//!
//! The TUI runs on the existing `current_thread` tokio runtime alongside the
//! single in-flight FTPS download. `tokio::select!` interleaves four sources:
//! a fixed-cadence render tick, the [`ProgressEvent`] channel from the
//! download path, the `crossterm` event stream, and the download future
//! itself. Pressing `q`, `Esc`, or `Ctrl+C` exits the loop. If the download
//! is still in flight, dropping its pinned future cancels the FTPS transfer
//! (the partial `.part` file persists for FTP `REST` resume on the next run).

use std::collections::VecDeque;
use std::io::stdout;
use std::pin::pin;
use std::time::Duration;

use anyhow::{Context, Result};
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers,
};
use crossterm::execute;
use futures_util::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use std::io::Stdout;

use crate::config::Config;
use crate::db::{DownloadFilter, DownloadProgressTotals, SourceFileRow, StateDb};
use crate::massive::download_pending_mzml;
use crate::progress_sink::{ProgressEvent, ProgressSink};

pub mod running;

/// Maximum log lines retained in the running screen's `Log` tab.
const MAX_LOG_LINES: usize = 256;
/// Maximum file rows retained for the `In flight` and `Completed` tabs.
const MAX_FILE_ROWS: usize = 2048;
/// Render tick cadence. ~12 fps is plenty for FTPS throughput.
const TICK_RATE: Duration = Duration::from_millis(80);

/// Outcome of one file in the [`App`]'s file history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileOutcome {
    /// In flight or queued.
    Pending,
    /// Finished successfully.
    Downloaded,
    /// Failed after exhausting retries.
    Failed,
}

/// One row of the file history shown in the right-pane tabs.
#[derive(Debug, Clone)]
pub struct FileEntry {
    /// Source-files row pulled from the database.
    pub source: SourceFileRow,
    /// Current outcome.
    pub outcome: FileOutcome,
    /// Last error message, if any.
    pub error: Option<String>,
    /// Bytes delivered (final on success, partial on pending/failed).
    pub bytes: u64,
}

/// Right-pane tab the user is currently viewing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RunningTab {
    /// Files currently in flight or queued.
    #[default]
    InFlight,
    /// Successfully downloaded files.
    Completed,
    /// Failed files with their error messages.
    Failed,
    /// Log scrollback.
    Log,
}

impl RunningTab {
    /// Ordered list of tabs cycled by `Tab` / `Shift+Tab`.
    #[must_use]
    pub const fn all() -> [Self; 4] {
        [Self::InFlight, Self::Completed, Self::Failed, Self::Log]
    }

    /// Title shown in the [`ratatui::widgets::Tabs`] header.
    #[must_use]
    pub const fn title(self) -> &'static str {
        match self {
            Self::InFlight => "In flight",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
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
    /// File-history ring buffer.
    pub files: VecDeque<FileEntry>,
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
}

impl App {
    /// Builds a fresh application with the default tab selection.
    fn new() -> Self {
        Self {
            tab: RunningTab::InFlight,
            ..Self::default()
        }
    }

    /// Handles one terminal event.
    pub fn on_terminal_event(&mut self, event: &Event) {
        if let Event::Key(KeyEvent {
            code,
            modifiers,
            kind,
            ..
        }) = event
        {
            if *kind != KeyEventKind::Press {
                return;
            }
            self.on_key(*code, *modifiers);
        }
    }

    /// Dispatches a key press.
    fn on_key(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        match code {
            KeyCode::Char('q') | KeyCode::Esc => self.quit_requested = true,
            KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => {
                self.quit_requested = true;
            }
            KeyCode::Tab | KeyCode::Right => self.advance_tab(1),
            KeyCode::BackTab | KeyCode::Left => self.advance_tab(-1),
            _ => {}
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

    /// Handles one progress event from the download path.
    pub fn on_progress_event(&mut self, event: ProgressEvent) {
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
                self.push_file(FileEntry {
                    source: row.clone(),
                    outcome: FileOutcome::Pending,
                    error: None,
                    bytes: 0,
                });
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
            }
            ProgressEvent::FileBytes { delivered } => {
                if delivered > self.current_delivered {
                    self.current_delivered = delivered;
                }
                self.credit_in_flight(delivered);
            }
            ProgressEvent::AttemptFinished => {}
            ProgressEvent::FileFinished { bytes, .. } => {
                self.current_delivered = bytes;
                self.credit_in_flight(bytes);
                self.downloaded = self.downloaded.saturating_add(1);
                self.remaining = self.remaining.saturating_sub(1);
                if let Some(last) = self.files.back_mut() {
                    last.outcome = FileOutcome::Downloaded;
                    last.bytes = bytes;
                }
                self.current = None;
            }
            ProgressEvent::FileFailed { filepath, error } => {
                self.failed = self.failed.saturating_add(1);
                self.remaining = self.remaining.saturating_sub(1);
                if let Some(last) = self.files.back_mut() {
                    last.outcome = FileOutcome::Failed;
                    last.error = Some(error.clone());
                }
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

    /// Appends a file row, evicting the oldest when at capacity.
    fn push_file(&mut self, entry: FileEntry) {
        if self.files.len() >= MAX_FILE_ROWS {
            self.files.pop_front();
        }
        self.files.push_back(entry);
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
                terminal.draw(|frame| running::render(frame, &app))?;
            }
            Some(event) = rx.recv() => {
                app.on_progress_event(event);
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

    terminal.draw(|frame| running::render(frame, &app))?;
    download_result
}

#[cfg(test)]
mod tests {
    use super::{App, FileOutcome, RunningTab};
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
        assert_eq!(app.tab, RunningTab::InFlight);
        app.advance_tab(1);
        assert_eq!(app.tab, RunningTab::Completed);
        app.advance_tab(1);
        assert_eq!(app.tab, RunningTab::Failed);
        app.advance_tab(1);
        assert_eq!(app.tab, RunningTab::Log);
        app.advance_tab(1);
        assert_eq!(app.tab, RunningTab::InFlight);
        app.advance_tab(-1);
        assert_eq!(app.tab, RunningTab::Log);
    }

    /// Confirms aggregate bytes only advance by per-file deltas across retries.
    #[test]
    fn aggregate_bytes_dedup_across_retries() {
        let mut app = App::new();
        app.on_progress_event(ProgressEvent::DownloadStarted {
            totals: DownloadProgressTotals {
                planned_files: 1,
                planned_bytes: 1_000,
                completed_files: 0,
                completed_bytes: 0,
            },
            remaining_indexed: 1,
        });
        app.on_progress_event(ProgressEvent::FileStarted {
            source: Box::new(dummy_row("a.mzML", 1_000)),
        });
        app.on_progress_event(ProgressEvent::AttemptStarted {
            attempt: 1,
            expected_bytes: 1_000,
            resume_offset: 0,
        });
        app.on_progress_event(ProgressEvent::FileBytes { delivered: 400 });
        assert_eq!(app.aggregate_bytes, 400);
        // attempt 1 fails halfway, attempt 2 starts from 0
        app.on_progress_event(ProgressEvent::AttemptStarted {
            attempt: 2,
            expected_bytes: 1_000,
            resume_offset: 0,
        });
        app.on_progress_event(ProgressEvent::FileBytes { delivered: 200 });
        // 200 is below the 400 high water, so aggregate stays put
        assert_eq!(app.aggregate_bytes, 400);
        app.on_progress_event(ProgressEvent::FileBytes { delivered: 1_000 });
        assert_eq!(app.aggregate_bytes, 1_000);
        app.on_progress_event(ProgressEvent::FileFinished {
            local_path: std::path::PathBuf::from("/tmp/a"),
            bytes: 1_000,
        });
        assert_eq!(app.aggregate_bytes, 1_000);
        assert_eq!(app.downloaded, 1);
        assert_eq!(
            app.files.back().map(|entry| entry.outcome),
            Some(FileOutcome::Downloaded)
        );
    }
}
