//! Channel-based progress events for the download stage.
//!
//! The download path writes [`ProgressEvent`]s to a [`ProgressSink`] without
//! caring whether the consumer is the legacy `indicatif` adapter or the
//! `ratatui` TUI. Each event is the smallest atomic update the receiver needs
//! to advance its bars or redraw its tabs.
//!
//! Send semantics are best-effort: if the receiver has been dropped (the
//! consumer task exited early), the event is silently discarded so the
//! download itself does not fail because the UI went away.

use std::path::PathBuf;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::db::{DownloadProgressTotals, SourceFileRow};

/// One discrete update from the download path to its UI consumer.
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    /// Free-form status line (warnings, summaries, error messages).
    Log(String),
    /// The download loop is about to start.
    DownloadStarted {
        /// Cumulative workload totals at loop entry.
        totals: DownloadProgressTotals,
        /// Number of rows still in `indexed` status (i.e. yet to download).
        remaining_indexed: u64,
    },
    /// A new source file is about to be processed (before any attempt).
    FileStarted {
        /// Row pulled from `source_files` (boxed to keep [`ProgressEvent`] slim).
        source: Box<SourceFileRow>,
    },
    /// The on-disk file already matched the indexed size, so no FTPS transfer
    /// will run. The bytes count is the full expected size.
    FileAlreadyPresent {
        /// Bytes that should be credited to the aggregate counter.
        bytes: u64,
    },
    /// A new transfer attempt for the current file has started.
    AttemptStarted {
        /// 1-based attempt number.
        attempt: usize,
        /// Total bytes expected for this file.
        expected_bytes: u64,
        /// Bytes carried over from a partial `.part` file via FTP `REST`.
        resume_offset: u64,
    },
    /// Cumulative bytes delivered so far in the current attempt.
    FileBytes {
        /// Cumulative bytes (not a delta).
        delivered: u64,
    },
    /// The current attempt finished successfully, so the receiver should hide
    /// the per-attempt bar.
    AttemptFinished,
    /// The current file completed successfully across one or more attempts.
    FileFinished {
        /// Final on-disk path.
        local_path: PathBuf,
        /// Final bytes written.
        bytes: u64,
    },
    /// The current file failed after exhausting retries.
    FileFailed {
        /// Source file path that failed.
        filepath: String,
        /// Combined error message (`format!("{error:#}")`).
        error: String,
    },
    /// The download loop has exited. Carries final tallies.
    DownloadFinished {
        /// Files that ended in `downloaded` status (including already-present).
        downloaded: u64,
        /// Files that ended in `download_failed` status.
        failed: u64,
    },
}

/// Sender half of the progress channel handed to the download path.
#[derive(Debug, Clone)]
pub struct ProgressSink {
    /// Underlying tokio mpsc sender.
    tx: UnboundedSender<ProgressEvent>,
}

impl ProgressSink {
    /// Builds a fresh sink + receiver pair.
    #[must_use]
    pub fn channel() -> (Self, UnboundedReceiver<ProgressEvent>) {
        let (tx, rx) = unbounded_channel();
        (Self { tx }, rx)
    }

    /// Wraps an existing sender (useful when handing sinks to background tasks).
    #[must_use]
    pub const fn from_sender(tx: UnboundedSender<ProgressEvent>) -> Self {
        Self { tx }
    }

    /// Sends one event. If the receiver has been dropped, the event is
    /// silently discarded so the download is not blocked by a missing UI.
    pub fn send(&self, event: ProgressEvent) {
        drop(self.tx.send(event));
    }

    /// Convenience for [`ProgressEvent::Log`].
    pub fn log(&self, message: impl Into<String>) {
        self.send(ProgressEvent::Log(message.into()));
    }
}
