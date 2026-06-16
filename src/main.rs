//! Binary entrypoint for the MassIVE mzML to MGF pipeline.

use anyhow::{Context, bail};
use gnps_massive_to_mgf::{
    Config, DownloadProgressTotals, IndexFreshness, ProgressEvent, ProgressReporter, ProgressSink,
    StateDb, convert_downloaded_mzml, download_pending_mzml, ensure_openformats_indexes_fresh,
    ingest_openformats, ingest_volume_manifest,
};
use indicatif::ProgressBar;
use tokio::sync::mpsc::UnboundedReceiver;

/// Runs the requested pipeline subcommand.
///
/// Uses the `current_thread` runtime: the pipeline runs a single async task
/// at a time (one FTPS download in flight, one Zenodo index fetch, etc.) and
/// gains nothing from work-stealing across worker threads. Picking the
/// single-thread runtime drops the worker threadpool, removes the `Send`
/// bound on futures, and lets the download progress accounting use plain
/// `Cell` instead of atomics.
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let progress = ProgressReporter::new();
    let config = Config::from_env().context("failed to read runtime configuration")?;
    let mut db = StateDb::connect(&config.database_url).context("failed to open state database")?;
    db.initialize()
        .context("failed to initialize state database")?;

    match std::env::args().nth(1).as_deref() {
        Some("download") => {
            refresh_indexes_and_ingest(&mut db, &config, &progress).await?;
            let (sink, rx) = ProgressSink::channel();
            let adapter = tokio::spawn(run_indicatif_adapter(progress.clone(), rx));
            let download_result = download_pending_mzml(&mut db, &config, &sink)
                .await
                .context("failed to download pending mzML files");
            drop(sink);
            adapter
                .await
                .context("progress adapter task panicked")?
                .context("progress adapter task failed")?;
            download_result?;
        }
        Some("convert") => {
            let summary = convert_downloaded_mzml(&mut db, &config, &progress)
                .context("failed to convert downloaded mzML files")?;
            progress.println(format!(
                "conversion complete: {} spectra written across {} shards",
                summary.spectra_written, summary.shards_written
            ))?;
        }
        Some(command) => {
            bail!("unknown command {command}; expected one of: download, convert");
        }
        None => {
            bail!("no command provided; expected one of: download, convert");
        }
    }

    Ok(())
}

/// Refreshes the open-format indexes when the Zenodo record has been
/// modified since the local TSVs were last downloaded, then re-ingests
/// `source_files` if the TSVs were refreshed. Always re-ingests the volume
/// manifest, which is idempotent and fast.
async fn refresh_indexes_and_ingest(
    db: &mut StateDb,
    config: &Config,
    progress: &ProgressReporter,
) -> anyhow::Result<()> {
    let freshness = ensure_openformats_indexes_fresh(config, progress)
        .await
        .context("failed to refresh open-format index files")?;
    if matches!(freshness, IndexFreshness::Refreshed) {
        let report = ingest_openformats(db, config, progress).context("failed to ingest index")?;
        progress.println(format!(
            "indexed MassIVE mzML files: {} queued, {} candidates, {} indexed MS/MS spectra",
            report.inserted_mzml, report.candidate_mzml, report.target_ms2_spectra
        ))?;
    }
    let volume_report = ingest_volume_manifest(db, config, progress)
        .context("failed to ingest MassIVE volume manifest")?;
    progress.println(format!(
        "ingested MassIVE volume manifest: {} updated, {} skipped, {} planned download(s) still missing a volume",
        volume_report.rows_updated,
        volume_report.rows_skipped,
        volume_report.missing_volume_after_ingest
    ))?;
    Ok(())
}

/// State held by the `indicatif` adapter that translates [`ProgressEvent`]s
/// back into bar updates and `println` lines.
struct IndicatifAdapter {
    /// Underlying progress reporter.
    progress: ProgressReporter,
    /// Aggregate files-completed bar.
    files_bar: Option<ProgressBar>,
    /// Aggregate bytes-downloaded bar.
    bytes_bar: Option<ProgressBar>,
    /// Per-attempt byte bar (recreated on every [`ProgressEvent::AttemptStarted`]).
    attempt_bar: Option<ProgressBar>,
    /// Cumulative download counter (matches the source-of-truth in `massive.rs`).
    downloaded: u64,
    /// Cumulative failure counter.
    failed: u64,
    /// Files still in `indexed` status; decremented per `FileFinished` / `FileFailed`.
    remaining: u64,
    /// High-water mark of bytes credited to `bytes_bar` for the in-flight file.
    in_flight_bytes: u64,
    /// Source path of the in-flight file (for per-attempt bar messages).
    current_filepath: Option<String>,
}

impl IndicatifAdapter {
    /// Builds a fresh adapter wrapping the supplied reporter.
    const fn new(progress: ProgressReporter) -> Self {
        Self {
            progress,
            files_bar: None,
            bytes_bar: None,
            attempt_bar: None,
            downloaded: 0,
            failed: 0,
            remaining: 0,
            in_flight_bytes: 0,
            current_filepath: None,
        }
    }

    /// Formats the aggregate file-bar message.
    fn files_message(&self) -> String {
        format!(
            "download files | downloaded_or_present={} failed={} remaining={}",
            self.downloaded, self.failed, self.remaining,
        )
    }

    /// Initialises the aggregate bars at loop entry.
    fn on_download_started(
        &mut self,
        totals: DownloadProgressTotals,
        remaining_indexed: u64,
    ) -> anyhow::Result<()> {
        self.downloaded = totals.completed_files;
        self.remaining = remaining_indexed;
        if totals.planned_files > 0 {
            let bar = self
                .progress
                .count_bar(totals.planned_files, self.files_message())?;
            bar.inc(totals.completed_files);
            self.files_bar = Some(bar);
        }
        if totals.planned_bytes > 0 {
            let bar = self
                .progress
                .byte_bar(totals.planned_bytes, "download total bytes")?;
            bar.inc(totals.completed_bytes);
            self.bytes_bar = Some(bar);
        }
        Ok(())
    }

    /// Advances the aggregate bytes bar to a new high-water mark for the
    /// in-flight file (callers pass cumulative, not delta).
    fn credit_in_flight_bytes(&mut self, cumulative: u64) {
        if cumulative <= self.in_flight_bytes {
            return;
        }
        let delta = cumulative - self.in_flight_bytes;
        self.in_flight_bytes = cumulative;
        if let Some(bar) = &self.bytes_bar {
            bar.inc(delta);
        }
    }

    /// Records a finished file (success or failure) on the aggregate file bar.
    fn record_file_completion(&self) {
        if let Some(bar) = &self.files_bar {
            bar.inc(1);
            bar.set_message(self.files_message());
        }
    }

    /// Handles one event.
    fn on_event(&mut self, event: ProgressEvent) -> anyhow::Result<()> {
        match event {
            ProgressEvent::Log(message) => self.progress.println(message)?,
            ProgressEvent::DownloadStarted {
                totals,
                remaining_indexed,
            } => self.on_download_started(totals, remaining_indexed)?,
            ProgressEvent::FileStarted { source } => {
                self.in_flight_bytes = 0;
                self.current_filepath = Some(source.filepath);
            }
            ProgressEvent::FileAlreadyPresent { bytes } => {
                self.credit_in_flight_bytes(bytes);
            }
            ProgressEvent::AttemptStarted {
                attempt,
                expected_bytes,
                resume_offset,
            } => {
                let filepath = self.current_filepath.as_deref().unwrap_or("(unknown)");
                let bar = self
                    .progress
                    .byte_bar(expected_bytes, format!("{filepath} | attempt {attempt}"))?;
                if resume_offset > 0 {
                    bar.inc(resume_offset);
                }
                self.attempt_bar = Some(bar);
            }
            ProgressEvent::FileBytes { delivered } => {
                if let Some(bar) = &self.attempt_bar {
                    let pos = bar.position();
                    if delivered > pos {
                        bar.inc(delivered - pos);
                    }
                }
                self.credit_in_flight_bytes(delivered);
            }
            ProgressEvent::AttemptFinished => {
                if let Some(bar) = self.attempt_bar.take() {
                    bar.finish_and_clear();
                }
            }
            ProgressEvent::FileFinished { bytes, .. } => {
                self.credit_in_flight_bytes(bytes);
                self.downloaded = self.downloaded.saturating_add(1);
                self.remaining = self.remaining.saturating_sub(1);
                self.record_file_completion();
            }
            ProgressEvent::FileFailed { filepath, error } => {
                self.failed = self.failed.saturating_add(1);
                self.remaining = self.remaining.saturating_sub(1);
                self.progress
                    .println(format!("download failed: {filepath} | {error}"))?;
                self.record_file_completion();
            }
            ProgressEvent::DownloadFinished { downloaded, failed } => {
                self.downloaded = downloaded;
                self.failed = failed;
                if let Some(bar) = self.files_bar.take() {
                    bar.finish_with_message(self.files_message());
                }
                if let Some(bar) = self.bytes_bar.take() {
                    bar.finish_with_message("download total bytes");
                }
                self.progress.println(format!(
                    "download summary: downloaded_or_present={downloaded} failed={failed}"
                ))?;
            }
        }
        Ok(())
    }
}

/// Consumes [`ProgressEvent`]s from `rx` and drives the existing `indicatif`
/// progress bars. Returns when the sender side is dropped.
async fn run_indicatif_adapter(
    progress: ProgressReporter,
    mut rx: UnboundedReceiver<ProgressEvent>,
) -> anyhow::Result<()> {
    let mut adapter = IndicatifAdapter::new(progress);
    while let Some(event) = rx.recv().await {
        adapter.on_event(event)?;
    }
    Ok(())
}
