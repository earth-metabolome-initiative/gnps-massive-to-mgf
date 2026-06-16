//! Read-only pipeline status reporting.

use std::path::PathBuf;

use anyhow::Context;

use crate::config::Config;
use crate::db::{ConversionDbSummary, FinalizedShardRow, StateDb};
use crate::index::SourceFileStatus;

/// Counts of source files by stored source status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct SourceStatusCounts {
    /// Candidate mzML files not yet queued for download.
    pub candidate: u64,
    /// mzML files queued for download.
    pub indexed: u64,
    /// Downloaded mzML files.
    pub downloaded: u64,
    /// mzML files with failed downloads.
    pub download_failed: u64,
    /// Source rows marked converted.
    pub converted: u64,
    /// Source rows marked conversion failed.
    pub conversion_failed: u64,
    /// Unsupported open-format source rows.
    pub unsupported: u64,
}

impl SourceStatusCounts {
    /// Loads all source status counts from the state database.
    fn load(db: &mut StateDb) -> anyhow::Result<Self> {
        Ok(Self {
            candidate: db.count_status(SourceFileStatus::Candidate)?,
            indexed: db.count_status(SourceFileStatus::Indexed)?,
            downloaded: db.count_status(SourceFileStatus::Downloaded)?,
            download_failed: db.count_status(SourceFileStatus::DownloadFailed)?,
            converted: db.count_status(SourceFileStatus::Converted)?,
            conversion_failed: db.count_status(SourceFileStatus::ConversionFailed)?,
            unsupported: db.count_status(SourceFileStatus::Unsupported)?,
        })
    }
}

/// Read-only summary of the pipeline state.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PipelineStatus {
    /// Configured top-k peak count.
    pub top_k_peaks: usize,
    /// Configured target number of unique MS/MS spectra.
    pub target_ms2_spectra: u64,
    /// Counts of source files by source status.
    pub source_status_counts: SourceStatusCounts,
    /// Downloaded source files that still need conversion for this top-k value.
    pub downloaded_unconverted: u64,
    /// Aggregate conversion counters for this top-k value.
    pub conversion_summary: ConversionDbSummary,
    /// Number of finalized shard rows for this top-k value.
    pub shard_count: u64,
    /// Total bytes across finalized shard rows for this top-k value.
    pub finalized_shard_bytes: u64,
    /// Largest finalized shard path, when any finalized shard exists.
    pub largest_shard_path: Option<PathBuf>,
    /// Largest finalized shard byte count, or zero when no finalized shard exists.
    pub largest_shard_bytes: u64,
}

impl PipelineStatus {
    /// Formats this status as stable human-readable lines.
    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        let lines = vec![
            "pipeline status".to_owned(),
            format!("top_k_peaks: {}", self.top_k_peaks),
            format!("target_ms2_spectra: {}", self.target_ms2_spectra),
            format!(
                "source files: candidate={} indexed={} downloaded={} download_failed={} converted={} conversion_failed={} unsupported={}",
                self.source_status_counts.candidate,
                self.source_status_counts.indexed,
                self.source_status_counts.downloaded,
                self.source_status_counts.download_failed,
                self.source_status_counts.converted,
                self.source_status_counts.conversion_failed,
                self.source_status_counts.unsupported
            ),
            format!(
                "downloaded_unconverted_for_top_k: {}",
                self.downloaded_unconverted
            ),
            format!(
                "conversion: files_converted={} files_failed={} spectra_written={} duplicates_skipped={} missing_precursor_skipped={} low_peak_skipped={} shards_written={}",
                self.conversion_summary.files_converted,
                self.conversion_summary.files_failed,
                self.conversion_summary.spectra_written,
                self.conversion_summary.duplicates_skipped,
                self.conversion_summary.missing_precursor_skipped,
                self.conversion_summary.low_peak_skipped,
                self.conversion_summary.shards_written
            ),
            format!(
                "finalized shards: count={} bytes={} largest={}",
                self.shard_count,
                self.finalized_shard_bytes,
                self.largest_shard_label()
            ),
        ];
        lines
    }

    /// Formats the largest shard as a compact label.
    fn largest_shard_label(&self) -> String {
        self.largest_shard_path.as_ref().map_or_else(
            || "none".to_owned(),
            |path| format!("{} ({} bytes)", path.display(), self.largest_shard_bytes),
        )
    }
}

/// Collects a read-only pipeline status report.
///
/// # Errors
///
/// Returns an error if SQLite queries or filesystem metadata checks fail.
pub fn collect_pipeline_status(
    db: &mut StateDb,
    config: &Config,
) -> anyhow::Result<PipelineStatus> {
    let source_status_counts = SourceStatusCounts::load(db)?;
    let downloaded_unconverted = db.count_downloaded_unconverted(config.top_k_peaks)?;
    let conversion_summary = db.conversion_summary(config.top_k_peaks)?;
    let finalized_shards = db.finalized_shards(config.top_k_peaks)?;
    let (shard_count, finalized_shard_bytes, largest_shard_path, largest_shard_bytes) =
        summarize_shards(&finalized_shards)?;
    Ok(PipelineStatus {
        top_k_peaks: config.top_k_peaks,
        target_ms2_spectra: config.target_ms2_spectra,
        source_status_counts,
        downloaded_unconverted,
        conversion_summary,
        shard_count,
        finalized_shard_bytes,
        largest_shard_path,
        largest_shard_bytes,
    })
}

/// Builds aggregate shard counters.
fn summarize_shards(
    shards: &[FinalizedShardRow],
) -> anyhow::Result<(u64, u64, Option<PathBuf>, u64)> {
    let mut total_bytes = 0_u64;
    let mut largest_path = None;
    let mut largest_bytes = 0_u64;
    for shard in shards {
        let bytes = u64::try_from(shard.bytes_written).context("shard byte count is negative")?;
        total_bytes = total_bytes.saturating_add(bytes);
        if bytes > largest_bytes {
            largest_bytes = bytes;
            largest_path = Some(PathBuf::from(&shard.shard_path));
        }
    }
    Ok((
        u64::try_from(shards.len())?,
        total_bytes,
        largest_path,
        largest_bytes,
    ))
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use tempfile::tempdir;

    use crate::checksum::sha256_file;
    use crate::config::Config;
    use crate::db::{SourceConversionRecord, SpectrumSeenRecord, StateDb};
    use crate::index::{OpenFormatRecord, SourceFileStatus};

    use super::collect_pipeline_status;

    /// Confirms status reports DB counts, conversion counters, shard bytes, and readiness.
    #[test]
    fn status_reports_counts_and_shard_summary() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        let config = test_config(tempdir.path());
        insert_source(
            &mut db,
            "MSV000000001/path/candidate.mzML",
            SourceFileStatus::Candidate,
        )?;
        insert_source(
            &mut db,
            "MSV000000001/path/indexed.mzML",
            SourceFileStatus::Indexed,
        )?;
        insert_source(
            &mut db,
            "MSV000000001/path/downloaded.mzML",
            SourceFileStatus::Downloaded,
        )?;
        seed_complete_output(&mut db, tempdir.path())?;

        let status = collect_pipeline_status(&mut db, &config)?;

        assert_eq!(status.source_status_counts.candidate, 1);
        assert_eq!(status.source_status_counts.indexed, 1);
        assert_eq!(status.source_status_counts.downloaded, 1);
        assert_eq!(status.downloaded_unconverted, 1);
        assert_eq!(status.conversion_summary.spectra_written, 1);
        assert_eq!(status.shard_count, 1);
        assert_eq!(status.finalized_shard_bytes, 3);
        Ok(())
    }

    /// Inserts one source row with the requested status.
    fn insert_source(
        db: &mut StateDb,
        filepath: &str,
        status: SourceFileStatus,
    ) -> anyhow::Result<()> {
        let record = OpenFormatRecord {
            filepath: filepath.to_owned(),
            dataset: "MSV000000001".to_owned(),
            collection: "peak".to_owned(),
            create_time: "2021-01-01 00:00:00".to_owned(),
            size: 3,
            size_mb: 1,
            spectra_ms1: Some(1.0),
            spectra_ms2: Some(1.0),
            instrument_vendor: "vendor".to_owned(),
            instrument_model: "model".to_owned(),
            extension: ".mzML".to_owned(),
        };
        db.upsert_source_file(&record, Path::new("/tmp/source.mzML"), 0, status, None)
    }

    /// Seeds a tiny complete output corpus.
    fn seed_complete_output(db: &mut StateDb, root: &Path) -> anyhow::Result<()> {
        let shard = root.join("massive_ms2.top-0256.part-000000.mgf.zst");
        std::fs::write(&shard, b"abc")?;
        let sha256 = sha256_file(&shard)?;
        std::fs::write(
            root.join("manifest.csv"),
            format!(
                "shard_index,path,spectra_written,bytes_written,sha256\n0,{},1,3,{sha256}\n",
                shard.display()
            ),
        )?;
        std::fs::write(root.join("conversion_report.json"), b"{}")?;
        std::fs::write(root.join("README.md"), b"readme")?;
        db.upsert_shard(&shard, 0, 256, 1, 3, Some(&sha256))?;
        db.insert_seen_spectra(
            256,
            &shard,
            &[SpectrumSeenRecord {
                splash: "splash10-test".to_owned(),
                pepmass: 100.0,
                source_filepath: "MSV000000001/path/file.mzML".to_owned(),
                spectrum_id: "scan=1".to_owned(),
            }],
        )?;
        db.mark_converted(
            "MSV000000001/path/file.mzML",
            256,
            &SourceConversionRecord {
                ms1_count: 1,
                ms2_count: 1,
                msn_counts_json: "{\"1\":1,\"2\":1}".to_owned(),
                converted_spectra: 1,
                duplicate_spectra: 0,
                missing_precursor_skipped: 0,
                low_peak_skipped: 0,
            },
        )
    }

    /// Builds a minimal test config.
    fn test_config(root: &Path) -> Config {
        Config {
            openformats_record_id: 4_549_746,
            openformats_index_dir: root.join("openformats"),
            massive_volumes_path: root.join("massive_volumes.tsv"),
            database_url: ":memory:".to_owned(),
            mzml_download_dir: root.join("mzml"),
            mgf_output_dir: root.to_path_buf(),
            target_ms2_spectra: 1,
            top_k_peaks: 256,
            mgf_shard_max_spectra: 1_000,
            source_selection_buffer: 1.25,
            source_selection_chunk_ms2: 50_000_000,
            download_retry_attempts: 1,
            download_retry_delay: Duration::from_secs(0),
            ftps_host: "127.0.0.1".to_owned(),
            ftps_port: 21,
            ftps_cert_pin_path: root.join("ftps_cert_pin.txt"),
            ftps_connect_timeout: Duration::from_secs(1),
            ftps_transfer_timeout: None,
        }
    }
}
