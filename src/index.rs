//! Open-format index ingestion and source-file row models.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, bail};
use csv::ReaderBuilder;
use serde::Deserialize;

use crate::config::Config;
use crate::db::StateDb;
use crate::progress::ProgressReporter;

/// Status labels stored for source files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceFileStatus {
    /// Available as a later source candidate, but not queued for download yet.
    Candidate,
    /// Indexed but not downloaded yet.
    Indexed,
    /// Download succeeded and the local file size matches the index.
    Downloaded,
    /// Download or validation failed.
    DownloadFailed,
    /// Conversion succeeded.
    Converted,
    /// Conversion failed.
    ConversionFailed,
    /// Source format is not supported by this crate yet.
    Unsupported,
}

impl SourceFileStatus {
    /// Returns the stable database label for this status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Indexed => "indexed",
            Self::Downloaded => "downloaded",
            Self::DownloadFailed => "download_failed",
            Self::Converted => "converted",
            Self::ConversionFailed => "conversion_failed",
            Self::Unsupported => "unsupported",
        }
    }
}

/// One row from the GNPS/MassIVE open-format TSV files.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct OpenFormatRecord {
    /// Repository-relative file path.
    pub filepath: String,
    /// MassIVE dataset accession.
    pub dataset: String,
    /// MassIVE collection label.
    pub collection: String,
    /// File creation timestamp as reported by the index.
    pub create_time: String,
    /// Source file size in bytes.
    pub size: u64,
    /// Source file size in megabytes as reported by the index.
    pub size_mb: u64,
    /// Number of MS1 spectra reported by the index.
    #[serde(deserialize_with = "deserialize_optional_f64")]
    pub spectra_ms1: Option<f64>,
    /// Number of MS2 spectra reported by the index.
    #[serde(deserialize_with = "deserialize_optional_f64")]
    pub spectra_ms2: Option<f64>,
    /// Instrument vendor reported by the index.
    pub instrument_vendor: String,
    /// Instrument model reported by the index.
    pub instrument_model: String,
    /// File extension reported by the index.
    pub extension: String,
}

impl OpenFormatRecord {
    /// Returns whether this row points to an mzML file.
    #[must_use]
    pub fn is_mzml(&self) -> bool {
        self.extension.eq_ignore_ascii_case(".mzml")
    }

    /// Returns the indexed MS/MS spectrum count rounded down.
    #[must_use]
    pub fn ms2_count_floor(&self) -> u64 {
        self.spectra_ms2
            .filter(|value| value.is_finite() && *value > 0.0)
            .map(f64::floor)
            .and_then(f64_to_u64)
            .unwrap_or_default()
    }

    /// Builds the local raw download path under `download_root`.
    ///
    /// # Errors
    ///
    /// Returns an error if the indexed path is absolute or contains `..`.
    pub fn local_path(&self, download_root: &Path) -> anyhow::Result<PathBuf> {
        let relative = Path::new(&self.filepath);
        for component in relative.components() {
            match component {
                Component::Normal(_) | Component::CurDir => {}
                Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                    bail!("unsafe indexed filepath {}", self.filepath);
                }
            }
        }
        Ok(download_root.join(relative))
    }
}

/// Summary returned after index ingestion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct IngestReport {
    /// Number of mzML rows inserted or refreshed.
    pub inserted_mzml: u64,
    /// Number of mzML rows retained as later candidates.
    pub candidate_mzml: u64,
    /// Number of non-mzML rows skipped as unsupported.
    pub skipped_unsupported: u64,
    /// Cumulative indexed MS/MS spectra selected toward the target.
    pub target_ms2_spectra: u64,
}

/// Downloads are aimed at the deduplicated MassIVE superset only.
///
/// # Errors
///
/// Returns an error if the cached MassIVE TSV cannot be read or if the state
/// database update fails.
pub fn ingest_openformats(
    db: &mut StateDb,
    config: &Config,
    progress: &ProgressReporter,
) -> anyhow::Result<IngestReport> {
    let path = config.massive_index_path();
    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .from_path(&path)
        .with_context(|| format!("failed to open {}", path.display()))?;

    let rows = csv_row_count(&path)?;
    let bar = progress.row_bar(rows, "ingesting deduped MassIVE mzML index")?;
    let mut inserted_mzml = 0_u64;
    let mut candidate_mzml = 0_u64;
    let mut skipped_unsupported = 0_u64;
    let mut target_ms2_spectra = 0_u64;
    let selection_target =
        buffered_selection_target(config.target_ms2_spectra, config.source_selection_buffer);
    let mut mzml_order = 0_u64;

    db.transaction(|db| {
        for row in reader.deserialize::<OpenFormatRecord>() {
            let record = row.context("failed to parse MassIVE open-format row")?;
            bar.inc(1);
            if !record.is_mzml() {
                skipped_unsupported = skipped_unsupported.saturating_add(1);
                continue;
            }
            let local_path = record.local_path(&config.mzml_download_dir)?;
            let expected_ms2 = record.ms2_count_floor();
            let status = if target_ms2_spectra < selection_target {
                target_ms2_spectra = target_ms2_spectra.saturating_add(expected_ms2);
                inserted_mzml = inserted_mzml.saturating_add(1);
                SourceFileStatus::Indexed
            } else {
                candidate_mzml = candidate_mzml.saturating_add(1);
                SourceFileStatus::Candidate
            };
            db.upsert_source_file(&record, &local_path, mzml_order, status)?;
            mzml_order = mzml_order.saturating_add(1);
        }
        anyhow::Ok(())
    })?;
    bar.finish_with_message(format!(
        "indexed {inserted_mzml} mzML files for {target_ms2_spectra} target MS/MS spectra"
    ));

    Ok(IngestReport {
        inserted_mzml,
        candidate_mzml,
        skipped_unsupported,
        target_ms2_spectra,
    })
}

/// Returns the indexed MS/MS source-selection target after applying the buffer.
#[allow(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "the user-facing buffer multiplier is configured as f64"
)]
fn buffered_selection_target(target: u64, buffer: f64) -> u64 {
    let buffered = (target as f64 * buffer).ceil();
    if !(buffered.is_finite() && buffered >= 0.0) {
        return u64::MAX;
    }
    let text = format!("{buffered:.0}");
    text.parse::<u64>().unwrap_or(u64::MAX)
}

/// Counts data rows in a TSV file.
fn csv_row_count(path: &Path) -> anyhow::Result<u64> {
    let file = File::open(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut rows = 0_u64;
    let mut buffer = Vec::new();
    while reader
        .read_until(b'\n', &mut buffer)
        .with_context(|| format!("failed to read {}", path.display()))?
        != 0
    {
        rows = rows.saturating_add(1);
        buffer.clear();
    }
    Ok(rows.saturating_sub(1))
}

/// Deserializes an optional floating-point TSV field.
fn deserialize_optional_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    match value.as_deref().map(str::trim) {
        Some("") | None => Ok(None),
        Some(value) => value
            .parse::<f64>()
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

/// Converts a finite non-negative `f64` to `u64` after bounds checking.
fn f64_to_u64(value: f64) -> Option<u64> {
    if !(value.is_finite() && value >= 0.0) {
        return None;
    }
    let text = format!("{value:.0}");
    text.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::time::Duration;

    use anyhow::Context;
    use tempfile::tempdir;

    use crate::config::Config;
    use crate::db::StateDb;
    use crate::progress::ProgressReporter;

    use super::{OpenFormatRecord, SourceFileStatus, ingest_openformats};

    /// Confirms MassIVE download paths are rooted under the configured directory.
    #[test]
    fn local_path_rejects_parent_components() {
        let row = OpenFormatRecord {
            filepath: "../bad.mzML".to_owned(),
            dataset: "MSV000000001".to_owned(),
            collection: "peak".to_owned(),
            create_time: "2021-01-01 00:00:00".to_owned(),
            size: 1,
            size_mb: 1,
            spectra_ms1: Some(1.0),
            spectra_ms2: Some(2.0),
            instrument_vendor: "vendor".to_owned(),
            instrument_model: "model".to_owned(),
            extension: ".mzML".to_owned(),
        };
        assert!(row.local_path(Path::new("/mnt/bfd/mzml")).is_err());
    }

    /// Confirms ingestion selects only the buffered mzML target and keeps later mzML candidates.
    #[test]
    fn ingest_selects_buffered_mzml_target_and_candidates_rest() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        write_massive_index(
            tempdir.path(),
            "\
filepath\tdataset\tcollection\tcreate_time\tsize\tsize_mb\tspectra_ms1\tspectra_ms2\tinstrument_vendor\tinstrument_model\textension
MSV000000001/a.mzML\tMSV000000001\tpeak\t2021-01-01 00:00:00\t1\t1\t1\t6\tvendor\tmodel\t.mzML
MSV000000001/b.mzML\tMSV000000001\tpeak\t2021-01-01 00:00:00\t1\t1\t1\t5\tvendor\tmodel\t.mzML
MSV000000001/c.mzML\tMSV000000001\tpeak\t2021-01-01 00:00:00\t1\t1\t1\t9\tvendor\tmodel\t.mzML
MSV000000001/d.mgf\tMSV000000001\tpeak\t2021-01-01 00:00:00\t1\t1\t1\t100\tvendor\tmodel\t.mgf
",
        )?;
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        let config = test_config(tempdir.path(), 10, 1.0);
        let report = ingest_openformats(&mut db, &config, &ProgressReporter::hidden())?;
        assert_eq!(report.inserted_mzml, 2);
        assert_eq!(report.candidate_mzml, 1);
        assert_eq!(report.skipped_unsupported, 1);
        assert_eq!(report.target_ms2_spectra, 11);
        assert_eq!(db.count_status(SourceFileStatus::Indexed)?, 2);
        assert_eq!(db.count_status(SourceFileStatus::Candidate)?, 1);
        Ok(())
    }

    /// Confirms unsafe indexed paths roll back the ingestion transaction.
    #[test]
    fn ingest_rejects_unsafe_paths_and_rolls_back() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        write_massive_index(
            tempdir.path(),
            "\
filepath\tdataset\tcollection\tcreate_time\tsize\tsize_mb\tspectra_ms1\tspectra_ms2\tinstrument_vendor\tinstrument_model\textension
MSV000000001/a.mzML\tMSV000000001\tpeak\t2021-01-01 00:00:00\t1\t1\t1\t6\tvendor\tmodel\t.mzML
../bad.mzML\tMSV000000001\tpeak\t2021-01-01 00:00:00\t1\t1\t1\t5\tvendor\tmodel\t.mzML
",
        )?;
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        let config = test_config(tempdir.path(), 10, 1.0);
        let error = ingest_openformats(&mut db, &config, &ProgressReporter::hidden())
            .err()
            .context("expected unsafe path to be rejected")?;
        assert!(error.to_string().contains("unsafe indexed filepath"));
        assert_eq!(db.count_status(SourceFileStatus::Indexed)?, 0);
        Ok(())
    }

    /// Writes a test MassIVE open-format TSV under the configured cache path.
    fn write_massive_index(root: &Path, contents: &str) -> anyhow::Result<()> {
        let index_dir = root.join("openformats");
        fs::create_dir_all(&index_dir)?;
        fs::write(index_dir.join("massive_public_openformats.tsv"), contents)?;
        Ok(())
    }

    /// Builds a minimal test configuration rooted in a temporary directory.
    fn test_config(root: &Path, target_ms2_spectra: u64, source_selection_buffer: f64) -> Config {
        Config {
            openformats_record_id: 4_549_746,
            openformats_index_dir: root.join("openformats"),
            database_url: ":memory:".to_owned(),
            mzml_download_dir: root.join("mzml"),
            mgf_output_dir: root.join("mgf"),
            target_ms2_spectra,
            top_k_peaks: 256,
            mgf_shard_max_spectra: 1_000,
            source_selection_buffer,
            source_selection_chunk_ms2: 50_000_000,
            download_workers: 1,
            download_retry_attempts: 1,
            download_retry_delay: Duration::from_secs(0),
            http_request_timeout: None,
            http_connect_timeout: Duration::from_secs(1),
            publish_to_zenodo: false,
            zenodo_deposition_id: None,
            zenodo_max_file_bytes: 50_000_000_000,
        }
    }
}
