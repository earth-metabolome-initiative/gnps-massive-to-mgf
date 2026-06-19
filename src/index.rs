//! Open-format index ingestion and source-file row models.

use std::collections::HashMap;
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
/// The function makes two passes. The first walks the openformats TSV
/// streaming-deserializing every mzML row into memory. Each row is annotated
/// with a 0/1 priority based on whether the associated dataset's keywords
/// match any entry in `config.ingest_keyword_allowlist`. After the read, the
/// rows are stable-sorted (priority desc, original TSV order asc), so
/// metabolomics / natural-products datasets fill the indexed set before
/// alphabetically-first proteomics rows. Then the function upserts every row
/// in priority order, marking the first N as `indexed` until the buffered MS2
/// target is hit; everything after that becomes `candidate`. Each upsert
/// receives a `selected_order` matching its position in the sorted list, so
/// the downloader's `ORDER BY selected_order` pulls highest-priority files
/// first.
///
/// When the allowlist is empty (default for tests, or user-cleared), every
/// row scores zero and the behaviour collapses to "TSV order only", matching
/// the pre-prioritization semantics.
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
    let mut skipped_unsupported = 0_u64;

    let dataset_keywords = if config.ingest_keyword_allowlist.is_empty() {
        HashMap::new()
    } else {
        load_dataset_keyword_map(&config.massive_volumes_path)?
    };
    let allowlist: Vec<&str> = config
        .ingest_keyword_allowlist
        .iter()
        .map(String::as_str)
        .collect();

    // First pass: read every row + compute priority + buffer.
    let mut buffered: Vec<(OpenFormatRecord, u8)> = Vec::with_capacity(512_000);
    for row in reader.deserialize::<OpenFormatRecord>() {
        let record = row.context("failed to parse MassIVE open-format row")?;
        bar.inc(1);
        if !record.is_mzml() {
            skipped_unsupported = skipped_unsupported.saturating_add(1);
            continue;
        }
        let priority = match dataset_keywords.get(&record.dataset) {
            Some(keywords) if any_token_matches(keywords, &allowlist) => 1,
            _ => 0,
        };
        buffered.push((record, priority));
    }

    // Stable sort: priority desc, TSV order preserved within tier.
    buffered.sort_by_key(|entry| std::cmp::Reverse(entry.1));

    // Second pass: upsert in sorted order, assigning indexed vs candidate by
    // cumulative MS2 against the buffered target.
    let mut inserted_mzml = 0_u64;
    let mut candidate_mzml = 0_u64;
    let mut target_ms2_spectra = 0_u64;
    let selection_target =
        buffered_selection_target(config.target_ms2_spectra, config.source_selection_buffer);

    db.transaction(|db| {
        for (mzml_order, (record, _priority)) in buffered.iter().enumerate() {
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
            let mzml_order = u64::try_from(mzml_order).context("mzml_order does not fit u64")?;
            db.upsert_source_file(record, &local_path, mzml_order, status, None)?;
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

/// Returns whether `keywords` (the manifest's raw lowercase value) contains
/// any allowlist token as a substring.
fn any_token_matches(keywords: &str, allowlist: &[&str]) -> bool {
    allowlist
        .iter()
        .any(|token| !token.is_empty() && keywords.contains(token))
}

/// Loads the dataset_accession -> lowercased keywords map from the volume
/// manifest TSV without touching the database. Used by `ingest_openformats`
/// to score each row before the upsert loop.
fn load_dataset_keyword_map(path: &Path) -> anyhow::Result<HashMap<String, String>> {
    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    let mut map = HashMap::with_capacity(20_000);
    let mut record = csv::ByteRecord::new();
    while reader
        .read_byte_record(&mut record)
        .context("failed to read MassIVE volume-manifest row")?
    {
        let dataset_bytes = record.get(VOLUME_MANIFEST_DATASET_COLUMN).unwrap_or(&[]);
        let keywords_bytes = record.get(VOLUME_MANIFEST_KEYWORDS_COLUMN).unwrap_or(&[]);
        let dataset = std::str::from_utf8(dataset_bytes)
            .context("dataset_accession column is not valid UTF-8")?
            .trim();
        if dataset.is_empty() || dataset == "dataset_accession" {
            continue;
        }
        let keywords = String::from_utf8_lossy(keywords_bytes)
            .trim()
            .to_ascii_lowercase();
        if keywords.is_empty() || keywords == "null" {
            continue;
        }
        map.insert(dataset.to_owned(), keywords);
    }
    Ok(map)
}

/// Column index of `dataset_accession` in the MassIVE volumes manifest.
const VOLUME_MANIFEST_DATASET_COLUMN: usize = 0;
/// Column index of `volume` in the MassIVE volumes manifest.
const VOLUME_MANIFEST_VOLUME_COLUMN: usize = 1;
/// Column index of `species` in the MassIVE volumes manifest.
const VOLUME_MANIFEST_SPECIES_COLUMN: usize = 2;
/// Column index of `keywords` in the MassIVE volumes manifest.
const VOLUME_MANIFEST_KEYWORDS_COLUMN: usize = 3;

/// Summary returned after ingesting the dataset-level volume manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct VolumeIngestReport {
    /// Number of rows read from the TSV (including any skipped).
    pub rows_read: u64,
    /// Number of `source_files` rows whose `volume` actually changed.
    pub rows_updated: u64,
    /// Number of TSV rows skipped because they were blank or otherwise unusable.
    pub rows_skipped: u64,
    /// Number of indexed source files still lacking a volume after ingest.
    pub missing_volume_after_ingest: u64,
}

/// Populates `source_files.volume`, `source_files.keywords`, and
/// `source_files.species` from the dataset-level volumes manifest.
///
/// The manifest is a tab-separated file with the columns
/// `dataset_accession, volume, species, keywords`. Volume drives FTPS-path
/// construction; keywords and species ride along into `source_files` so the
/// downloader can score datasets at ingest time and the converter can stamp
/// them as MGF headers.
///
/// The function is idempotent: rows whose `volume`, `keywords`, and `species`
/// already match the manifest contribute nothing to `rows_updated`. After
/// ingest, [`StateDb::count_missing_volume`] reports how many planned
/// downloads still lack a mapping, surfaced via `missing_volume_after_ingest`.
///
/// Species and keyword bytes may be Latin-1 (e.g. `ó`, `á` in place names);
/// we lossy-decode via [`String::from_utf8_lossy`] so the strict-UTF-8 layer
/// never crashes.
///
/// # Errors
///
/// Returns an error if the TSV cannot be read, if a row fails to parse, or if
/// `SQLite` rejects an update.
pub fn ingest_volume_manifest(
    db: &mut StateDb,
    config: &Config,
    progress: &ProgressReporter,
) -> anyhow::Result<VolumeIngestReport> {
    let path = &config.massive_volumes_path;
    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("failed to open {}", path.display()))?;

    let rows = csv_row_count(path)?;
    let bar = progress.row_bar(rows, "ingesting MassIVE volume manifest")?;
    let mut rows_read = 0_u64;
    let mut rows_updated = 0_u64;
    let mut rows_skipped = 0_u64;

    db.transaction(|db| {
        let mut record = csv::ByteRecord::new();
        while reader
            .read_byte_record(&mut record)
            .context("failed to read MassIVE volume-manifest row")?
        {
            bar.inc(1);
            rows_read = rows_read.saturating_add(1);
            let dataset_bytes = record.get(VOLUME_MANIFEST_DATASET_COLUMN).unwrap_or(&[]);
            let volume_bytes = record.get(VOLUME_MANIFEST_VOLUME_COLUMN).unwrap_or(&[]);
            let species_bytes = record.get(VOLUME_MANIFEST_SPECIES_COLUMN).unwrap_or(&[]);
            let keywords_bytes = record.get(VOLUME_MANIFEST_KEYWORDS_COLUMN).unwrap_or(&[]);
            let dataset = std::str::from_utf8(dataset_bytes)
                .context("dataset_accession column is not valid UTF-8")?
                .trim();
            let volume = std::str::from_utf8(volume_bytes)
                .context("volume column is not valid UTF-8")?
                .trim();
            if dataset.is_empty() || volume.is_empty() || volume == "volume" {
                rows_skipped = rows_skipped.saturating_add(1);
                continue;
            }
            let species = nullable_string(species_bytes);
            let keywords = nullable_string(keywords_bytes).map(|raw| raw.to_ascii_lowercase());
            let volume_updated = db.update_dataset_volume(dataset, volume)?;
            let metadata_updated =
                db.update_dataset_metadata(dataset, species.as_deref(), keywords.as_deref())?;
            rows_updated = rows_updated.saturating_add(volume_updated.max(metadata_updated));
        }
        anyhow::Ok(())
    })?;
    let missing_volume_after_ingest = db.count_missing_volume()?;
    bar.finish_with_message(format!(
        "volume manifest ingested: {rows_updated} row(s) updated, {missing_volume_after_ingest} planned download(s) still missing a volume",
    ));

    Ok(VolumeIngestReport {
        rows_read,
        rows_updated,
        rows_skipped,
        missing_volume_after_ingest,
    })
}

/// Lossy-decodes a manifest byte field and returns `None` for empty or
/// literal `NULL` values; otherwise returns the trimmed string.
fn nullable_string(bytes: &[u8]) -> Option<String> {
    let trimmed = String::from_utf8_lossy(bytes).trim().to_owned();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        None
    } else {
        Some(trimmed)
    }
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

    use super::{OpenFormatRecord, SourceFileStatus, ingest_openformats, ingest_volume_manifest};

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

    /// Confirms metabolomics-tagged rows fill the indexed set ahead of
    /// alphabetically-first proteomics rows, and that keywords + species
    /// columns are populated by the volume manifest.
    #[test]
    fn ingest_prioritizes_allowlist_keyword_datasets() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        write_massive_index(
            tempdir.path(),
            "\
filepath\tdataset\tcollection\tcreate_time\tsize\tsize_mb\tspectra_ms1\tspectra_ms2\tinstrument_vendor\tinstrument_model\textension
MSV000000001/a.mzML\tMSV000000001\tpeak\t2021-01-01 00:00:00\t1\t1\t1\t6\tvendor\tmodel\t.mzML
MSV000000001/b.mzML\tMSV000000001\tpeak\t2021-01-01 00:00:00\t1\t1\t1\t5\tvendor\tmodel\t.mzML
MSV000000002/c.mzML\tMSV000000002\tpeak\t2021-01-01 00:00:00\t1\t1\t1\t9\tvendor\tmodel\t.mzML
MSV000000003/d.mzML\tMSV000000003\tpeak\t2021-01-01 00:00:00\t1\t1\t1\t100\tvendor\tmodel\t.mzML
",
        )?;
        write_volume_manifest(
            tempdir.path(),
            "\
dataset_accession\tvolume\tspecies\tkeywords
MSV000000001\tv01\tHomo sapiens\tproteomics
MSV000000002\tv01\tStreptomyces sp.\tmetabolomics;natural products
MSV000000003\tv01\tNULL\tNULL
",
        )?;
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        let mut config = test_config(tempdir.path(), 10, 1.0);
        config.ingest_keyword_allowlist = vec!["metabolomics".to_owned()];
        let report = ingest_openformats(&mut db, &config, &ProgressReporter::hidden())?;
        // selection_target = target_ms2 * buffer = 10 * 1.0 = 10 spectra.
        // Metabolomics row (9 MS2) lands first, then alphabetical proteomics
        // a.mzML (6 MS2) tips us past the cap. b.mzML and d.mzML stay candidate.
        assert_eq!(report.inserted_mzml, 2);
        assert_eq!(report.candidate_mzml, 2);
        assert_eq!(report.skipped_unsupported, 0);
        assert_eq!(report.target_ms2_spectra, 15);

        let volume_report = ingest_volume_manifest(&mut db, &config, &ProgressReporter::hidden())?;
        assert_eq!(volume_report.missing_volume_after_ingest, 0);

        let metabolomics_row = single_source(&mut db, "MSV000000002/c.mzML")?;
        assert_eq!(metabolomics_row.status, "indexed");
        assert_eq!(
            metabolomics_row.keywords.as_deref(),
            Some("metabolomics;natural products")
        );
        assert_eq!(
            metabolomics_row.species.as_deref(),
            Some("Streptomyces sp.")
        );

        let proteomics_row = single_source(&mut db, "MSV000000001/a.mzML")?;
        assert_eq!(proteomics_row.status, "indexed");
        assert_eq!(proteomics_row.keywords.as_deref(), Some("proteomics"));

        let unmatched = single_source(&mut db, "MSV000000003/d.mzML")?;
        assert_eq!(unmatched.status, "candidate");
        assert!(unmatched.keywords.is_none());
        assert!(unmatched.species.is_none());
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

    /// Confirms the volume manifest populates planned source files by dataset.
    #[test]
    fn ingest_volume_manifest_populates_existing_rows() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        write_massive_index(
            tempdir.path(),
            "\
filepath\tdataset\tcollection\tcreate_time\tsize\tsize_mb\tspectra_ms1\tspectra_ms2\tinstrument_vendor\tinstrument_model\textension
MSV000000001/a.mzML\tMSV000000001\tpeak\t2021-01-01 00:00:00\t1\t1\t1\t6\tvendor\tmodel\t.mzML
MSV000000002/b.mzML\tMSV000000002\tpeak\t2021-01-01 00:00:00\t1\t1\t1\t5\tvendor\tmodel\t.mzML
",
        )?;
        write_volume_manifest(
            tempdir.path(),
            "\
dataset_accession\tvolume\tspecies\tkeywords
MSV000000001\tv07\tHomo sapiens\tMetabolomics
MSV000000002\tx01\tNULL\tNULL
MSV000999999\tv01\tNULL\tNULL
",
        )?;
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        let config = test_config(tempdir.path(), 100, 1.0);
        ingest_openformats(&mut db, &config, &ProgressReporter::hidden())?;
        let report = ingest_volume_manifest(&mut db, &config, &ProgressReporter::hidden())?;
        assert_eq!(report.rows_read, 3);
        assert_eq!(report.rows_updated, 2);
        assert_eq!(report.rows_skipped, 0);
        assert_eq!(report.missing_volume_after_ingest, 0);
        Ok(())
    }

    /// Confirms repeated ingestion of the same manifest reports zero updates.
    #[test]
    fn ingest_volume_manifest_is_idempotent() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        write_massive_index(
            tempdir.path(),
            "\
filepath\tdataset\tcollection\tcreate_time\tsize\tsize_mb\tspectra_ms1\tspectra_ms2\tinstrument_vendor\tinstrument_model\textension
MSV000000001/a.mzML\tMSV000000001\tpeak\t2021-01-01 00:00:00\t1\t1\t1\t6\tvendor\tmodel\t.mzML
",
        )?;
        write_volume_manifest(
            tempdir.path(),
            "\
dataset_accession\tvolume\tspecies\tkeywords
MSV000000001\tv01\tNULL\tNULL
",
        )?;
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        let config = test_config(tempdir.path(), 100, 1.0);
        ingest_openformats(&mut db, &config, &ProgressReporter::hidden())?;
        let first = ingest_volume_manifest(&mut db, &config, &ProgressReporter::hidden())?;
        assert_eq!(first.rows_updated, 1);
        let second = ingest_volume_manifest(&mut db, &config, &ProgressReporter::hidden())?;
        assert_eq!(second.rows_updated, 0);
        assert_eq!(second.missing_volume_after_ingest, 0);
        Ok(())
    }

    /// Writes a test MassIVE open-format TSV under the configured cache path.
    fn write_massive_index(root: &Path, contents: &str) -> anyhow::Result<()> {
        let index_dir = root.join("openformats");
        fs::create_dir_all(&index_dir)?;
        fs::write(index_dir.join("massive_public_openformats.tsv"), contents)?;
        Ok(())
    }

    /// Writes a test MassIVE volume manifest at the configured path.
    fn write_volume_manifest(root: &Path, contents: &str) -> anyhow::Result<()> {
        fs::write(root.join("massive_volumes.tsv"), contents)?;
        Ok(())
    }

    /// Looks up a single `source_files` row by filepath, panicking if absent.
    fn single_source(db: &mut StateDb, filepath: &str) -> anyhow::Result<crate::db::SourceFileRow> {
        db.get_source_file(filepath)?
            .with_context(|| format!("expected source_files row {filepath} after ingest"))
    }

    /// Builds a minimal test configuration rooted in a temporary directory.
    fn test_config(root: &Path, target_ms2_spectra: u64, source_selection_buffer: f64) -> Config {
        Config {
            openformats_record_id: 4_549_746,
            openformats_index_dir: root.join("openformats"),
            massive_volumes_path: root.join("massive_volumes.tsv"),
            database_url: ":memory:".to_owned(),
            mzml_download_dir: root.join("mzml"),
            mgf_output_dir: root.join("mgf"),
            target_ms2_spectra,
            top_k_peaks: 256,
            mgf_shard_max_spectra: 1_000,
            source_selection_buffer,
            source_selection_chunk_ms2: 50_000_000,
            download_retry_attempts: 1,
            download_retry_delay: Duration::from_secs(0),
            ftps_host: "127.0.0.1".to_owned(),
            ftps_port: 21,
            ftps_cert_pin_path: root.join("ftps_cert_pin.txt"),
            ftps_connect_timeout: Duration::from_secs(1),
            ftps_transfer_timeout: None,
            ftps_chunk_read_timeout: None,
            download_failure_cooldown: Duration::from_secs(0),
            ingest_keyword_allowlist: Vec::new(),
        }
    }
}
