//! SQLite state store backed by Diesel.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Double, Nullable, Text};
use diesel::sqlite::SqliteConnection;

use crate::index::{OpenFormatRecord, SourceFileStatus};

/// Persistent state database.
pub struct StateDb {
    /// SQLite connection.
    connection: SqliteConnection,
}

/// Source file row used by download and conversion stages.
#[derive(Debug, Clone, QueryableByName, PartialEq)]
pub struct SourceFileRow {
    /// Indexed MassIVE path.
    #[diesel(sql_type = Text)]
    pub filepath: String,
    /// Dataset accession.
    #[diesel(sql_type = Text)]
    pub dataset: String,
    /// Collection name.
    #[diesel(sql_type = Text)]
    pub collection: String,
    /// Source creation time as text.
    #[diesel(sql_type = Text)]
    pub create_time: String,
    /// Source size in bytes.
    #[diesel(sql_type = BigInt)]
    pub size_bytes: i64,
    /// Indexed MS1 spectrum count.
    #[diesel(sql_type = Nullable<Double>)]
    pub spectra_ms1: Option<f64>,
    /// Indexed MS2 spectrum count.
    #[diesel(sql_type = Nullable<Double>)]
    pub spectra_ms2: Option<f64>,
    /// Instrument vendor.
    #[diesel(sql_type = Text)]
    pub instrument_vendor: String,
    /// Instrument model.
    #[diesel(sql_type = Text)]
    pub instrument_model: String,
    /// Extension.
    #[diesel(sql_type = Text)]
    pub extension: String,
    /// Local path for the downloaded file.
    #[diesel(sql_type = Text)]
    pub local_path: String,
    /// Current status.
    #[diesel(sql_type = Text)]
    pub status: String,
}

impl SourceFileRow {
    /// Returns the local file path.
    #[must_use]
    pub fn local_path_buf(&self) -> PathBuf {
        PathBuf::from(&self.local_path)
    }
}

/// Deduplication row belonging to a finalized shard.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SpectrumSeenRecord {
    /// Spectrum SPLASH.
    pub splash: String,
    /// Precursor m/z.
    pub pepmass: f64,
    /// Source mzML filepath.
    pub source_filepath: String,
    /// Source spectrum identifier.
    pub spectrum_id: String,
}

/// One finalized shard row stored in SQLite.
#[derive(Debug, Clone, QueryableByName, PartialEq, Eq)]
#[non_exhaustive]
pub struct FinalizedShardRow {
    /// Shard path.
    #[diesel(sql_type = Text)]
    pub shard_path: String,
    /// Zero-based shard index.
    #[diesel(sql_type = BigInt)]
    pub shard_index: i64,
    /// Configured top-k peak count.
    #[diesel(sql_type = BigInt)]
    pub top_k_peaks: i64,
    /// Number of spectra in the shard.
    #[diesel(sql_type = BigInt)]
    pub spectra_written: i64,
    /// Compressed bytes written.
    #[diesel(sql_type = BigInt)]
    pub bytes_written: i64,
    /// SHA256 checksum.
    #[diesel(sql_type = Nullable<Text>)]
    pub sha256: Option<String>,
}

/// Cumulative download workload totals across the planned source files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct DownloadProgressTotals {
    /// Files whose download is planned (downloaded, indexed, or failed).
    pub planned_files: u64,
    /// Total bytes summed across the planned files.
    pub planned_bytes: u64,
    /// Files already downloaded successfully.
    pub completed_files: u64,
    /// Bytes already downloaded successfully.
    pub completed_bytes: u64,
}

/// Aggregate conversion counters for one top-k setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConversionDbSummary {
    /// Converted source files.
    pub files_converted: u64,
    /// Failed source files.
    pub files_failed: u64,
    /// Unique finalized spectra.
    pub spectra_written: u64,
    /// Duplicate spectra skipped.
    pub duplicates_skipped: u64,
    /// Missing-precursor spectra skipped.
    pub missing_precursor_skipped: u64,
    /// Low-peak spectra skipped.
    pub low_peak_skipped: u64,
    /// Finalized shard count.
    pub shards_written: u64,
}

/// Per-source conversion counters persisted for one top-k setting.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SourceConversionRecord {
    /// Number of MS1 spectra observed.
    pub ms1_count: u64,
    /// Number of MS2 spectra observed.
    pub ms2_count: u64,
    /// Observed MSn counters as JSON.
    pub msn_counts_json: String,
    /// Unique spectra written during this conversion attempt.
    pub converted_spectra: u64,
    /// Duplicate spectra skipped during this conversion attempt.
    pub duplicate_spectra: u64,
    /// Missing-precursor spectra skipped.
    pub missing_precursor_skipped: u64,
    /// Low-peak spectra skipped.
    pub low_peak_skipped: u64,
}

impl StateDb {
    /// Opens a SQLite state database.
    ///
    /// # Errors
    ///
    /// Returns an error if the parent directory cannot be created or SQLite
    /// cannot open the database.
    pub fn connect(database_url: &str) -> anyhow::Result<Self> {
        create_sqlite_parent(database_url)?;
        let connection = SqliteConnection::establish(database_url)
            .with_context(|| format!("failed to open SQLite database {database_url}"))?;
        Ok(Self { connection })
    }

    /// Creates all pipeline state tables and indexes.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the schema.
    pub fn initialize(&mut self) -> anyhow::Result<()> {
        self.connection
            .batch_execute(
                "\
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 60000;
CREATE TABLE IF NOT EXISTS source_files (
    filepath TEXT PRIMARY KEY NOT NULL,
    dataset TEXT NOT NULL,
    collection TEXT NOT NULL,
    create_time TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    size_mb INTEGER NOT NULL,
    spectra_ms1 REAL,
    spectra_ms2 REAL,
    instrument_vendor TEXT NOT NULL,
    instrument_model TEXT NOT NULL,
    extension TEXT NOT NULL,
    local_path TEXT NOT NULL,
    selected_order INTEGER NOT NULL,
    status TEXT NOT NULL,
    error TEXT,
    downloaded_bytes INTEGER,
    ms1_count INTEGER NOT NULL DEFAULT 0,
    ms2_count INTEGER NOT NULL DEFAULT 0,
    msn_counts_json TEXT NOT NULL DEFAULT '{}',
    converted_spectra INTEGER NOT NULL DEFAULT 0,
    converted_top_k INTEGER,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS source_files_status_order_idx
    ON source_files(status, selected_order);
CREATE TABLE IF NOT EXISTS spectra_seen (
    top_k_peaks INTEGER NOT NULL,
    splash TEXT NOT NULL,
    pepmass_key TEXT NOT NULL,
    pepmass REAL NOT NULL,
    source_filepath TEXT NOT NULL,
    spectrum_id TEXT NOT NULL,
    shard_path TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (top_k_peaks, splash, pepmass_key)
);
CREATE INDEX IF NOT EXISTS spectra_seen_source_idx ON spectra_seen(source_filepath);
CREATE TABLE IF NOT EXISTS mgf_shards (
    shard_path TEXT PRIMARY KEY NOT NULL,
    shard_index INTEGER NOT NULL,
    top_k_peaks INTEGER NOT NULL,
    spectra_written INTEGER NOT NULL,
    bytes_written INTEGER NOT NULL,
    sha256 TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS mgf_shards_top_k_idx
    ON mgf_shards(top_k_peaks, shard_index);
CREATE TABLE IF NOT EXISTS source_file_conversions (
    filepath TEXT NOT NULL,
    top_k_peaks INTEGER NOT NULL,
    status TEXT NOT NULL,
    ms1_count INTEGER NOT NULL DEFAULT 0,
    ms2_count INTEGER NOT NULL DEFAULT 0,
    msn_counts_json TEXT NOT NULL DEFAULT '{}',
    converted_spectra INTEGER NOT NULL DEFAULT 0,
    duplicate_spectra INTEGER NOT NULL DEFAULT 0,
    missing_precursor_skipped INTEGER NOT NULL DEFAULT 0,
    low_peak_skipped INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (filepath, top_k_peaks)
);
CREATE INDEX IF NOT EXISTS source_file_conversions_status_idx
    ON source_file_conversions(top_k_peaks, status);
",
            )
            .context("failed to initialize state schema")
    }

    /// Runs a closure inside a simple immediate transaction.
    ///
    /// # Errors
    ///
    /// Returns the closure error or a transaction control error.
    pub fn transaction<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        self.connection
            .batch_execute("BEGIN IMMEDIATE TRANSACTION")
            .context("failed to begin transaction")?;
        match f(self) {
            Ok(value) => {
                self.connection
                    .batch_execute("COMMIT")
                    .context("failed to commit transaction")?;
                Ok(value)
            }
            Err(error) => {
                self.connection
                    .batch_execute("ROLLBACK")
                    .context("failed to roll back transaction")?;
                Err(error)
            }
        }
    }

    /// Inserts or refreshes one selected source file.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the row.
    pub fn upsert_source_file(
        &mut self,
        record: &OpenFormatRecord,
        local_path: &Path,
        selected_order: u64,
        status: SourceFileStatus,
    ) -> anyhow::Result<()> {
        let size = i64::try_from(record.size).context("source size does not fit i64")?;
        let size_mb = i64::try_from(record.size_mb).context("source size_mb does not fit i64")?;
        let selected_order =
            i64::try_from(selected_order).context("selected_order does not fit i64")?;
        let local_path = local_path.to_string_lossy().into_owned();
        sql_query(
            "\
INSERT INTO source_files (
    filepath, dataset, collection, create_time, size_bytes, size_mb,
    spectra_ms1, spectra_ms2, instrument_vendor, instrument_model, extension,
    local_path, selected_order, status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(filepath) DO UPDATE SET
    dataset = excluded.dataset,
    collection = excluded.collection,
    create_time = excluded.create_time,
    size_bytes = excluded.size_bytes,
    size_mb = excluded.size_mb,
    spectra_ms1 = excluded.spectra_ms1,
    spectra_ms2 = excluded.spectra_ms2,
    instrument_vendor = excluded.instrument_vendor,
    instrument_model = excluded.instrument_model,
    extension = excluded.extension,
    local_path = excluded.local_path,
    selected_order = excluded.selected_order,
    status = CASE
        WHEN source_files.status IN ('downloaded', 'converted') THEN source_files.status
        ELSE excluded.status
    END,
    updated_at = CURRENT_TIMESTAMP
",
        )
        .bind::<Text, _>(&record.filepath)
        .bind::<Text, _>(&record.dataset)
        .bind::<Text, _>(&record.collection)
        .bind::<Text, _>(&record.create_time)
        .bind::<BigInt, _>(size)
        .bind::<BigInt, _>(size_mb)
        .bind::<Nullable<Double>, _>(record.spectra_ms1)
        .bind::<Nullable<Double>, _>(record.spectra_ms2)
        .bind::<Text, _>(&record.instrument_vendor)
        .bind::<Text, _>(&record.instrument_model)
        .bind::<Text, _>(&record.extension)
        .bind::<Text, _>(&local_path)
        .bind::<BigInt, _>(selected_order)
        .bind::<Text, _>(status.as_str())
        .execute(&mut self.connection)
        .context("failed to upsert source file")?;
        Ok(())
    }

    /// Returns source files pending download.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub fn pending_downloads(&mut self, limit: usize) -> anyhow::Result<Vec<SourceFileRow>> {
        let limit = i64::try_from(limit).context("download limit does not fit i64")?;
        sql_query(
            "\
SELECT filepath, dataset, collection, create_time, size_bytes, spectra_ms1,
       spectra_ms2, instrument_vendor, instrument_model, extension, local_path, status
FROM source_files
WHERE status = 'indexed'
ORDER BY selected_order
LIMIT ?
",
        )
        .bind::<BigInt, _>(limit)
        .load::<SourceFileRow>(&mut self.connection)
        .context("failed to load pending downloads")
    }

    /// Returns the total indexed bytes still pending download.
    ///
    /// # Errors
    ///
    /// Returns an error if `SQLite` rejects the query.
    pub fn pending_download_bytes(&mut self) -> anyhow::Result<u64> {
        #[derive(QueryableByName)]
        struct SumRow {
            /// Total byte count.
            #[diesel(sql_type = BigInt)]
            bytes: i64,
        }

        let row = sql_query(
            "\
SELECT COALESCE(SUM(size_bytes), 0) AS bytes
FROM source_files
WHERE status = 'indexed'
",
        )
        .get_result::<SumRow>(&mut self.connection)
        .context("failed to sum pending download bytes")?;
        i64_to_u64(row.bytes, "pending download byte count")
    }

    /// Returns cumulative file and byte totals across the planned download workload.
    ///
    /// The planned set spans every source file already downloaded, still queued
    /// for download, or marked as a previous download failure. The completed
    /// counters cover only files already on disk and persisted to the database.
    ///
    /// # Errors
    ///
    /// Returns an error if `SQLite` rejects the query or returns negative totals.
    pub fn download_progress_totals(&mut self) -> anyhow::Result<DownloadProgressTotals> {
        #[derive(QueryableByName)]
        struct TotalsRow {
            /// Number of planned files.
            #[diesel(sql_type = BigInt)]
            planned_files: i64,
            /// Sum of planned file sizes in bytes.
            #[diesel(sql_type = BigInt)]
            planned_bytes: i64,
            /// Number of files already downloaded successfully.
            #[diesel(sql_type = BigInt)]
            completed_files: i64,
            /// Sum of bytes for files already downloaded successfully.
            #[diesel(sql_type = BigInt)]
            completed_bytes: i64,
        }

        let row = sql_query(
            "\
SELECT
    COUNT(*) AS planned_files,
    COALESCE(SUM(size_bytes), 0) AS planned_bytes,
    COALESCE(SUM(CASE WHEN status = 'downloaded' THEN 1 ELSE 0 END), 0) AS completed_files,
    COALESCE(SUM(CASE WHEN status = 'downloaded' THEN size_bytes ELSE 0 END), 0) AS completed_bytes
FROM source_files
WHERE status IN ('downloaded', 'indexed', 'download_failed')
",
        )
        .get_result::<TotalsRow>(&mut self.connection)
        .context("failed to read download progress totals")?;
        Ok(DownloadProgressTotals {
            planned_files: i64_to_u64(row.planned_files, "planned download file count")?,
            planned_bytes: i64_to_u64(row.planned_bytes, "planned download byte count")?,
            completed_files: i64_to_u64(row.completed_files, "completed download file count")?,
            completed_bytes: i64_to_u64(row.completed_bytes, "completed download byte count")?,
        })
    }

    /// Moves previous failed downloads back to the indexed queue for a new run.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the update.
    pub fn reset_download_failures(&mut self) -> anyhow::Result<u64> {
        let updated = sql_query(
            "\
UPDATE source_files
SET status = 'indexed', updated_at = CURRENT_TIMESTAMP
WHERE status = 'download_failed'
",
        )
        .execute(&mut self.connection)
        .context("failed to reset failed downloads")?;
        u64::try_from(updated).context("reset count does not fit u64")
    }

    /// Returns downloaded source files pending conversion.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub fn downloaded_unconverted(
        &mut self,
        top_k_peaks: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<SourceFileRow>> {
        let limit = i64::try_from(limit).context("conversion limit does not fit i64")?;
        let top_k_peaks = i64::try_from(top_k_peaks).context("top_k does not fit i64")?;
        sql_query(
            "\
SELECT filepath, dataset, collection, create_time, size_bytes, spectra_ms1,
       spectra_ms2, instrument_vendor, instrument_model, extension, local_path, status
FROM source_files
WHERE status = 'downloaded'
  AND NOT EXISTS (
    SELECT 1
    FROM source_file_conversions
    WHERE source_file_conversions.filepath = source_files.filepath
      AND source_file_conversions.top_k_peaks = ?
      AND source_file_conversions.status IN ('converted', 'failed')
  )
ORDER BY selected_order
LIMIT ?
",
        )
        .bind::<BigInt, _>(top_k_peaks)
        .bind::<BigInt, _>(limit)
        .load::<SourceFileRow>(&mut self.connection)
        .context("failed to load downloaded files")
    }

    /// Counts source files with a status.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub fn count_status(&mut self, status: SourceFileStatus) -> anyhow::Result<u64> {
        #[derive(QueryableByName)]
        struct CountRow {
            /// Count value.
            #[diesel(sql_type = BigInt)]
            count: i64,
        }

        let row = sql_query("SELECT COUNT(*) AS count FROM source_files WHERE status = ?")
            .bind::<Text, _>(status.as_str())
            .get_result::<CountRow>(&mut self.connection)
            .context("failed to count source files")?;
        u64::try_from(row.count).context("status count does not fit u64")
    }

    /// Marks one source file as downloaded.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the update.
    pub fn mark_downloaded(&mut self, filepath: &str, bytes: u64) -> anyhow::Result<()> {
        let bytes = i64::try_from(bytes).context("downloaded byte count does not fit i64")?;
        sql_query(
            "\
UPDATE source_files
SET status = 'downloaded', downloaded_bytes = ?, error = NULL, updated_at = CURRENT_TIMESTAMP
WHERE filepath = ?
",
        )
        .bind::<BigInt, _>(bytes)
        .bind::<Text, _>(filepath)
        .execute(&mut self.connection)
        .context("failed to mark source file downloaded")?;
        Ok(())
    }

    /// Marks one source file as failed during download.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the update.
    pub fn mark_download_failed(&mut self, filepath: &str, error: &str) -> anyhow::Result<()> {
        self.mark_failed(filepath, SourceFileStatus::DownloadFailed, error)
    }

    /// Marks one source file as failed during conversion for a top-k setting.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the update.
    pub fn mark_conversion_failed(
        &mut self,
        filepath: &str,
        top_k_peaks: usize,
        error: &str,
    ) -> anyhow::Result<()> {
        sql_query(
            "\
INSERT INTO source_file_conversions (filepath, top_k_peaks, status, error)
VALUES (?, ?, 'failed', ?)
ON CONFLICT(filepath, top_k_peaks) DO UPDATE SET
    status = 'failed',
    error = excluded.error,
    updated_at = CURRENT_TIMESTAMP
",
        )
        .bind::<Text, _>(filepath)
        .bind::<BigInt, _>(i64::try_from(top_k_peaks).context("top_k does not fit i64")?)
        .bind::<Text, _>(error)
        .execute(&mut self.connection)
        .context("failed to mark source file conversion failed")?;
        Ok(())
    }

    /// Marks one source file as running conversion for a top-k setting.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the row.
    pub fn mark_conversion_running(
        &mut self,
        filepath: &str,
        top_k_peaks: usize,
    ) -> anyhow::Result<()> {
        sql_query(
            "\
INSERT INTO source_file_conversions (filepath, top_k_peaks, status)
VALUES (?, ?, 'running')
ON CONFLICT(filepath, top_k_peaks) DO UPDATE SET
    status = 'running',
    error = NULL,
    updated_at = CURRENT_TIMESTAMP
",
        )
        .bind::<Text, _>(filepath)
        .bind::<BigInt, _>(i64::try_from(top_k_peaks).context("top_k does not fit i64")?)
        .execute(&mut self.connection)
        .context("failed to mark source file conversion running")?;
        Ok(())
    }

    /// Updates conversion counters and marks a source file converted.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the update.
    pub fn mark_converted(
        &mut self,
        filepath: &str,
        top_k_peaks: usize,
        record: &SourceConversionRecord,
    ) -> anyhow::Result<()> {
        sql_query(
            "\
INSERT INTO source_file_conversions (
    filepath, top_k_peaks, status, ms1_count, ms2_count, msn_counts_json,
    converted_spectra, duplicate_spectra, missing_precursor_skipped,
    low_peak_skipped
) VALUES (?, ?, 'converted', ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(filepath, top_k_peaks) DO UPDATE SET
    status = 'converted',
    ms1_count = excluded.ms1_count,
    ms2_count = excluded.ms2_count,
    msn_counts_json = excluded.msn_counts_json,
    converted_spectra = excluded.converted_spectra,
    duplicate_spectra = excluded.duplicate_spectra,
    missing_precursor_skipped = excluded.missing_precursor_skipped,
    low_peak_skipped = excluded.low_peak_skipped,
    error = NULL,
    updated_at = CURRENT_TIMESTAMP
",
        )
        .bind::<Text, _>(filepath)
        .bind::<BigInt, _>(i64::try_from(top_k_peaks).context("top_k does not fit i64")?)
        .bind::<BigInt, _>(i64::try_from(record.ms1_count).context("ms1 count does not fit i64")?)
        .bind::<BigInt, _>(i64::try_from(record.ms2_count).context("ms2 count does not fit i64")?)
        .bind::<Text, _>(&record.msn_counts_json)
        .bind::<BigInt, _>(
            i64::try_from(record.converted_spectra)
                .context("converted spectra count does not fit i64")?,
        )
        .bind::<BigInt, _>(
            i64::try_from(record.duplicate_spectra).context("duplicate count does not fit i64")?,
        )
        .bind::<BigInt, _>(
            i64::try_from(record.missing_precursor_skipped)
                .context("missing precursor count does not fit i64")?,
        )
        .bind::<BigInt, _>(
            i64::try_from(record.low_peak_skipped).context("low peak count does not fit i64")?,
        )
        .execute(&mut self.connection)
        .context("failed to mark source file converted")?;
        Ok(())
    }

    /// Inserts a deduplication key for a converted spectrum.
    ///
    /// Returns `true` if this spectrum is new and should be written.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the insert.
    pub fn insert_seen_spectrum(
        &mut self,
        top_k_peaks: usize,
        splash: &str,
        pepmass: f64,
        source_filepath: &str,
        spectrum_id: &str,
        shard_path: &Path,
    ) -> anyhow::Result<bool> {
        let pepmass_key = pepmass_key(pepmass);
        let shard_path = shard_path.to_string_lossy().into_owned();
        let inserted = sql_query(
            "\
INSERT OR IGNORE INTO spectra_seen (
    top_k_peaks, splash, pepmass_key, pepmass, source_filepath, spectrum_id, shard_path
) VALUES (?, ?, ?, ?, ?, ?, ?)
",
        )
        .bind::<BigInt, _>(i64::try_from(top_k_peaks).context("top_k does not fit i64")?)
        .bind::<Text, _>(splash)
        .bind::<Text, _>(&pepmass_key)
        .bind::<Double, _>(pepmass)
        .bind::<Text, _>(source_filepath)
        .bind::<Text, _>(spectrum_id)
        .bind::<Text, _>(&shard_path)
        .execute(&mut self.connection)
        .context("failed to insert spectrum dedupe key")?;
        Ok(inserted == 1)
    }

    /// Returns whether a deduplication key is already finalized.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the query.
    pub fn spectrum_seen(
        &mut self,
        top_k_peaks: usize,
        splash: &str,
        pepmass: f64,
    ) -> anyhow::Result<bool> {
        #[derive(QueryableByName)]
        struct CountRow {
            /// Count value.
            #[diesel(sql_type = BigInt)]
            count: i64,
        }

        let key = pepmass_key(pepmass);
        let row = sql_query(
            "\
SELECT COUNT(*) AS count
FROM spectra_seen
WHERE top_k_peaks = ? AND splash = ? AND pepmass_key = ?
",
        )
        .bind::<BigInt, _>(i64::try_from(top_k_peaks).context("top_k does not fit i64")?)
        .bind::<Text, _>(splash)
        .bind::<Text, _>(&key)
        .get_result::<CountRow>(&mut self.connection)
        .context("failed to query spectrum dedupe key")?;
        Ok(row.count > 0)
    }

    /// Inserts deduplication keys for one finalized shard.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects an insert.
    pub fn insert_seen_spectra(
        &mut self,
        top_k_peaks: usize,
        shard_path: &Path,
        records: &[SpectrumSeenRecord],
    ) -> anyhow::Result<()> {
        let shard_path = shard_path.to_string_lossy().into_owned();
        for record in records {
            let pepmass_key = pepmass_key(record.pepmass);
            sql_query(
                "\
INSERT OR IGNORE INTO spectra_seen (
    top_k_peaks, splash, pepmass_key, pepmass, source_filepath, spectrum_id, shard_path
) VALUES (?, ?, ?, ?, ?, ?, ?)
",
            )
            .bind::<BigInt, _>(i64::try_from(top_k_peaks).context("top_k does not fit i64")?)
            .bind::<Text, _>(&record.splash)
            .bind::<Text, _>(&pepmass_key)
            .bind::<Double, _>(record.pepmass)
            .bind::<Text, _>(&record.source_filepath)
            .bind::<Text, _>(&record.spectrum_id)
            .bind::<Text, _>(&shard_path)
            .execute(&mut self.connection)
            .context("failed to insert finalized spectrum dedupe key")?;
        }
        Ok(())
    }

    /// Upserts a generated MGF shard row.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the row.
    pub fn upsert_shard(
        &mut self,
        path: &Path,
        shard_index: u64,
        top_k_peaks: usize,
        spectra_written: u64,
        bytes_written: u64,
        sha256: Option<&str>,
    ) -> anyhow::Result<()> {
        let path = path.to_string_lossy().into_owned();
        sql_query(
            "\
INSERT INTO mgf_shards (
    shard_path, shard_index, top_k_peaks, spectra_written, bytes_written, sha256
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(shard_path) DO UPDATE SET
    spectra_written = excluded.spectra_written,
    bytes_written = excluded.bytes_written,
    sha256 = excluded.sha256
",
        )
        .bind::<Text, _>(&path)
        .bind::<BigInt, _>(i64::try_from(shard_index).context("shard index does not fit i64")?)
        .bind::<BigInt, _>(i64::try_from(top_k_peaks).context("top_k does not fit i64")?)
        .bind::<BigInt, _>(
            i64::try_from(spectra_written).context("spectra_written does not fit i64")?,
        )
        .bind::<BigInt, _>(i64::try_from(bytes_written).context("bytes_written does not fit i64")?)
        .bind::<Nullable<Text>, _>(sha256)
        .execute(&mut self.connection)
        .context("failed to upsert MGF shard")?;
        Ok(())
    }

    /// Returns finalized shards for one top-k setting.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the query.
    pub fn finalized_shards(
        &mut self,
        top_k_peaks: usize,
    ) -> anyhow::Result<Vec<FinalizedShardRow>> {
        sql_query(
            "\
SELECT shard_path, shard_index, top_k_peaks, spectra_written, bytes_written, sha256
FROM mgf_shards
WHERE top_k_peaks = ?
ORDER BY shard_index
",
        )
        .bind::<BigInt, _>(i64::try_from(top_k_peaks).context("top_k does not fit i64")?)
        .load::<FinalizedShardRow>(&mut self.connection)
        .context("failed to load finalized shards")
    }

    /// Returns the next available shard index for one top-k setting.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the query.
    pub fn next_shard_index(&mut self, top_k_peaks: usize) -> anyhow::Result<u64> {
        #[derive(QueryableByName)]
        struct MaxRow {
            /// Maximum shard index.
            #[diesel(sql_type = Nullable<BigInt>)]
            value: Option<i64>,
        }

        let row =
            sql_query("SELECT MAX(shard_index) AS value FROM mgf_shards WHERE top_k_peaks = ?")
                .bind::<BigInt, _>(i64::try_from(top_k_peaks).context("top_k does not fit i64")?)
                .get_result::<MaxRow>(&mut self.connection)
                .context("failed to query next shard index")?;
        let next = row.value.map_or(0_i64, |value| value.saturating_add(1));
        u64::try_from(next).context("next shard index does not fit u64")
    }

    /// Returns aggregate conversion counters for one top-k setting.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects a query.
    pub fn conversion_summary(
        &mut self,
        top_k_peaks: usize,
    ) -> anyhow::Result<ConversionDbSummary> {
        #[derive(QueryableByName)]
        struct SummaryRow {
            /// Converted file count.
            #[diesel(sql_type = BigInt)]
            files_converted: i64,
            /// Failed file count.
            #[diesel(sql_type = BigInt)]
            files_failed: i64,
            /// Duplicate count.
            #[diesel(sql_type = Nullable<BigInt>)]
            duplicates_skipped: Option<i64>,
            /// Missing precursor count.
            #[diesel(sql_type = Nullable<BigInt>)]
            missing_precursor_skipped: Option<i64>,
            /// Low peak count.
            #[diesel(sql_type = Nullable<BigInt>)]
            low_peak_skipped: Option<i64>,
        }
        #[derive(QueryableByName)]
        struct CountRow {
            /// Count value.
            #[diesel(sql_type = BigInt)]
            count: i64,
        }

        let top_k = i64::try_from(top_k_peaks).context("top_k does not fit i64")?;
        let summary = sql_query(
            "\
SELECT
  COUNT(CASE WHEN status = 'converted' THEN 1 END) AS files_converted,
  COUNT(CASE WHEN status = 'failed' THEN 1 END) AS files_failed,
  SUM(duplicate_spectra) AS duplicates_skipped,
  SUM(missing_precursor_skipped) AS missing_precursor_skipped,
  SUM(low_peak_skipped) AS low_peak_skipped
FROM source_file_conversions
WHERE top_k_peaks = ?
",
        )
        .bind::<BigInt, _>(top_k)
        .get_result::<SummaryRow>(&mut self.connection)
        .context("failed to query conversion summary")?;
        let spectra = sql_query("SELECT COUNT(*) AS count FROM spectra_seen WHERE top_k_peaks = ?")
            .bind::<BigInt, _>(top_k)
            .get_result::<CountRow>(&mut self.connection)
            .context("failed to count finalized spectra")?;
        let shards = sql_query("SELECT COUNT(*) AS count FROM mgf_shards WHERE top_k_peaks = ?")
            .bind::<BigInt, _>(top_k)
            .get_result::<CountRow>(&mut self.connection)
            .context("failed to count finalized shards")?;
        Ok(ConversionDbSummary {
            files_converted: i64_to_u64(summary.files_converted, "converted file count")?,
            files_failed: i64_to_u64(summary.files_failed, "failed file count")?,
            spectra_written: i64_to_u64(spectra.count, "finalized spectrum count")?,
            duplicates_skipped: i64_to_u64(
                summary.duplicates_skipped.unwrap_or_default(),
                "duplicate count",
            )?,
            missing_precursor_skipped: i64_to_u64(
                summary.missing_precursor_skipped.unwrap_or_default(),
                "missing precursor count",
            )?,
            low_peak_skipped: i64_to_u64(
                summary.low_peak_skipped.unwrap_or_default(),
                "low peak count",
            )?,
            shards_written: i64_to_u64(shards.count, "finalized shard count")?,
        })
    }

    /// Returns stored MSn counter JSON blobs for converted files.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the query.
    pub fn converted_msn_counts_json(&mut self, top_k_peaks: usize) -> anyhow::Result<Vec<String>> {
        #[derive(QueryableByName)]
        struct MsnRow {
            /// Stored MSn counter JSON.
            #[diesel(sql_type = Text)]
            msn_counts_json: String,
        }

        sql_query(
            "\
SELECT msn_counts_json
FROM source_file_conversions
WHERE top_k_peaks = ? AND status = 'converted'
",
        )
        .bind::<BigInt, _>(i64::try_from(top_k_peaks).context("top_k does not fit i64")?)
        .load::<MsnRow>(&mut self.connection)
        .map(|rows| {
            rows.into_iter()
                .map(|row| row.msn_counts_json)
                .collect::<Vec<_>>()
        })
        .context("failed to load converted MSn counters")
    }

    /// Counts downloaded files that still need conversion for one top-k setting.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the query.
    pub fn count_downloaded_unconverted(&mut self, top_k_peaks: usize) -> anyhow::Result<u64> {
        #[derive(QueryableByName)]
        struct CountRow {
            /// Count value.
            #[diesel(sql_type = BigInt)]
            count: i64,
        }

        let row = sql_query(
            "\
SELECT COUNT(*) AS count
FROM source_files
WHERE status = 'downloaded'
  AND NOT EXISTS (
    SELECT 1 FROM source_file_conversions
    WHERE source_file_conversions.filepath = source_files.filepath
      AND source_file_conversions.top_k_peaks = ?
      AND source_file_conversions.status IN ('converted', 'failed')
  )
",
        )
        .bind::<BigInt, _>(i64::try_from(top_k_peaks).context("top_k does not fit i64")?)
        .get_result::<CountRow>(&mut self.connection)
        .context("failed to count unconverted downloads")?;
        i64_to_u64(row.count, "unconverted download count")
    }

    /// Counts failed conversions for one top-k setting.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects the query.
    pub fn count_conversion_failed(&mut self, top_k_peaks: usize) -> anyhow::Result<u64> {
        #[derive(QueryableByName)]
        struct CountRow {
            /// Count value.
            #[diesel(sql_type = BigInt)]
            count: i64,
        }

        let row = sql_query(
            "\
SELECT COUNT(*) AS count
FROM source_file_conversions
WHERE top_k_peaks = ? AND status = 'failed'
",
        )
        .bind::<BigInt, _>(i64::try_from(top_k_peaks).context("top_k does not fit i64")?)
        .get_result::<CountRow>(&mut self.connection)
        .context("failed to count failed conversions")?;
        i64_to_u64(row.count, "failed conversion count")
    }

    /// Promotes candidate mzML files to the download queue until an MS/MS budget is reached.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite rejects a query or update.
    pub fn promote_candidates_by_ms2(&mut self, target_ms2: u64) -> anyhow::Result<u64> {
        let candidates = sql_query(
            "\
SELECT filepath, dataset, collection, create_time, size_bytes, spectra_ms1,
       spectra_ms2, instrument_vendor, instrument_model, extension, local_path, status
FROM source_files
WHERE status = 'candidate'
ORDER BY selected_order
",
        )
        .load::<SourceFileRow>(&mut self.connection)
        .context("failed to load candidate source files")?;
        let mut promoted = 0_u64;
        let mut promoted_ms2 = 0_u64;
        for candidate in candidates {
            if promoted_ms2 >= target_ms2 {
                break;
            }
            sql_query(
                "\
UPDATE source_files
SET status = 'indexed', updated_at = CURRENT_TIMESTAMP
WHERE filepath = ? AND status = 'candidate'
",
            )
            .bind::<Text, _>(&candidate.filepath)
            .execute(&mut self.connection)
            .context("failed to promote candidate source file")?;
            promoted = promoted.saturating_add(1);
            promoted_ms2 = promoted_ms2.saturating_add(
                candidate
                    .spectra_ms2
                    .and_then(f64_to_u64)
                    .unwrap_or_default(),
            );
        }
        Ok(promoted)
    }

    /// Marks a source file failed with the requested status.
    fn mark_failed(
        &mut self,
        filepath: &str,
        status: SourceFileStatus,
        error: &str,
    ) -> anyhow::Result<()> {
        sql_query(
            "\
UPDATE source_files
SET status = ?, error = ?, updated_at = CURRENT_TIMESTAMP
WHERE filepath = ?
",
        )
        .bind::<Text, _>(status.as_str())
        .bind::<Text, _>(error)
        .bind::<Text, _>(filepath)
        .execute(&mut self.connection)
        .context("failed to mark source file failed")?;
        Ok(())
    }
}

/// Builds the canonical precursor m/z key for deduplication.
fn pepmass_key(pepmass: f64) -> String {
    format!("{pepmass:.8}")
}

/// Builds an in-memory deduplication key matching the database key.
#[must_use]
pub fn spectrum_dedupe_key(splash: &str, pepmass: f64) -> String {
    format!("{splash}\t{}", pepmass_key(pepmass))
}

/// Converts a non-negative SQLite integer to `u64`.
fn i64_to_u64(value: i64, label: &str) -> anyhow::Result<u64> {
    u64::try_from(value).with_context(|| format!("{label} does not fit u64"))
}

/// Converts a finite non-negative `f64` to `u64` after bounds checking.
fn f64_to_u64(value: f64) -> Option<u64> {
    if !(value.is_finite() && value >= 0.0) {
        return None;
    }
    let text = format!("{value:.0}");
    text.parse::<u64>().ok()
}

/// Creates the parent directory for a SQLite path when needed.
fn create_sqlite_parent(database_url: &str) -> anyhow::Result<()> {
    if database_url == ":memory:" || database_url.starts_with("file:") {
        return Ok(());
    }
    if let Some(parent) = Path::new(database_url).parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use diesel::sql_types::BigInt;
    use diesel::{QueryableByName, RunQueryDsl};

    use crate::index::{OpenFormatRecord, SourceFileStatus};

    use super::{SourceConversionRecord, SpectrumSeenRecord, StateDb};

    /// Confirms the schema can be initialized in memory.
    #[test]
    fn initializes_in_memory() -> anyhow::Result<()> {
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        assert_eq!(db.count_status(crate::index::SourceFileStatus::Indexed)?, 0);
        Ok(())
    }

    /// Confirms connections wait briefly for concurrent SQLite writers.
    #[test]
    fn initializes_sqlite_busy_timeout() -> anyhow::Result<()> {
        #[derive(QueryableByName)]
        struct BusyTimeoutRow {
            #[diesel(sql_type = BigInt)]
            timeout: i64,
        }

        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        let row = diesel::sql_query("PRAGMA busy_timeout")
            .get_result::<BusyTimeoutRow>(&mut db.connection)?;
        assert_eq!(row.timeout, 60_000);
        Ok(())
    }

    /// Confirms download progress totals fold completed and pending work together.
    #[test]
    fn download_progress_totals_combine_completed_and_pending() -> anyhow::Result<()> {
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        let downloaded_record = test_record_with_size("MSV000000001/path/done.mzML", 1.0, 1_000);
        let pending_record = test_record_with_size("MSV000000001/path/pending.mzML", 1.0, 2_500);
        let failed_record = test_record_with_size("MSV000000001/path/failed.mzML", 1.0, 500);
        let candidate_record = test_record_with_size("MSV000000001/path/candidate.mzML", 1.0, 9_000);
        db.upsert_source_file(
            &downloaded_record,
            Path::new("/tmp/done.mzML"),
            0,
            SourceFileStatus::Downloaded,
        )?;
        db.upsert_source_file(
            &pending_record,
            Path::new("/tmp/pending.mzML"),
            1,
            SourceFileStatus::Indexed,
        )?;
        db.upsert_source_file(
            &failed_record,
            Path::new("/tmp/failed.mzML"),
            2,
            SourceFileStatus::DownloadFailed,
        )?;
        db.upsert_source_file(
            &candidate_record,
            Path::new("/tmp/candidate.mzML"),
            3,
            SourceFileStatus::Candidate,
        )?;
        let totals = db.download_progress_totals()?;
        assert_eq!(totals.planned_files, 3);
        assert_eq!(totals.planned_bytes, 4_000);
        assert_eq!(totals.completed_files, 1);
        assert_eq!(totals.completed_bytes, 1_000);
        Ok(())
    }

    /// Confirms candidate rows can be promoted into the download queue.
    #[test]
    fn promotes_candidate_sources() -> anyhow::Result<()> {
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        let record = test_record("MSV000000001/path/file.mzML", 10.0);
        db.upsert_source_file(
            &record,
            Path::new("/tmp/file.mzML"),
            0,
            SourceFileStatus::Candidate,
        )?;
        assert_eq!(db.count_status(SourceFileStatus::Candidate)?, 1);
        assert_eq!(db.promote_candidates_by_ms2(1)?, 1);
        assert_eq!(db.count_status(SourceFileStatus::Indexed)?, 1);
        Ok(())
    }

    /// Confirms failed conversions are not immediately selected again.
    #[test]
    fn failed_conversion_is_not_selected_again() -> anyhow::Result<()> {
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        let record = test_record("MSV000000001/path/file.mzML", 10.0);
        db.upsert_source_file(
            &record,
            Path::new("/tmp/file.mzML"),
            0,
            SourceFileStatus::Downloaded,
        )?;
        assert_eq!(db.downloaded_unconverted(256, 10)?.len(), 1);
        db.mark_conversion_failed(&record.filepath, 256, "bad mzML")?;
        assert!(db.downloaded_unconverted(256, 10)?.is_empty());
        assert_eq!(db.count_downloaded_unconverted(256)?, 0);
        assert_eq!(db.count_conversion_failed(256)?, 1);
        assert_eq!(db.downloaded_unconverted(128, 10)?.len(), 1);
        Ok(())
    }

    /// Confirms conversion summaries count only finalized dedupe rows as spectra.
    #[test]
    fn summarizes_finalized_conversion_rows() -> anyhow::Result<()> {
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        db.mark_converted(
            "MSV000000001/path/file.mzML",
            256,
            &SourceConversionRecord {
                ms1_count: 1,
                ms2_count: 2,
                msn_counts_json: "{\"1\":1,\"2\":2}".to_owned(),
                converted_spectra: 1,
                duplicate_spectra: 1,
                missing_precursor_skipped: 0,
                low_peak_skipped: 0,
            },
        )?;
        db.insert_seen_spectra(
            256,
            Path::new("/tmp/shard.mgf.zst"),
            &[SpectrumSeenRecord {
                splash: "splash10-test".to_owned(),
                pepmass: 100.0,
                source_filepath: "MSV000000001/path/file.mzML".to_owned(),
                spectrum_id: "scan=1".to_owned(),
            }],
        )?;
        let summary = db.conversion_summary(256)?;
        assert_eq!(summary.files_converted, 1);
        assert_eq!(summary.spectra_written, 1);
        assert_eq!(summary.duplicates_skipped, 1);
        Ok(())
    }

    /// Builds a small test open-format record.
    fn test_record(filepath: &str, spectra_ms2: f64) -> OpenFormatRecord {
        test_record_with_size(filepath, spectra_ms2, 1)
    }

    /// Builds a small test open-format record with a specific byte size.
    fn test_record_with_size(filepath: &str, spectra_ms2: f64, size: u64) -> OpenFormatRecord {
        OpenFormatRecord {
            filepath: filepath.to_owned(),
            dataset: "MSV000000001".to_owned(),
            collection: "peak".to_owned(),
            create_time: "2021-01-01 00:00:00".to_owned(),
            size,
            size_mb: 1,
            spectra_ms1: Some(1.0),
            spectra_ms2: Some(spectra_ms2),
            instrument_vendor: "vendor".to_owned(),
            instrument_model: "model".to_owned(),
            extension: ".mzML".to_owned(),
        }
    }
}
