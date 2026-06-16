//! Keeps the GNPS/MassIVE open-format index TSVs in sync with Zenodo.
//!
//! On each invocation, the module's only public entry point fetches the
//! record metadata for `OPENFORMATS_RECORD_ID`, compares its `modified`
//! timestamp against the on-disk mtime of the cached TSVs, and re-downloads
//! the TSVs only when the record has been updated since the last fetch.
//!
//! Despite the module name (kept to minimise churn), this module no longer
//! publishes anything to Zenodo. Publication of the resulting MGF corpus was
//! dropped because Zenodo's 50 GB per-record cap is far below the corpus
//! size; bulk distribution will happen through other channels.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use zenodo_rs::{RecordId, ZenodoClient};

use crate::config::Config;
use crate::progress::ProgressReporter;

/// GNPS index filename in the Zenodo record.
const GNPS_INDEX_KEY: &str = "gnps_public_openformats.tsv";
/// MassIVE index filename in the Zenodo record.
const MASSIVE_INDEX_KEY: &str = "massive_public_openformats.tsv";

/// Outcome of a [`ensure_openformats_indexes_fresh`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IndexFreshness {
    /// Local TSVs were already up to date; nothing was downloaded.
    AlreadyFresh,
    /// Local TSVs were missing or older than the Zenodo record and were
    /// re-downloaded.
    Refreshed,
}

/// Keeps the local open-format index TSVs in sync with Zenodo.
///
/// Compares the Zenodo record's `modified` timestamp against the on-disk
/// mtime of each TSV. If either TSV is older than the Zenodo record, both
/// are re-downloaded so the local copies stay consistent. Returns
/// [`IndexFreshness::AlreadyFresh`] when nothing needed to be fetched.
///
/// # Errors
///
/// Returns an error if the anonymous Zenodo client cannot be built, the
/// record metadata cannot be fetched, a filesystem inspection fails, or a
/// TSV download fails.
pub async fn ensure_openformats_indexes_fresh(
    config: &Config,
    progress: &ProgressReporter,
) -> Result<IndexFreshness> {
    fs::create_dir_all(&config.openformats_index_dir).with_context(|| {
        format!(
            "failed to create {}",
            config.openformats_index_dir.display()
        )
    })?;
    let client = ZenodoClient::anonymous().context("failed to build anonymous Zenodo client")?;
    let record_id = RecordId(config.openformats_record_id);
    let record = client.get_record(record_id).await.with_context(|| {
        format!(
            "failed to fetch Zenodo record {}",
            config.openformats_record_id
        )
    })?;
    let zenodo_modified = record
        .modified
        .context("Zenodo record has no modification timestamp")?;

    let gnps_path = config.gnps_index_path();
    let massive_path = config.massive_index_path();
    let needs_refresh =
        is_stale(&gnps_path, zenodo_modified)? || is_stale(&massive_path, zenodo_modified)?;
    if !needs_refresh {
        progress.println(format!(
            "open-format indexes are up to date (Zenodo record {} last modified {zenodo_modified})",
            config.openformats_record_id,
        ))?;
        return Ok(IndexFreshness::AlreadyFresh);
    }

    progress.println(format!(
        "open-format indexes are stale or missing; refreshing from Zenodo record {} (modified {zenodo_modified})",
        config.openformats_record_id,
    ))?;
    let files_bar = progress.count_bar(2, "open-format index files")?;
    download_record_file(&client, record_id, GNPS_INDEX_KEY, &gnps_path, progress).await?;
    files_bar.inc(1);
    download_record_file(
        &client,
        record_id,
        MASSIVE_INDEX_KEY,
        &massive_path,
        progress,
    )
    .await?;
    files_bar.inc(1);
    files_bar.finish_with_message("open-format index files downloaded");
    Ok(IndexFreshness::Refreshed)
}

/// Returns whether the local file is older than the Zenodo record, missing,
/// or has an unreadable mtime.
fn is_stale(path: &Path, zenodo_modified: DateTime<Utc>) -> Result<bool> {
    match fs::metadata(path) {
        Ok(metadata) => {
            let mtime = metadata.modified().with_context(|| {
                format!("filesystem does not report mtime for {}", path.display())
            })?;
            let local_modified: DateTime<Utc> = mtime.into();
            Ok(local_modified < zenodo_modified)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error).with_context(|| format!("failed to stat {}", path.display())),
    }
}

/// Downloads one named file from the Zenodo record to the local path.
///
/// Uses zenodo-rs's progress-aware download so each byte chunk advances an
/// `indicatif` bar in the shared reporter.
async fn download_record_file(
    client: &ZenodoClient,
    record_id: RecordId,
    key: &str,
    destination: &Path,
    progress: &ProgressReporter,
) -> Result<()> {
    let bar = progress.byte_bar(0, format!("downloading Zenodo index {key}"))?;
    client
        .download_record_file_by_key_to_path_with_progress(record_id, key, destination, bar.clone())
        .await
        .with_context(|| format!("failed to download Zenodo file {key}"))?;
    bar.finish_with_message(format!("downloaded {key}"));
    Ok(())
}
