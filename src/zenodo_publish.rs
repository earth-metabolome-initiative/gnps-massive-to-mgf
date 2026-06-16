//! Downloads the GNPS/MassIVE open-format index TSVs from Zenodo.
//!
//! Despite the module name (kept for now to minimise churn), this module no
//! longer publishes anything to Zenodo. It only fetches the two cached
//! open-format index TSVs from the public Zenodo record so the pipeline can
//! ingest them. Publication of the resulting MGF corpus was dropped because
//! Zenodo's 50 GB per-record cap is far below the corpus size, and bulk
//! distribution will happen through other channels.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail};
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;

use crate::config::Config;
use crate::progress::ProgressReporter;

/// GNPS index filename in the Zenodo record.
const GNPS_INDEX_KEY: &str = "gnps_public_openformats.tsv";
/// MassIVE index filename in the Zenodo record.
const MASSIVE_INDEX_KEY: &str = "massive_public_openformats.tsv";

/// Downloads the two Zenodo index TSV files into the configured cache directory.
///
/// # Errors
///
/// Returns an error if the files cannot be downloaded or written.
pub async fn download_openformats_indexes(
    config: &Config,
    progress: &ProgressReporter,
) -> anyhow::Result<()> {
    fs::create_dir_all(&config.openformats_index_dir).with_context(|| {
        format!(
            "failed to create {}",
            config.openformats_index_dir.display()
        )
    })?;
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_mins(10))
        .user_agent("gnps-massive-to-mgf/0.1")
        .build()
        .context("failed to build HTTP client")?;
    let files_bar = progress.count_bar(2, "open-format index files")?;

    download_record_file(
        &client,
        config.openformats_record_id,
        GNPS_INDEX_KEY,
        &config.gnps_index_path(),
        progress,
    )
    .await?;
    files_bar.inc(1);
    download_record_file(
        &client,
        config.openformats_record_id,
        MASSIVE_INDEX_KEY,
        &config.massive_index_path(),
        progress,
    )
    .await?;
    files_bar.inc(1);
    files_bar.finish_with_message("open-format index files downloaded");
    Ok(())
}

/// Downloads one public Zenodo record file by key.
async fn download_record_file(
    client: &reqwest::Client,
    record_id: u64,
    key: &str,
    destination: &Path,
    progress: &ProgressReporter,
) -> anyhow::Result<()> {
    let url = format!("https://zenodo.org/api/records/{record_id}/files/{key}/content");
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to request Zenodo file {key}"))?;
    if !response.status().is_success() {
        bail!("Zenodo returned HTTP {} for {key}", response.status());
    }
    let length = response.content_length().unwrap_or_default();
    let bar = progress.byte_bar(length, format!("downloading Zenodo index {key}"))?;
    let part_path = part_path(destination);
    let mut file = tokio::fs::File::create(&part_path)
        .await
        .with_context(|| format!("failed to create {}", part_path.display()))?;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("failed while downloading {key}"))?;
        file.write_all(&chunk)
            .await
            .with_context(|| format!("failed to write {}", part_path.display()))?;
        bar.inc(u64::try_from(chunk.len()).context("chunk length does not fit u64")?);
    }
    file.flush()
        .await
        .with_context(|| format!("failed to flush {}", part_path.display()))?;
    drop(file);
    fs::rename(&part_path, destination).with_context(|| {
        format!(
            "failed to rename {} to {}",
            part_path.display(),
            destination.display()
        )
    })?;
    bar.finish_with_message(format!("downloaded {key}"));
    Ok(())
}

/// Builds the temporary partial-download path.
fn part_path(destination: &Path) -> PathBuf {
    let mut value = destination.as_os_str().to_owned();
    value.push(".part");
    PathBuf::from(value)
}
