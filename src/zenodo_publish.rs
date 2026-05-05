//! Zenodo index download and result publication.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use chrono::Utc;
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use zenodo_rs::{
    AccessRight, Auth, Creator, DepositMetadataUpdate, DepositionId, FileReplacePolicy,
    PublishedRecord, RelatedIdentifier, UploadSpec, UploadType, ZenodoClient,
};

use crate::checksum::sha256_file;
use crate::config::Config;
use crate::db::StateDb;
use crate::index::SourceFileStatus;
use crate::progress::ProgressReporter;

/// GNPS index filename in the Zenodo record.
const GNPS_INDEX_KEY: &str = "gnps_public_openformats.tsv";
/// MassIVE index filename in the Zenodo record.
const MASSIVE_INDEX_KEY: &str = "massive_public_openformats.tsv";
/// Converter repository URL.
const CONVERTER_REPOSITORY_URL: &str = "https://github.com/LucaCappelletti94/gnps-massive-to-mgf";
/// Source Zenodo DOI.
const SOURCE_ZENODO_DOI: &str = "10.5281/zenodo.4549746";
/// Source Zenodo record URL.
const SOURCE_ZENODO_URL: &str = "https://zenodo.org/records/4549746";
/// Zenodo creator display name.
const ZENODO_CREATOR_NAME: &str = "Cappelletti, Luca";
/// Zenodo creator ORCID identifier.
const ZENODO_CREATOR_ORCID: &str = "0000-0002-1269-2038";
/// Zenodo creator affiliation.
const ZENODO_CREATOR_AFFILIATION: &str = "University of Fribourg";
/// Target Zenodo community slug.
const ZENODO_COMMUNITY: &str = "earth-metabolome";
/// Zenodo license identifier for the converted dataset.
const ZENODO_LICENSE: &str = "cc-by-4.0";

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
    let mut builder = reqwest::Client::builder()
        .connect_timeout(config.http_connect_timeout)
        .user_agent("gnps-massive-to-mgf/0.1");
    if let Some(timeout) = config.http_request_timeout {
        builder = builder.timeout(timeout);
    }
    let client = builder.build().context("failed to build HTTP client")?;
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

/// Publishes generated MGF shards and sidecars to production Zenodo.
///
/// # Errors
///
/// Returns an error if `ZENODO_TOKEN` is absent, output files are missing, or
/// Zenodo rejects metadata, upload, or publication.
pub async fn publish_outputs_to_zenodo(
    db: &mut StateDb,
    config: &Config,
    progress: &ProgressReporter,
) -> anyhow::Result<PublishedRecord> {
    if !config.publish_to_zenodo {
        bail!("ZENODO_TOKEN is not set");
    }
    validate_outputs_for_publication(db, config)?;
    let metadata = publication_metadata(config)?;
    let client = ZenodoClient::new(Auth::from_env()?)?;
    let specs = upload_specs(&config.mgf_output_dir)?;
    progress.println(format!(
        "publishing {} files to production Zenodo",
        specs.len()
    ))?;
    let publish_bar = progress.spinner(format!(
        "publishing {} files to production Zenodo",
        specs.len()
    ))?;

    let result = match config.zenodo_deposition_id {
        Some(id) => client
            .publish_dataset_with_policy(
                DepositionId::from(id),
                &metadata,
                FileReplacePolicy::ReplaceAll,
                specs,
            )
            .await
            .context("failed to publish existing Zenodo deposition"),
        None => client
            .create_and_publish_dataset_with_policy(&metadata, FileReplacePolicy::ReplaceAll, specs)
            .await
            .context("failed to create and publish Zenodo deposition"),
    };
    match result {
        Ok(record) => {
            publish_bar
                .finish_with_message(format!("published Zenodo record {}", record.record.id));
            Ok(record)
        }
        Err(error) => {
            publish_bar.abandon_with_message("Zenodo publication failed");
            Err(error)
        }
    }
}

/// Dry-run summary for a future Zenodo publication.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PublicationDryRunSummary {
    /// Whether publication would create a new deposition or replace an existing one.
    pub deposition_mode: String,
    /// Number of upload specs built for Zenodo.
    pub upload_spec_count: usize,
    /// Number of generated output files discovered locally.
    pub file_count: usize,
    /// Total generated output bytes discovered locally.
    pub total_bytes: u64,
    /// Largest generated output file path.
    pub largest_file: Option<PathBuf>,
    /// Largest generated output file byte count.
    pub largest_file_bytes: u64,
    /// Configured target number of unique MS/MS spectra.
    pub target_ms2_spectra: u64,
    /// Finalized unique MS/MS spectra for the configured top-k value.
    pub finalized_spectra: u64,
    /// Configured top-k fragment peak cap.
    pub top_k_peaks: usize,
    /// Whether blocking publication validation passed.
    pub validation_passed: bool,
}

impl PublicationDryRunSummary {
    /// Formats this dry-run summary as stable human-readable lines.
    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        vec![
            "Zenodo publication dry run".to_owned(),
            format!("validation_passed: {}", self.validation_passed),
            format!("deposition_mode: {}", self.deposition_mode),
            format!("upload_spec_count: {}", self.upload_spec_count),
            format!("file_count: {}", self.file_count),
            format!("total_bytes: {}", self.total_bytes),
            format!("largest_file: {}", self.largest_file_label()),
            format!("top_k_peaks: {}", self.top_k_peaks),
            format!("target_ms2_spectra: {}", self.target_ms2_spectra),
            format!("finalized_spectra: {}", self.finalized_spectra),
        ]
    }

    /// Formats the largest file as a compact label.
    fn largest_file_label(&self) -> String {
        self.largest_file.as_ref().map_or_else(
            || "none".to_owned(),
            |path| format!("{} ({} bytes)", path.display(), self.largest_file_bytes),
        )
    }
}

/// Builds the same local publication inputs as real publishing without contacting Zenodo.
///
/// # Errors
///
/// Returns an error if local publication validation fails, metadata cannot be
/// built, upload specs cannot be built, or generated output files cannot be
/// inspected.
pub fn publication_dry_run(
    db: &mut StateDb,
    config: &Config,
) -> anyhow::Result<PublicationDryRunSummary> {
    validate_outputs_for_publication(db, config)?;
    publication_metadata(config).context("failed to build Zenodo metadata")?;
    let upload_spec_count = upload_specs(&config.mgf_output_dir)?.len();
    let paths = generated_output_paths(&config.mgf_output_dir)?;
    let (file_count, total_bytes, largest_file, largest_file_bytes) =
        summarize_output_paths(&paths)?;
    let conversion_summary = db.conversion_summary(config.top_k_peaks)?;
    Ok(PublicationDryRunSummary {
        deposition_mode: deposition_mode_label(config),
        upload_spec_count,
        file_count,
        total_bytes,
        largest_file,
        largest_file_bytes,
        target_ms2_spectra: config.target_ms2_spectra,
        finalized_spectra: conversion_summary.spectra_written,
        top_k_peaks: config.top_k_peaks,
        validation_passed: true,
    })
}

/// Validates that generated output is complete and internally consistent.
///
/// # Errors
///
/// Returns an error describing every blocking publication issue found.
pub fn validate_outputs_for_publication(db: &mut StateDb, config: &Config) -> anyhow::Result<()> {
    let mut errors = Vec::new();
    let indexed = db.count_status(SourceFileStatus::Indexed)?;
    let download_failed = db.count_status(SourceFileStatus::DownloadFailed)?;
    let unconverted = db.count_downloaded_unconverted(config.top_k_peaks)?;
    let conversion_failed = db.count_conversion_failed(config.top_k_peaks)?;
    let summary = db.conversion_summary(config.top_k_peaks)?;
    if indexed > 0 {
        errors.push(format!(
            "{indexed} selected source file(s) still need download"
        ));
    }
    if download_failed > 0 {
        errors.push(format!("{download_failed} source file download(s) failed"));
    }
    if unconverted > 0 {
        errors.push(format!(
            "{unconverted} downloaded source file(s) still need conversion"
        ));
    }
    if conversion_failed > 0 {
        errors.push(format!(
            "{conversion_failed} source file conversion(s) failed"
        ));
    }
    if summary.spectra_written < config.target_ms2_spectra {
        errors.push(format!(
            "only {} finalized unique spectra, target is {}",
            summary.spectra_written, config.target_ms2_spectra
        ));
    }
    validate_no_temporary_shards(&config.mgf_output_dir, config.top_k_peaks, &mut errors)?;
    validate_required_sidecars(&config.mgf_output_dir, &mut errors);
    validate_manifest_matches_shards(db, config, &mut errors)?;
    validate_finalized_shards(db, config, &mut errors)?;
    validate_upload_file_sizes(
        &config.mgf_output_dir,
        config.zenodo_max_file_bytes,
        &mut errors,
    )?;
    if errors.is_empty() {
        Ok(())
    } else {
        bail!("publication validation failed:\n{}", errors.join("\n"))
    }
}

/// Validates that required sidecar files exist.
fn validate_required_sidecars(output_dir: &Path, errors: &mut Vec<String>) {
    for name in ["manifest.csv", "conversion_report.json", "README.md"] {
        let path = output_dir.join(name);
        if !path.is_file() {
            errors.push(format!("required sidecar is missing: {}", path.display()));
        }
    }
}

/// Validates that `manifest.csv` matches finalized shard rows in SQLite.
fn validate_manifest_matches_shards(
    db: &mut StateDb,
    config: &Config,
    errors: &mut Vec<String>,
) -> anyhow::Result<()> {
    let path = config.manifest_path();
    if !path.is_file() {
        return Ok(());
    }
    let mut manifest = Vec::new();
    let mut reader = csv::Reader::from_path(&path)
        .with_context(|| format!("failed to read manifest {}", path.display()))?;
    for row in reader.records() {
        let row = row.with_context(|| format!("failed to parse manifest {}", path.display()))?;
        let Some(shard_path) = row.get(1) else {
            errors.push(format!(
                "manifest row is missing path in {}",
                path.display()
            ));
            continue;
        };
        let spectra_written = row.get(2).and_then(|value| value.parse::<u64>().ok());
        let bytes_written = row.get(3).and_then(|value| value.parse::<u64>().ok());
        let sha256 = row.get(4).map(str::to_owned);
        manifest.push((
            shard_path.to_owned(),
            spectra_written,
            bytes_written,
            sha256,
        ));
    }
    let shards = db.finalized_shards(config.top_k_peaks)?;
    if manifest.len() != shards.len() {
        errors.push(format!(
            "manifest lists {} shard(s), SQLite records {}",
            manifest.len(),
            shards.len()
        ));
    }
    for shard in shards {
        let expected_spectra =
            u64::try_from(shard.spectra_written).context("shard spectra count does not fit u64")?;
        let expected_bytes =
            u64::try_from(shard.bytes_written).context("shard byte count does not fit u64")?;
        let Some(row) = manifest
            .iter()
            .find(|(path, _, _, _)| path == &shard.shard_path)
        else {
            errors.push(format!("manifest is missing shard {}", shard.shard_path));
            continue;
        };
        if row.1 != Some(expected_spectra) {
            errors.push(format!(
                "manifest spectra count for {} does not match SQLite",
                shard.shard_path
            ));
        }
        if row.2 != Some(expected_bytes) {
            errors.push(format!(
                "manifest byte count for {} does not match SQLite",
                shard.shard_path
            ));
        }
        if row.3.as_deref() != shard.sha256.as_deref() {
            errors.push(format!(
                "manifest SHA256 for {} does not match SQLite",
                shard.shard_path
            ));
        }
    }
    Ok(())
}

/// Validates that there are no temporary shard files left in the output directory.
fn validate_no_temporary_shards(
    output_dir: &Path,
    top_k_peaks: usize,
    errors: &mut Vec<String>,
) -> anyhow::Result<()> {
    if !output_dir.exists() {
        errors.push(format!(
            "output directory {} does not exist",
            output_dir.display()
        ));
        return Ok(());
    }
    let prefix = format!("massive_ms2.top-{top_k_peaks:04}.part-");
    for entry in fs::read_dir(output_dir)
        .with_context(|| format!("failed to read {}", output_dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read {}", output_dir.display()))?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.starts_with(&prefix)
            && path
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("tmp"))
        {
            errors.push(format!("temporary shard remains: {}", path.display()));
        }
    }
    Ok(())
}

/// Validates finalized shard files against SQLite byte counts and checksums.
fn validate_finalized_shards(
    db: &mut StateDb,
    config: &Config,
    errors: &mut Vec<String>,
) -> anyhow::Result<()> {
    let shards = db.finalized_shards(config.top_k_peaks)?;
    if shards.is_empty() {
        errors.push("no finalized MGF shards are recorded".to_owned());
    }
    for shard in shards {
        let path = PathBuf::from(&shard.shard_path);
        let expected_bytes =
            u64::try_from(shard.bytes_written).context("shard byte count does not fit u64")?;
        match fs::metadata(&path) {
            Ok(metadata) => {
                if metadata.len() != expected_bytes {
                    errors.push(format!(
                        "{} has {} bytes, SQLite expects {expected_bytes}",
                        path.display(),
                        metadata.len()
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                errors.push(format!("finalized shard is missing: {}", path.display()));
                continue;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("failed to stat {}", path.display()));
            }
        }
        if let Some(expected_sha256) = shard.sha256 {
            let actual = sha256_file(&path)?;
            if actual != expected_sha256 {
                errors.push(format!(
                    "{} has SHA256 {actual}, SQLite expects {expected_sha256}",
                    path.display()
                ));
            }
        }
    }
    Ok(())
}

/// Validates upload file sizes against the configured publication limit.
fn validate_upload_file_sizes(
    output_dir: &Path,
    max_file_bytes: u64,
    errors: &mut Vec<String>,
) -> anyhow::Result<()> {
    for path in generated_output_paths(output_dir)? {
        let size = fs::metadata(&path)
            .with_context(|| format!("failed to stat {}", path.display()))?
            .len();
        if size > max_file_bytes {
            errors.push(format!(
                "{} is {size} bytes, larger than ZENODO_MAX_FILE_BYTES={max_file_bytes}",
                path.display()
            ));
        }
    }
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

/// Builds the production Zenodo metadata.
fn publication_metadata(config: &Config) -> anyhow::Result<DepositMetadataUpdate> {
    Ok(DepositMetadataUpdate::builder()
        .title(format!(
            "Deduplicated MassIVE mzML MS/MS spectra converted to Mascot Generic Format (top-{} peaks)",
            config.top_k_peaks
        ))
        .upload_type(UploadType::Dataset)
        .publication_date(Utc::now().date_naive())
        .version(format!("top-{}-peaks", config.top_k_peaks))
        .description_html(publication_description_html(config))
        .creator(
            Creator::builder()
                .name(ZENODO_CREATOR_NAME)
                .affiliation(ZENODO_CREATOR_AFFILIATION)
                .orcid(ZENODO_CREATOR_ORCID)
                .build()?,
        )
        .community_identifier(ZENODO_COMMUNITY)
        .access_right(AccessRight::Open)
        .license(ZENODO_LICENSE)
        .keywords(publication_keywords(config))
        .related_identifiers(publication_related_identifiers()?)
        .build()?)
}

/// Builds the Zenodo HTML description.
fn publication_description_html(config: &Config) -> String {
    format!(
        "\
<p>This record contains sharded zstd-compressed Mascot Generic Format (MGF) files generated from deduplicated MassIVE mzML public open-format files indexed by the GNPS/MassIVE Public Data Index.</p>
<p>The conversion keeps MS/MS spectra, records precursor and associated scan context when available, caps each retained spectrum to the top {} fragment peaks by intensity, and deduplicates records by the pair of SPLASH and PEPMASS values.</p>
<p>The target corpus size is approximately {} retained MS/MS spectra. The record includes MGF shards, a manifest, conversion counters including observed MSn levels, and enough source metadata to trace each spectrum back to its MassIVE file path.</p>",
        config.top_k_peaks, config.target_ms2_spectra
    )
}

/// Builds Zenodo keywords.
fn publication_keywords(config: &Config) -> Vec<String> {
    let mut keywords = [
        "mass spectrometry",
        "MS/MS",
        "MGF",
        "Mascot Generic Format",
        "MassIVE",
        "GNPS",
        "mzML",
        "SPLASH",
        "spectral library",
        "deduplicated spectra",
        "Earth Metabolome Initiative",
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();
    keywords.push(format!("top-{} peaks", config.top_k_peaks));
    keywords
}

/// Builds related identifiers for source data and converter software.
fn publication_related_identifiers() -> anyhow::Result<Vec<RelatedIdentifier>> {
    Ok(vec![
        RelatedIdentifier::builder()
            .identifier(SOURCE_ZENODO_DOI)
            .relation("isDerivedFrom")
            .scheme("doi")
            .resource_type("dataset")
            .build()?,
        RelatedIdentifier::builder()
            .identifier(SOURCE_ZENODO_URL)
            .relation("isDerivedFrom")
            .scheme("url")
            .resource_type("dataset")
            .build()?,
        RelatedIdentifier::builder()
            .identifier(CONVERTER_REPOSITORY_URL)
            .relation("isCompiledBy")
            .scheme("url")
            .resource_type("software")
            .build()?,
    ])
}

/// Builds upload specifications for generated output files.
fn upload_specs(output_dir: &Path) -> anyhow::Result<Vec<UploadSpec>> {
    let mut paths = generated_output_paths(output_dir)?;
    paths.sort();
    paths
        .into_iter()
        .map(UploadSpec::from_path)
        .collect::<Result<Vec<_>, _>>()
        .context("failed to build Zenodo upload specs")
}

/// Returns all generated output files that should be published.
fn generated_output_paths(output_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(output_dir)
        .with_context(|| format!("failed to read {}", output_dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read {}", output_dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.ends_with(".mgf.zst")
            || matches!(
                name,
                "manifest.csv" | "conversion_report.json" | "README.md"
            )
        {
            paths.push(path);
        }
    }
    if paths.is_empty() {
        bail!(
            "no generated output files found in {}",
            output_dir.display()
        );
    }
    Ok(paths)
}

/// Summarizes generated output file sizes.
fn summarize_output_paths(paths: &[PathBuf]) -> anyhow::Result<(usize, u64, Option<PathBuf>, u64)> {
    let mut total_bytes = 0_u64;
    let mut largest_file = None;
    let mut largest_file_bytes = 0_u64;
    for path in paths {
        let bytes = fs::metadata(path)
            .with_context(|| format!("failed to inspect {}", path.display()))?
            .len();
        total_bytes = total_bytes.saturating_add(bytes);
        if bytes > largest_file_bytes {
            largest_file_bytes = bytes;
            largest_file = Some(path.clone());
        }
    }
    Ok((paths.len(), total_bytes, largest_file, largest_file_bytes))
}

/// Labels the deposition mode that real publication would use.
fn deposition_mode_label(config: &Config) -> String {
    config.zenodo_deposition_id.map_or_else(
        || "create new production deposition".to_owned(),
        |id| format!("replace production deposition {id}"),
    )
}

/// Builds a temporary partial-download path.
fn part_path(destination: &Path) -> PathBuf {
    let mut value = destination.as_os_str().to_owned();
    value.push(".part");
    PathBuf::from(value)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use anyhow::Context;
    use tempfile::tempdir;

    use crate::config::Config;
    use crate::db::{SourceConversionRecord, SpectrumSeenRecord, StateDb};

    use super::{publication_dry_run, sha256_file, validate_outputs_for_publication};

    /// Confirms publication is blocked when no finalized corpus exists.
    #[test]
    fn validation_rejects_empty_outputs() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        let config = test_config(tempdir.path());
        assert!(validate_outputs_for_publication(&mut db, &config).is_err());
        Ok(())
    }

    /// Confirms publication validation accepts a complete finalized tiny corpus.
    #[test]
    fn validation_accepts_complete_outputs() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let mut db = seed_complete_corpus(tempdir.path())?;
        let config = test_config(tempdir.path());
        validate_outputs_for_publication(&mut db, &config)?;
        Ok(())
    }

    /// Confirms publication dry-run accepts complete outputs without a Zenodo token.
    #[test]
    fn dry_run_accepts_complete_outputs_without_token() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let mut db = seed_complete_corpus(tempdir.path())?;
        let config = test_config(tempdir.path());
        let summary = publication_dry_run(&mut db, &config)?;
        assert!(summary.validation_passed);
        assert_eq!(summary.upload_spec_count, 4);
        assert_eq!(summary.file_count, 4);
        assert_eq!(summary.finalized_spectra, 1);
        assert_eq!(summary.top_k_peaks, 256);
        assert!(summary.total_bytes >= 3);
        Ok(())
    }

    /// Confirms publication dry-run rejects invalid local outputs.
    #[test]
    fn dry_run_rejects_invalid_outputs() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        let config = test_config(tempdir.path());
        assert!(publication_dry_run(&mut db, &config).is_err());
        Ok(())
    }

    /// Confirms conversion failures block publication even when outputs otherwise exist.
    #[test]
    fn validation_rejects_failed_conversion() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let mut db = seed_complete_corpus(tempdir.path())?;
        db.mark_conversion_failed("MSV000000001/path/bad.mzML", 256, "bad mzML")?;
        let config = test_config(tempdir.path());
        assert_validation_error_contains(&mut db, &config, "source file conversion(s) failed")?;
        Ok(())
    }

    /// Confirms missing sidecars block publication.
    #[test]
    fn validation_rejects_missing_sidecar() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let mut db = seed_complete_corpus(tempdir.path())?;
        std::fs::remove_file(tempdir.path().join("README.md"))?;
        let config = test_config(tempdir.path());
        assert_validation_error_contains(&mut db, &config, "required sidecar is missing")?;
        Ok(())
    }

    /// Confirms manifest byte-count mismatches block publication.
    #[test]
    fn validation_rejects_manifest_byte_mismatch() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let mut db = seed_complete_corpus(tempdir.path())?;
        let shard = tempdir
            .path()
            .join("massive_ms2.top-0256.part-000000.mgf.zst");
        let sha256 = sha256_file(&shard)?;
        std::fs::write(
            tempdir.path().join("manifest.csv"),
            format!(
                "shard_index,path,spectra_written,bytes_written,sha256\n0,{},1,4,{sha256}\n",
                shard.display()
            ),
        )?;
        let config = test_config(tempdir.path());
        assert_validation_error_contains(&mut db, &config, "manifest byte count")?;
        Ok(())
    }

    /// Confirms leftover temporary shards block publication.
    #[test]
    fn validation_rejects_temporary_shard() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let mut db = seed_complete_corpus(tempdir.path())?;
        std::fs::write(
            tempdir
                .path()
                .join("massive_ms2.top-0256.part-000001.mgf.zst.tmp"),
            b"partial",
        )?;
        let config = test_config(tempdir.path());
        assert_validation_error_contains(&mut db, &config, "temporary shard remains")?;
        Ok(())
    }

    /// Confirms oversized upload files block publication.
    #[test]
    fn validation_rejects_oversized_upload_file() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let mut db = seed_complete_corpus(tempdir.path())?;
        let mut config = test_config(tempdir.path());
        config.zenodo_max_file_bytes = 2;
        assert_validation_error_contains(&mut db, &config, "larger than ZENODO_MAX_FILE_BYTES")?;
        Ok(())
    }

    /// Builds a complete finalized tiny corpus fixture.
    fn seed_complete_corpus(root: &Path) -> anyhow::Result<StateDb> {
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
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
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
        )?;
        Ok(db)
    }

    /// Asserts that publication validation fails with a specific message.
    fn assert_validation_error_contains(
        db: &mut StateDb,
        config: &Config,
        needle: &str,
    ) -> anyhow::Result<()> {
        let error = validate_outputs_for_publication(db, config)
            .err()
            .context("expected publication validation to fail")?;
        let text = error.to_string();
        assert!(text.contains(needle), "{text}");
        Ok(())
    }

    /// Builds a minimal test configuration rooted in a temporary directory.
    fn test_config(root: &std::path::Path) -> Config {
        Config {
            openformats_record_id: 4_549_746,
            openformats_index_dir: root.join("openformats"),
            database_url: ":memory:".to_owned(),
            mzml_download_dir: root.join("mzml"),
            mgf_output_dir: root.to_path_buf(),
            target_ms2_spectra: 1,
            top_k_peaks: 256,
            mgf_shard_max_spectra: 1_000,
            source_selection_buffer: 1.25,
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
