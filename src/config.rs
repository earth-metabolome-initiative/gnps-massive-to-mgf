//! Environment-backed runtime configuration.

use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail};

/// Default Zenodo record containing the open-format index TSV files.
const DEFAULT_OPENFORMATS_RECORD_ID: u64 = 4_549_746;
/// Default directory for cached index TSV files.
const DEFAULT_INDEX_DIR: &str = "data/openformats";
/// Default SQLite state database.
const DEFAULT_DATABASE_URL: &str = "data/gnps_massive_to_mgf.sqlite";
/// Default raw mzML download directory.
const DEFAULT_MZML_DOWNLOAD_DIR: &str = "/mnt/bfd/mzml";
/// Default generated MGF output directory.
const DEFAULT_MGF_OUTPUT_DIR: &str = "/mnt/bfd/mgf";
/// Default target number of retained MS/MS spectra.
const DEFAULT_TARGET_MS2_SPECTRA: u64 = 200_000_000;
/// Default number of fragment peaks retained per MS/MS spectrum.
const DEFAULT_TOP_K_PEAKS: usize = 256;
/// Default number of spectra written per compressed MGF shard.
const DEFAULT_MGF_SHARD_MAX_SPECTRA: u64 = 1_000_000;
/// Default concurrent MassIVE download worker count.
const DEFAULT_DOWNLOAD_WORKERS: usize = 1;
/// Default number of retry attempts for a source file download.
const DEFAULT_DOWNLOAD_RETRY_ATTEMPTS: usize = 5;
/// Default delay between retryable download failures.
const DEFAULT_DOWNLOAD_RETRY_DELAY_SECONDS: u64 = 30;
/// Default overall HTTP request timeout.
const DEFAULT_HTTP_REQUEST_TIMEOUT_SECONDS: u64 = 600;
/// Default HTTP connect timeout.
const DEFAULT_HTTP_CONNECT_TIMEOUT_SECONDS: u64 = 30;
/// Default source selection buffer over the requested final unique target.
const DEFAULT_SOURCE_SELECTION_BUFFER: f64 = 1.25;
/// Default extra indexed MS/MS spectra promoted when the unique target is short.
const DEFAULT_SOURCE_SELECTION_CHUNK_MS2: u64 = 50_000_000;
/// Default maximum upload file size for Zenodo publication.
const DEFAULT_ZENODO_MAX_FILE_BYTES: u64 = 50_000_000_000;

/// Runtime configuration for the MassIVE mzML to MGF pipeline.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Config {
    /// Zenodo record containing the open-format index files.
    pub openformats_record_id: u64,
    /// Directory where the index TSV files are cached.
    pub openformats_index_dir: PathBuf,
    /// SQLite state database URL/path.
    pub database_url: String,
    /// Directory where raw mzML files are downloaded.
    pub mzml_download_dir: PathBuf,
    /// Directory where generated MGF shards and sidecars are written.
    pub mgf_output_dir: PathBuf,
    /// Target number of retained MS/MS spectra.
    pub target_ms2_spectra: u64,
    /// Maximum number of fragment peaks retained per MS/MS spectrum.
    pub top_k_peaks: usize,
    /// Number of spectra per compressed MGF shard.
    pub mgf_shard_max_spectra: u64,
    /// Source-selection multiplier applied to the requested final unique target.
    pub source_selection_buffer: f64,
    /// Indexed MS/MS spectra promoted when converted unique spectra are short.
    pub source_selection_chunk_ms2: u64,
    /// Concurrent source-file download worker count.
    pub download_workers: usize,
    /// Number of retry attempts for each source-file download.
    pub download_retry_attempts: usize,
    /// Delay between retryable download attempts.
    pub download_retry_delay: Duration,
    /// HTTP request timeout; `None` disables the full-transfer timeout.
    pub http_request_timeout: Option<Duration>,
    /// HTTP connect timeout.
    pub http_connect_timeout: Duration,
    /// Whether production Zenodo publication should run.
    pub publish_to_zenodo: bool,
    /// Existing production Zenodo deposition id, when configured.
    pub zenodo_deposition_id: Option<u64>,
    /// Maximum per-file upload size accepted before publishing.
    pub zenodo_max_file_bytes: u64,
}

impl Config {
    /// Builds runtime configuration from process environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error when numeric variables are malformed or non-positive
    /// where a positive value is required.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_env_reader(&|name| match env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(env::VarError::NotPresent) => Ok(None),
            Err(source) => Err(source).with_context(|| format!("failed to read {name}")),
        })
    }

    fn from_env_reader(
        read_env: &impl Fn(&str) -> anyhow::Result<Option<String>>,
    ) -> anyhow::Result<Self> {
        let download_workers = parse_usize(read_env, "DOWNLOAD_WORKERS", DEFAULT_DOWNLOAD_WORKERS)?;
        if download_workers == 0 {
            bail!("DOWNLOAD_WORKERS must be positive");
        }

        let download_retry_attempts = parse_usize(
            read_env,
            "DOWNLOAD_RETRY_ATTEMPTS",
            DEFAULT_DOWNLOAD_RETRY_ATTEMPTS,
        )?;
        if download_retry_attempts == 0 {
            bail!("DOWNLOAD_RETRY_ATTEMPTS must be positive");
        }

        let top_k_peaks = parse_usize(read_env, "TOP_K_PEAKS", DEFAULT_TOP_K_PEAKS)?;
        if top_k_peaks == 0 {
            bail!("TOP_K_PEAKS must be positive");
        }

        let mgf_shard_max_spectra = parse_u64(
            read_env,
            "MGF_SHARD_MAX_SPECTRA",
            DEFAULT_MGF_SHARD_MAX_SPECTRA,
        )?;
        if mgf_shard_max_spectra == 0 {
            bail!("MGF_SHARD_MAX_SPECTRA must be positive");
        }

        let source_selection_buffer = parse_f64(
            read_env,
            "SOURCE_SELECTION_BUFFER",
            DEFAULT_SOURCE_SELECTION_BUFFER,
        )?;
        if !(source_selection_buffer.is_finite() && source_selection_buffer >= 1.0) {
            bail!("SOURCE_SELECTION_BUFFER must be finite and at least 1.0");
        }

        let source_selection_chunk_ms2 = parse_u64(
            read_env,
            "SOURCE_SELECTION_CHUNK_MS2",
            DEFAULT_SOURCE_SELECTION_CHUNK_MS2,
        )?;
        if source_selection_chunk_ms2 == 0 {
            bail!("SOURCE_SELECTION_CHUNK_MS2 must be positive");
        }

        let zenodo_max_file_bytes = parse_u64(
            read_env,
            "ZENODO_MAX_FILE_BYTES",
            DEFAULT_ZENODO_MAX_FILE_BYTES,
        )?;
        if zenodo_max_file_bytes == 0 {
            bail!("ZENODO_MAX_FILE_BYTES must be positive");
        }

        let http_request_timeout_seconds = parse_u64(
            read_env,
            "HTTP_REQUEST_TIMEOUT_SECONDS",
            DEFAULT_HTTP_REQUEST_TIMEOUT_SECONDS,
        )?;

        Ok(Self {
            openformats_record_id: parse_u64(
                read_env,
                "OPENFORMATS_RECORD_ID",
                DEFAULT_OPENFORMATS_RECORD_ID,
            )?,
            openformats_index_dir: parse_path(
                read_env,
                "OPENFORMATS_INDEX_DIR",
                DEFAULT_INDEX_DIR,
            )?,
            database_url: parse_string(read_env, "STATE_DATABASE_URL", DEFAULT_DATABASE_URL)?,
            mzml_download_dir: parse_path(
                read_env,
                "MZML_DOWNLOAD_DIR",
                DEFAULT_MZML_DOWNLOAD_DIR,
            )?,
            mgf_output_dir: parse_path(read_env, "MGF_OUTPUT_DIR", DEFAULT_MGF_OUTPUT_DIR)?,
            target_ms2_spectra: parse_u64(
                read_env,
                "TARGET_MS2_SPECTRA",
                DEFAULT_TARGET_MS2_SPECTRA,
            )?,
            top_k_peaks,
            mgf_shard_max_spectra,
            source_selection_buffer,
            source_selection_chunk_ms2,
            download_workers,
            download_retry_attempts,
            download_retry_delay: Duration::from_secs(parse_u64(
                read_env,
                "DOWNLOAD_RETRY_DELAY_SECONDS",
                DEFAULT_DOWNLOAD_RETRY_DELAY_SECONDS,
            )?),
            http_request_timeout: (http_request_timeout_seconds > 0)
                .then(|| Duration::from_secs(http_request_timeout_seconds)),
            http_connect_timeout: Duration::from_secs(parse_u64(
                read_env,
                "HTTP_CONNECT_TIMEOUT_SECONDS",
                DEFAULT_HTTP_CONNECT_TIMEOUT_SECONDS,
            )?),
            publish_to_zenodo: env_present(read_env, "ZENODO_TOKEN")?,
            zenodo_deposition_id: parse_optional_u64(read_env, "ZENODO_DEPOSITION_ID")?,
            zenodo_max_file_bytes,
        })
    }

    /// Returns the cached GNPS open-format TSV path.
    #[must_use]
    pub fn gnps_index_path(&self) -> PathBuf {
        self.openformats_index_dir
            .join("gnps_public_openformats.tsv")
    }

    /// Returns the cached MassIVE open-format TSV path.
    #[must_use]
    pub fn massive_index_path(&self) -> PathBuf {
        self.openformats_index_dir
            .join("massive_public_openformats.tsv")
    }

    /// Returns the output path for the conversion manifest.
    #[must_use]
    pub fn manifest_path(&self) -> PathBuf {
        self.mgf_output_dir.join("manifest.csv")
    }

    /// Returns the output path for the conversion summary.
    #[must_use]
    pub fn conversion_report_path(&self) -> PathBuf {
        self.mgf_output_dir.join("conversion_report.json")
    }
}

/// Parses an environment variable as a `u64`, falling back to a default.
fn parse_u64(
    read_env: &impl Fn(&str) -> anyhow::Result<Option<String>>,
    name: &str,
    default: u64,
) -> anyhow::Result<u64> {
    match read_env(name)? {
        Some(value) if !value.trim().is_empty() => value
            .trim()
            .parse::<u64>()
            .with_context(|| format!("failed to parse {name} as u64")),
        Some(_) | None => Ok(default),
    }
}

/// Parses an environment variable as a `usize`, falling back to a default.
fn parse_usize(
    read_env: &impl Fn(&str) -> anyhow::Result<Option<String>>,
    name: &str,
    default: usize,
) -> anyhow::Result<usize> {
    match read_env(name)? {
        Some(value) if !value.trim().is_empty() => value
            .trim()
            .parse::<usize>()
            .with_context(|| format!("failed to parse {name} as usize")),
        Some(_) | None => Ok(default),
    }
}

/// Parses an environment variable as an `f64`, falling back to a default.
fn parse_f64(
    read_env: &impl Fn(&str) -> anyhow::Result<Option<String>>,
    name: &str,
    default: f64,
) -> anyhow::Result<f64> {
    match read_env(name)? {
        Some(value) if !value.trim().is_empty() => value
            .trim()
            .parse::<f64>()
            .with_context(|| format!("failed to parse {name} as f64")),
        Some(_) | None => Ok(default),
    }
}

/// Parses an optional `u64` environment variable.
fn parse_optional_u64(
    read_env: &impl Fn(&str) -> anyhow::Result<Option<String>>,
    name: &str,
) -> anyhow::Result<Option<u64>> {
    match read_env(name)? {
        Some(value) if !value.trim().is_empty() => value
            .trim()
            .parse::<u64>()
            .map(Some)
            .with_context(|| format!("failed to parse {name} as u64")),
        Some(_) | None => Ok(None),
    }
}

/// Parses an environment variable as a string, falling back to a default.
fn parse_string(
    read_env: &impl Fn(&str) -> anyhow::Result<Option<String>>,
    name: &str,
    default: &str,
) -> anyhow::Result<String> {
    Ok(read_env(name)?
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_owned()))
}

/// Parses an environment variable as a path, falling back to a default.
fn parse_path(
    read_env: &impl Fn(&str) -> anyhow::Result<Option<String>>,
    name: &str,
    default: &str,
) -> anyhow::Result<PathBuf> {
    Ok(Path::new(&parse_string(read_env, name, default)?).to_path_buf())
}

/// Returns whether an environment variable is present and non-empty.
fn env_present(
    read_env: &impl Fn(&str) -> anyhow::Result<Option<String>>,
    name: &str,
) -> anyhow::Result<bool> {
    Ok(read_env(name)?.is_some_and(|value| !value.is_empty()))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use anyhow::Context;

    use super::Config;

    /// Confirms default configuration values parse without environment input.
    #[test]
    fn parses_default_values() -> anyhow::Result<()> {
        let config = config_from_pairs(&[])?;
        assert_eq!(config.mzml_download_dir, PathBuf::from("/mnt/bfd/mzml"));
        assert_eq!(config.mgf_output_dir, PathBuf::from("/mnt/bfd/mgf"));
        assert_eq!(config.target_ms2_spectra, 200_000_000);
        assert_eq!(config.top_k_peaks, 256);
        assert_eq!(config.http_request_timeout, Some(Duration::from_mins(10)));
        assert!(!config.publish_to_zenodo);
        Ok(())
    }

    /// Confirms zero disables the full-transfer HTTP request timeout.
    #[test]
    fn zero_request_timeout_disables_full_transfer_timeout() -> anyhow::Result<()> {
        let config = config_from_pairs(&[("HTTP_REQUEST_TIMEOUT_SECONDS", "0")])?;
        assert_eq!(config.http_request_timeout, None);
        Ok(())
    }

    /// Confirms source selection buffer values below one are rejected.
    #[test]
    fn rejects_source_selection_buffer_below_one() -> anyhow::Result<()> {
        let error = config_from_pairs(&[("SOURCE_SELECTION_BUFFER", "0.99")])
            .err()
            .context("expected SOURCE_SELECTION_BUFFER to be rejected")?;
        assert!(error.to_string().contains("SOURCE_SELECTION_BUFFER"));
        Ok(())
    }

    /// Confirms zero-valued positive knobs are rejected.
    #[test]
    fn rejects_zero_top_k_peaks() -> anyhow::Result<()> {
        let error = config_from_pairs(&[("TOP_K_PEAKS", "0")])
            .err()
            .context("expected TOP_K_PEAKS to be rejected")?;
        assert!(error.to_string().contains("TOP_K_PEAKS"));
        Ok(())
    }

    /// Builds configuration from a small test environment map.
    fn config_from_pairs(pairs: &[(&str, &str)]) -> anyhow::Result<Config> {
        Config::from_env_reader(&|name| {
            Ok(pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned()))
        })
    }
}
