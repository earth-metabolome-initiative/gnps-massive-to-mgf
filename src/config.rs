//! Environment-backed runtime configuration.

use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail};

/// Default Zenodo record containing the open-format index TSV files.
const DEFAULT_OPENFORMATS_RECORD_ID: u64 = 4_549_746;
/// Default directory for cached index TSV files.
const DEFAULT_INDEX_DIR: &str = "data/openformats";
/// Default dataset-level manifest mapping MSV accessions to FTPS volume buckets.
const DEFAULT_MASSIVE_VOLUMES_PATH: &str = "public_MSVs_2026_0615_0812.tsv";
/// Default SQLite state database.
const DEFAULT_DATABASE_URL: &str = "data/massive.sqlite";
/// Default raw mzML download directory.
const DEFAULT_MZML_DOWNLOAD_DIR: &str = "/mnt/bfd/mzml";
/// Default generated MGF output directory.
const DEFAULT_MGF_OUTPUT_DIR: &str = "/mnt/bfd/mgf";
/// Default target number of retained MS/MS spectra.
const DEFAULT_TARGET_MS2_SPECTRA: u64 = 1_000_000_000;
/// Default number of fragment peaks retained per MS/MS spectrum.
const DEFAULT_TOP_K_PEAKS: usize = 256;
/// Default number of spectra written per compressed MGF shard.
const DEFAULT_MGF_SHARD_MAX_SPECTRA: u64 = 1_000_000;
/// Default number of retry attempts for a source file download.
const DEFAULT_DOWNLOAD_RETRY_ATTEMPTS: usize = 1;
/// Default delay between retryable download failures.
const DEFAULT_DOWNLOAD_RETRY_DELAY_SECONDS: u64 = 30;
/// Default FTPS host for MassIVE bulk downloads.
const DEFAULT_FTPS_HOST: &str = "massive-ftp.ucsd.edu";
/// Default FTPS control-channel port.
const DEFAULT_FTPS_PORT: u16 = 21;
/// Default path the TOFU cert pin is persisted at.
const DEFAULT_FTPS_CERT_PIN_PATH: &str = "data/ftps_cert_pin.txt";
/// Default FTPS connect timeout.
const DEFAULT_FTPS_CONNECT_TIMEOUT_SECONDS: u64 = 30;
/// Default FTPS transfer (per-file) timeout.
const DEFAULT_FTPS_TRANSFER_TIMEOUT_SECONDS: u64 = 600;
/// Default FTPS per-chunk read timeout. A stall longer than this fails the
/// current attempt so the downloader can REST-resume from the `.part` file
/// instead of waiting out the whole-transfer timeout.
const DEFAULT_FTPS_CHUNK_READ_TIMEOUT_SECONDS: u64 = 15;
/// Default cooldown applied to `download_failed` rows before they are eligible
/// to retry. Keeps a `download` invocation from immediately reattempting files
/// that just failed (likely server-side broken or in a per-IP cool-down).
const DEFAULT_DOWNLOAD_FAILURE_COOLDOWN_SECONDS: u64 = 604_800;
/// Default keyword tokens that mark a dataset as worth indexing first. Each
/// token is a lowercase substring matched against the manifest's `keywords`
/// column. Datasets whose keywords match any token are picked into the
/// `indexed` set before non-matching datasets, so the target-MS2 cap fills
/// with metabolomics / natural-product data before alphabetically-first
/// proteomics files.
const DEFAULT_INGEST_KEYWORD_ALLOWLIST: &[&str] = &[
    "metabolomics",
    "natural products",
    "lipidomics",
    "untargeted metabolomics",
    "datasettype:metabolomics",
];
/// Default source selection buffer over the requested final unique target.
const DEFAULT_SOURCE_SELECTION_BUFFER: f64 = 1.25;
/// Default extra indexed MS/MS spectra promoted when the unique target is short.
const DEFAULT_SOURCE_SELECTION_CHUNK_MS2: u64 = 50_000_000;

/// Runtime configuration for the MassIVE mzML to MGF pipeline.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Config {
    /// Zenodo record containing the open-format index files.
    pub openformats_record_id: u64,
    /// Directory where the index TSV files are cached.
    pub openformats_index_dir: PathBuf,
    /// Path to the dataset-level manifest mapping MSV accessions to FTPS
    /// volume buckets (e.g. `v01`, `x01`).
    ///
    /// Consumed by [`crate::index::ingest_volume_manifest`] to populate
    /// `source_files.volume`, which the FTPS downloader needs to construct
    /// absolute server paths under `massive-ftp.ucsd.edu`.
    pub massive_volumes_path: PathBuf,
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
    /// Number of retry attempts for each source-file download.
    pub download_retry_attempts: usize,
    /// Delay between retryable download attempts.
    pub download_retry_delay: Duration,
    /// FTPS host for MassIVE bulk downloads.
    pub ftps_host: String,
    /// FTPS control-channel port.
    pub ftps_port: u16,
    /// Path the TOFU cert pin is persisted at; see [`crate::ftps_tofu`].
    pub ftps_cert_pin_path: PathBuf,
    /// FTPS connect timeout.
    pub ftps_connect_timeout: Duration,
    /// FTPS per-file transfer timeout; `None` disables it.
    pub ftps_transfer_timeout: Option<Duration>,
    /// FTPS per-chunk read timeout applied around each
    /// `data.read(&mut buf).await` inside the transfer loop. `None` disables
    /// it. Keeps a silent data-channel stall from waiting out the much larger
    /// `ftps_transfer_timeout`.
    pub ftps_chunk_read_timeout: Option<Duration>,
    /// Cooldown applied to `download_failed` rows before they are re-queued
    /// on the next `download` run. Recently-failed files skip the retry until
    /// at least this duration has elapsed since their last attempt.
    pub download_failure_cooldown: Duration,
    /// Lowercase keyword tokens that prioritize a dataset during ingest. The
    /// list is matched (substring, case-insensitive) against the manifest's
    /// `keywords` column. Matching datasets fill the `indexed` set first; the
    /// rest fall to `candidate` once the target-MS2 cap is hit.
    pub ingest_keyword_allowlist: Vec<String>,
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

    #[allow(
        clippy::too_many_lines,
        reason = "all knobs are individually trivial and grouped here on purpose; \
                  splitting into per-section helpers would add indirection without clarity"
    )]
    fn from_env_reader(
        read_env: &impl Fn(&str) -> anyhow::Result<Option<String>>,
    ) -> anyhow::Result<Self> {
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

        let ftps_transfer_timeout_seconds = parse_u64(
            read_env,
            "FTPS_TRANSFER_TIMEOUT_SECONDS",
            DEFAULT_FTPS_TRANSFER_TIMEOUT_SECONDS,
        )?;
        let ftps_chunk_read_timeout_seconds = parse_u64(
            read_env,
            "FTPS_CHUNK_READ_TIMEOUT_SECONDS",
            DEFAULT_FTPS_CHUNK_READ_TIMEOUT_SECONDS,
        )?;
        let download_failure_cooldown_seconds = parse_u64(
            read_env,
            "DOWNLOAD_FAILURE_COOLDOWN_SECONDS",
            DEFAULT_DOWNLOAD_FAILURE_COOLDOWN_SECONDS,
        )?;
        let ingest_keyword_allowlist =
            parse_keyword_allowlist(read_env, "INGEST_KEYWORD_ALLOWLIST")?;
        let ftps_port_value = parse_u64(read_env, "FTPS_PORT", u64::from(DEFAULT_FTPS_PORT))?;
        let ftps_port = u16::try_from(ftps_port_value)
            .with_context(|| format!("FTPS_PORT {ftps_port_value} does not fit u16"))?;
        if ftps_port == 0 {
            bail!("FTPS_PORT must be positive");
        }

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
            massive_volumes_path: parse_path(
                read_env,
                "MASSIVE_VOLUMES_PATH",
                DEFAULT_MASSIVE_VOLUMES_PATH,
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
            download_retry_attempts,
            download_retry_delay: Duration::from_secs(parse_u64(
                read_env,
                "DOWNLOAD_RETRY_DELAY_SECONDS",
                DEFAULT_DOWNLOAD_RETRY_DELAY_SECONDS,
            )?),
            ftps_host: parse_string(read_env, "FTPS_HOST", DEFAULT_FTPS_HOST)?,
            ftps_port,
            ftps_cert_pin_path: parse_path(
                read_env,
                "FTPS_CERT_PIN_PATH",
                DEFAULT_FTPS_CERT_PIN_PATH,
            )?,
            ftps_connect_timeout: Duration::from_secs(parse_u64(
                read_env,
                "FTPS_CONNECT_TIMEOUT_SECONDS",
                DEFAULT_FTPS_CONNECT_TIMEOUT_SECONDS,
            )?),
            ftps_transfer_timeout: (ftps_transfer_timeout_seconds > 0)
                .then(|| Duration::from_secs(ftps_transfer_timeout_seconds)),
            ftps_chunk_read_timeout: (ftps_chunk_read_timeout_seconds > 0)
                .then(|| Duration::from_secs(ftps_chunk_read_timeout_seconds)),
            download_failure_cooldown: Duration::from_secs(download_failure_cooldown_seconds),
            ingest_keyword_allowlist,
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

/// Parses a comma-separated keyword allowlist, falling back to
/// [`DEFAULT_INGEST_KEYWORD_ALLOWLIST`]. Empty entries are dropped and every
/// remaining entry is lowercased and trimmed once so downstream matching can
/// skip case folding.
fn parse_keyword_allowlist(
    read_env: &impl Fn(&str) -> anyhow::Result<Option<String>>,
    name: &str,
) -> anyhow::Result<Vec<String>> {
    let raw = read_env(name)?;
    let source = raw.as_deref().unwrap_or("");
    let mut tokens: Vec<String> = source
        .split(',')
        .map(|token| token.trim().to_ascii_lowercase())
        .filter(|token| !token.is_empty())
        .collect();
    if tokens.is_empty() && raw.is_none() {
        tokens = DEFAULT_INGEST_KEYWORD_ALLOWLIST
            .iter()
            .map(|token| (*token).to_owned())
            .collect();
    }
    Ok(tokens)
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
        assert_eq!(
            config.massive_volumes_path,
            PathBuf::from("public_MSVs_2026_0615_0812.tsv"),
        );
        assert_eq!(config.target_ms2_spectra, 1_000_000_000);
        assert_eq!(
            config.ingest_keyword_allowlist,
            vec![
                "metabolomics".to_owned(),
                "natural products".to_owned(),
                "lipidomics".to_owned(),
                "untargeted metabolomics".to_owned(),
                "datasettype:metabolomics".to_owned(),
            ],
        );
        assert_eq!(config.top_k_peaks, 256);
        assert_eq!(config.ftps_host, "massive-ftp.ucsd.edu");
        assert_eq!(config.ftps_port, 21);
        assert_eq!(
            config.ftps_cert_pin_path,
            PathBuf::from("data/ftps_cert_pin.txt"),
        );
        assert_eq!(config.ftps_transfer_timeout, Some(Duration::from_mins(10)));
        Ok(())
    }

    /// Confirms zero disables the full-transfer FTPS timeout.
    #[test]
    fn zero_transfer_timeout_disables_full_transfer_timeout() -> anyhow::Result<()> {
        let config = config_from_pairs(&[("FTPS_TRANSFER_TIMEOUT_SECONDS", "0")])?;
        assert_eq!(config.ftps_transfer_timeout, None);
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
