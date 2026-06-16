//! MassIVE source-file download helpers (anonymous FTPS).
//!
//! MassIVE designates `massive-ftp.ucsd.edu` (port 21, FTPS explicit mode) as
//! the supported bulk-download path for public data. The HTTPS endpoint at
//! `massive.ucsd.edu/ProteoSAFe/DownloadResultFile` is an internal webapp
//! helper, not a download API; using it at scale risks an IP-level ban via
//! `mod_qos`. This module talks to the documented FTPS endpoint instead.
//!
//! Downloads run strictly sequentially. The server fair-shares bandwidth
//! across concurrent connections from the same IP and additionally throttles
//! bursts, so adding workers does not raise aggregate throughput and risks
//! triggering an abuser cool-down that degrades every subsequent connection.
//!
//! Progress is reported by writing [`ProgressEvent`]s to a [`ProgressSink`];
//! the consumer (indicatif adapter or `ratatui` TUI) decides how to render.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use suppaftp::tokio::{AsyncRustlsConnector, AsyncRustlsFtpStream};
use suppaftp::tokio_rustls::TlsConnector;
use suppaftp::types::FileType;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::Config;
use crate::db::{SourceFileRow, StateDb};
use crate::ftps_tofu::{TofuCertVerifier, rustls_config};
use crate::index::SourceFileStatus;
use crate::progress_sink::{ProgressEvent, ProgressSink};

/// FTPS client for MassIVE anonymous downloads.
///
/// Cheap to clone; the TLS config and host string sit behind shared pointers.
#[derive(Clone)]
pub struct FtpsDownloadBackend {
    /// FTPS host (typically `massive-ftp.ucsd.edu`).
    host: Arc<str>,
    /// FTPS control-channel port.
    port: u16,
    /// Connect timeout for the initial TCP+TLS handshake.
    connect_timeout: Duration,
    /// Optional per-file transfer timeout; `None` disables it.
    transfer_timeout: Option<Duration>,
    /// rustls `ClientConfig` backed by the TOFU cert verifier.
    tls_config: Arc<rustls::ClientConfig>,
}

impl FtpsDownloadBackend {
    /// Builds a backend from runtime configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the rustls `ClientConfig` cannot be constructed
    /// (typically only when the rustls build lacks both TLS 1.2 and 1.3).
    pub fn from_config(config: &Config) -> anyhow::Result<Self> {
        let verifier = TofuCertVerifier::new(config.ftps_cert_pin_path.clone());
        let tls_config = rustls_config(verifier).context("failed to build FTPS TLS config")?;
        Ok(Self {
            host: Arc::from(config.ftps_host.as_str()),
            port: config.ftps_port,
            connect_timeout: config.ftps_connect_timeout,
            transfer_timeout: config.ftps_transfer_timeout,
            tls_config: Arc::new(tls_config),
        })
    }

    /// Opens a fresh authenticated FTPS session ready for binary transfer.
    async fn open(&self) -> anyhow::Result<AsyncRustlsFtpStream> {
        let connector = TlsConnector::from(Arc::clone(&self.tls_config));
        let host = &*self.host;
        let port = self.port;
        let stream = tokio::time::timeout(
            self.connect_timeout,
            AsyncRustlsFtpStream::connect((host, port)),
        )
        .await
        .with_context(|| format!("FTPS connect to {host}:{port} timed out"))?
        .with_context(|| format!("failed to connect to {host}:{port}"))?;
        let mut stream = stream
            .into_secure(AsyncRustlsConnector::from(connector), host)
            .await
            .context("failed to upgrade FTPS control channel to TLS")?;
        stream
            .login("anonymous", "anonymous@example.com")
            .await
            .context("anonymous FTPS login was refused")?;
        stream
            .transfer_type(FileType::Binary)
            .await
            .context("failed to set binary transfer type")?;
        Ok(stream)
    }
}

/// Downloads pending mzML files into the configured raw download directory.
///
/// Files are downloaded sequentially via anonymous FTPS. Each file is written
/// to a `.part` path first, byte-count validated, then atomically renamed.
/// Failed files are retried up to `Config::download_retry_attempts` times with
/// `Config::download_retry_delay` between attempts.
///
/// # Errors
///
/// Returns an error if the database query fails.
pub async fn download_pending_mzml(
    db: &mut StateDb,
    config: &Config,
    sink: &ProgressSink,
) -> anyhow::Result<()> {
    let backend = FtpsDownloadBackend::from_config(config)?;
    download_pending_mzml_with_backend(db, config, sink, backend).await
}

/// Downloads pending mzML files using the supplied FTPS backend.
///
/// Split out from [`download_pending_mzml`] so unit tests can hand in a
/// backend that targets an in-process FTPS mock.
///
/// # Errors
///
/// Returns an error if a database query fails or the loop cannot complete a
/// download retry cycle.
pub async fn download_pending_mzml_with_backend(
    db: &mut StateDb,
    config: &Config,
    sink: &ProgressSink,
    backend: FtpsDownloadBackend,
) -> anyhow::Result<()> {
    let reset = db.reset_download_failures()?;
    if reset > 0 {
        sink.log(format!("retrying {reset} previously failed download(s)"));
    }
    let missing_volume = db.count_missing_volume()?;
    if missing_volume > 0 {
        sink.log(format!(
            "warning: {missing_volume} planned download(s) lack an FTPS volume mapping and will fail fast"
        ));
    }
    let totals = db.download_progress_totals()?;
    let mut remaining = db.count_status(SourceFileStatus::Indexed)?;
    let mut downloaded = totals.completed_files;
    let mut failed = 0_u64;
    sink.log(format!(
        "download queue: {remaining} of {} planned mzML file(s) remaining (single worker, FTPS)",
        totals.planned_files,
    ));
    sink.send(ProgressEvent::DownloadStarted {
        totals,
        remaining_indexed: remaining,
    });

    while remaining > 0 {
        let batch = db.pending_downloads(1)?;
        let Some(source) = batch.into_iter().next() else {
            break;
        };
        sink.send(ProgressEvent::FileStarted {
            source: Box::new(source.clone()),
        });
        match download_with_retries(&backend, config, sink, &source).await {
            Ok(bytes) => {
                downloaded = downloaded.saturating_add(1);
                db.mark_downloaded(&source.filepath, bytes)?;
                sink.send(ProgressEvent::FileFinished {
                    local_path: source.local_path_buf(),
                    bytes,
                });
            }
            Err(error) => {
                failed = failed.saturating_add(1);
                let message = format!("{error:#}");
                db.mark_download_failed(&source.filepath, &message)?;
                sink.send(ProgressEvent::FileFailed {
                    filepath: source.filepath.clone(),
                    error: message,
                });
            }
        }
        remaining = db.count_status(SourceFileStatus::Indexed)?;
    }
    sink.send(ProgressEvent::DownloadFinished { downloaded, failed });
    Ok(())
}

/// Downloads one source file with retries.
async fn download_with_retries(
    backend: &FtpsDownloadBackend,
    config: &Config,
    sink: &ProgressSink,
    source: &SourceFileRow,
) -> anyhow::Result<u64> {
    if existing_file_is_complete(source)? {
        let bytes = u64::try_from(source.size_bytes).context("negative source size")?;
        sink.send(ProgressEvent::FileAlreadyPresent { bytes });
        sink.log(format!("already downloaded: {}", source.filepath));
        return Ok(bytes);
    }

    let mut last_error = None;
    for attempt in 1..=config.download_retry_attempts {
        match download_once(backend, sink, source, attempt).await {
            Ok(bytes) => return Ok(bytes),
            Err(error) => {
                last_error = Some(error);
                if attempt < config.download_retry_attempts {
                    tokio::time::sleep(config.download_retry_delay).await;
                }
            }
        }
    }

    let message = last_error.map_or_else(
        || "download failed without an error".to_owned(),
        |error| format!("{error:#}"),
    );
    bail!("{message}")
}

/// Downloads one source file once via FTPS.
#[allow(
    clippy::too_many_lines,
    reason = "single procedural transfer (path setup, connect, resume, stream, validate, rename); splitting it would add indirection without clarity"
)]
async fn download_once(
    backend: &FtpsDownloadBackend,
    sink: &ProgressSink,
    source: &SourceFileRow,
    attempt: usize,
) -> anyhow::Result<u64> {
    let destination = source.local_path_buf();
    let expected_bytes = u64::try_from(source.size_bytes).context("negative source size")?;
    let part_path = part_path(&destination);
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let existing_part = partial_size(&part_path)?;
    if existing_part == expected_bytes {
        fs::rename(&part_path, &destination).with_context(|| {
            format!(
                "failed to rename complete partial {} to {}",
                part_path.display(),
                destination.display()
            )
        })?;
        sink.send(ProgressEvent::AttemptStarted {
            attempt,
            expected_bytes,
            resume_offset: expected_bytes,
        });
        sink.send(ProgressEvent::AttemptFinished);
        return Ok(expected_bytes);
    }

    let volume = source
        .volume
        .as_deref()
        .with_context(|| format!("source file {} has no FTPS volume mapping", source.filepath))?;
    let server_path = format!("/{volume}/{}", source.filepath);

    let mut stream = backend.open().await?;
    let can_resume = existing_part > 0 && existing_part < expected_bytes;
    let resume_accepted = if can_resume {
        let offset = usize::try_from(existing_part)
            .with_context(|| format!("REST offset {existing_part} does not fit usize"))?;
        match stream.resume_transfer(offset).await {
            Ok(()) => true,
            Err(error) => {
                sink.log(format!(
                    "FTPS did not accept REST resume for {} ({error}); restarting from zero",
                    source.filepath
                ));
                false
            }
        }
    } else {
        false
    };

    let starting_bytes = if resume_accepted { existing_part } else { 0 };
    sink.send(ProgressEvent::AttemptStarted {
        attempt,
        expected_bytes,
        resume_offset: starting_bytes,
    });

    let mut file = if resume_accepted {
        tokio::fs::OpenOptions::new()
            .append(true)
            .open(&part_path)
            .await
            .with_context(|| format!("failed to append {}", part_path.display()))?
    } else {
        tokio::fs::File::create(&part_path)
            .await
            .with_context(|| format!("failed to create {}", part_path.display()))?
    };

    let mut downloaded = starting_bytes;
    if starting_bytes > 0 {
        sink.send(ProgressEvent::FileBytes {
            delivered: downloaded,
        });
    }

    let transfer = async {
        let mut data = stream
            .retr_as_stream(&server_path)
            .await
            .with_context(|| format!("failed to start FTPS RETR for {server_path}"))?;
        let mut buf = vec![0_u8; 64 * 1024];
        loop {
            let read = data
                .read(&mut buf)
                .await
                .with_context(|| format!("failed to read FTPS stream for {server_path}"))?;
            if read == 0 {
                break;
            }
            let chunk_len = u64::try_from(read).context("chunk length does not fit u64")?;
            file.write_all(&buf[..read])
                .await
                .with_context(|| format!("failed to write {}", part_path.display()))?;
            downloaded = downloaded.saturating_add(chunk_len);
            sink.send(ProgressEvent::FileBytes {
                delivered: downloaded,
            });
        }
        stream
            .finalize_retr_stream(data)
            .await
            .with_context(|| format!("failed to finalize FTPS RETR for {server_path}"))?;
        anyhow::Ok(())
    };

    if let Some(timeout) = backend.transfer_timeout {
        tokio::time::timeout(timeout, transfer)
            .await
            .with_context(|| {
                format!(
                    "FTPS transfer for {} timed out after {timeout:?}",
                    source.filepath,
                )
            })??;
    } else {
        transfer.await?;
    }

    file.flush()
        .await
        .with_context(|| format!("failed to flush {}", part_path.display()))?;
    drop(file);
    stream.quit().await.ok();

    if downloaded != expected_bytes {
        bail!(
            "downloaded {downloaded} bytes for {}, expected {expected_bytes}",
            source.filepath
        );
    }

    fs::rename(&part_path, &destination).with_context(|| {
        format!(
            "failed to rename {} to {}",
            part_path.display(),
            destination.display()
        )
    })?;
    sink.send(ProgressEvent::AttemptFinished);
    Ok(downloaded)
}

/// Returns whether a destination file already exists with the indexed size.
fn existing_file_is_complete(source: &SourceFileRow) -> anyhow::Result<bool> {
    let path = source.local_path_buf();
    let expected = u64::try_from(source.size_bytes).context("negative source size")?;
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len() == expected),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).context("failed to inspect existing download"),
    }
}

/// Builds the temporary partial-download path.
fn part_path(destination: &Path) -> PathBuf {
    let mut value = destination.as_os_str().to_owned();
    value.push(".part");
    PathBuf::from(value)
}

/// Returns the current partial-file size, if any.
fn partial_size(path: &Path) -> anyhow::Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error).with_context(|| format!("failed to inspect {}", path.display())),
    }
}
