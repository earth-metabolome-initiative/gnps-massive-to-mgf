//! MassIVE source-file download helpers.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, anyhow, bail};
use futures_util::{StreamExt, stream};
use indicatif::ProgressBar;
use reqwest::Client;
use reqwest::header::RANGE;
use tokio::io::AsyncWriteExt;

use crate::config::Config;
use crate::db::{SourceFileRow, StateDb};
use crate::index::SourceFileStatus;
use crate::progress::ProgressReporter;

/// MassIVE `DownloadResultFile` endpoint.
const MASSIVE_DOWNLOAD_ENDPOINT: &str = "https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile";

/// HTTP client and endpoint used for MassIVE file downloads.
#[derive(Clone)]
struct MassiveDownloadClient {
    /// Reusable HTTP client.
    http: Client,
    /// Base `DownloadResultFile` endpoint.
    endpoint: String,
}

impl MassiveDownloadClient {
    /// Builds a production MassIVE download client.
    fn new(config: &Config) -> anyhow::Result<Self> {
        let mut builder = Client::builder()
            .connect_timeout(config.http_connect_timeout)
            .user_agent("gnps-massive-to-mgf/0.1");
        if let Some(timeout) = config.http_request_timeout {
            builder = builder.timeout(timeout);
        }
        let http = builder.build().context("failed to build HTTP client")?;
        Ok(Self {
            http,
            endpoint: MASSIVE_DOWNLOAD_ENDPOINT.to_owned(),
        })
    }

    /// Builds a request for one indexed MassIVE filepath.
    fn get(&self, filepath: &str) -> reqwest::RequestBuilder {
        self.http
            .get(massive_download_url_with_endpoint(&self.endpoint, filepath))
    }

    /// Builds a client using a test endpoint.
    #[cfg(test)]
    fn with_endpoint(endpoint: String) -> anyhow::Result<Self> {
        let http = Client::builder()
            .user_agent("gnps-massive-to-mgf/0.1")
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self { http, endpoint })
    }
}

/// Shared aggregate byte progress for concurrent file downloads.
#[derive(Clone)]
struct AggregateDownloadProgress {
    /// Optional aggregate byte bar.
    bar: Option<ProgressBar>,
    /// Highest accounted byte count per source filepath.
    file_bytes: Arc<Mutex<HashMap<String, u64>>>,
}

impl AggregateDownloadProgress {
    /// Builds a new aggregate progress tracker.
    fn new(bar: Option<ProgressBar>) -> Self {
        Self {
            bar,
            file_bytes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Records the currently accounted bytes for one source file.
    fn set_file_bytes(&self, filepath: &str, bytes: u64) -> anyhow::Result<()> {
        let delta = self.file_byte_delta(filepath, bytes)?;
        if delta > 0
            && let Some(bar) = &self.bar
        {
            bar.inc(delta);
        }
        Ok(())
    }

    /// Computes the byte delta for one source file and updates the local cache.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the mutex guard is already scoped to one small progress-cache update"
    )]
    fn file_byte_delta(&self, filepath: &str, bytes: u64) -> anyhow::Result<u64> {
        let mut file_bytes = self
            .file_bytes
            .lock()
            .map_err(|error| anyhow!("download progress lock poisoned: {error}"))?;
        let current = file_bytes.entry(filepath.to_owned()).or_default();
        if bytes <= *current {
            Ok(0)
        } else {
            let delta = bytes.saturating_sub(*current);
            *current = bytes;
            Ok(delta)
        }
    }
}

/// Downloads pending mzML files into the configured raw download directory.
///
/// The implementation is intentionally conservative: files are retried, written
/// to `.part` paths, validated against the indexed byte size, then atomically
/// renamed into place.
///
/// # Errors
///
/// Returns an error if the database query fails or if progress rendering fails.
pub async fn download_pending_mzml(
    db: &mut StateDb,
    config: &Config,
    progress: &ProgressReporter,
) -> anyhow::Result<()> {
    let client = MassiveDownloadClient::new(config)?;
    download_pending_mzml_with_client(db, config, progress, client).await
}

/// Downloads pending mzML files using the provided MassIVE client.
async fn download_pending_mzml_with_client(
    db: &mut StateDb,
    config: &Config,
    progress: &ProgressReporter,
    client: MassiveDownloadClient,
) -> anyhow::Result<()> {
    let reset = db.reset_download_failures()?;
    if reset > 0 {
        progress.println(format!("retrying {reset} previously failed download(s)"))?;
    }
    let indexed = db.count_status(SourceFileStatus::Indexed)?;
    let pending_bytes = db.pending_download_bytes()?;
    let mut remaining = indexed;
    let mut downloaded = 0_u64;
    let mut failed = 0_u64;
    progress.println(format!(
        "download queue: {remaining} mzML files, workers {}",
        config.download_workers
    ))?;
    let files_bar = (indexed > 0)
        .then(|| progress.count_bar(indexed, format!("download files | remaining={remaining}")))
        .transpose()?;
    let bytes_bar = (pending_bytes > 0)
        .then(|| progress.byte_bar(pending_bytes, "download total bytes"))
        .transpose()?;
    let aggregate_progress = AggregateDownloadProgress::new(bytes_bar.clone());
    let worker_count = config.download_workers.max(1);

    while remaining > 0 {
        let batch = db.pending_downloads(worker_count)?;
        if batch.is_empty() {
            break;
        }
        let downloads = stream::iter(batch)
            .map(|source| {
                let client = client.clone();
                let config = config.clone();
                let progress = progress.clone();
                let aggregate_progress = aggregate_progress.clone();
                async move {
                    let result = download_with_retries(
                        &client,
                        &config,
                        &progress,
                        &aggregate_progress,
                        &source,
                    )
                    .await;
                    (source, result)
                }
            })
            .buffer_unordered(worker_count);
        futures_util::pin_mut!(downloads);
        while let Some((source, result)) = downloads.next().await {
            match result {
                Ok(bytes) => {
                    downloaded = downloaded.saturating_add(1);
                    db.mark_downloaded(&source.filepath, bytes)?;
                }
                Err(error) => {
                    failed = failed.saturating_add(1);
                    let message = format!("{error:#}");
                    db.mark_download_failed(&source.filepath, &message)?;
                    progress
                        .println(format!("download failed: {} | {message}", source.filepath))?;
                }
            }
            if let Some(bar) = &files_bar {
                bar.inc(1);
            }
        }
        remaining = db.count_status(SourceFileStatus::Indexed)?;
        if let Some(bar) = &files_bar {
            bar.set_message(format!(
                "download files | downloaded_or_present={downloaded} failed={failed} remaining={remaining}"
            ));
        }
    }
    if let Some(bar) = files_bar {
        bar.finish_with_message(format!(
            "download files | downloaded_or_present={downloaded} failed={failed} remaining={remaining}"
        ));
    }
    if let Some(bar) = bytes_bar {
        bar.finish_with_message("download total bytes");
    }
    progress.println(format!(
        "download summary: downloaded_or_present={downloaded} failed={failed}"
    ))?;

    Ok(())
}

/// Downloads one source file with retries.
async fn download_with_retries(
    client: &MassiveDownloadClient,
    config: &Config,
    progress: &ProgressReporter,
    aggregate_progress: &AggregateDownloadProgress,
    source: &SourceFileRow,
) -> anyhow::Result<u64> {
    if existing_file_is_complete(source)? {
        let bytes = u64::try_from(source.size_bytes).context("negative source size")?;
        aggregate_progress.set_file_bytes(&source.filepath, bytes)?;
        progress.println(format!("already downloaded: {}", source.filepath))?;
        return Ok(bytes);
    }

    let mut last_error = None;
    for attempt in 1..=config.download_retry_attempts {
        match download_once(client, progress, aggregate_progress, source, attempt).await {
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

/// Downloads one source file once.
async fn download_once(
    client: &MassiveDownloadClient,
    progress: &ProgressReporter,
    aggregate_progress: &AggregateDownloadProgress,
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

    let bar = progress.byte_bar(
        expected_bytes,
        format!("{} | attempt {attempt}", source.filepath),
    )?;
    let existing_part = partial_size(&part_path)?;
    if existing_part == expected_bytes {
        fs::rename(&part_path, &destination).with_context(|| {
            format!(
                "failed to rename complete partial {} to {}",
                part_path.display(),
                destination.display()
            )
        })?;
        aggregate_progress.set_file_bytes(&source.filepath, expected_bytes)?;
        bar.finish_with_message(format!("downloaded {}", source.filepath));
        return Ok(expected_bytes);
    }
    let can_resume = existing_part > 0 && existing_part < expected_bytes;
    let mut request = client.get(&source.filepath);
    if can_resume {
        request = request.header(RANGE, format!("bytes={existing_part}-"));
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("failed to request {}", source.filepath))?;
    if !response.status().is_success() {
        bail!(
            "MassIVE returned HTTP {} for {}",
            response.status(),
            source.filepath
        );
    }
    let resume_accepted = can_resume && response.status() == reqwest::StatusCode::PARTIAL_CONTENT;
    let starting_bytes = if resume_accepted { existing_part } else { 0 };
    if can_resume && !resume_accepted {
        progress.println(format!(
            "MassIVE did not accept range resume for {}; restarting from zero",
            source.filepath
        ))?;
    }

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
        bar.inc(starting_bytes);
        aggregate_progress.set_file_bytes(&source.filepath, starting_bytes)?;
    }
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.with_context(|| format!("failed while downloading {}", source.filepath))?;
        file.write_all(&chunk)
            .await
            .with_context(|| format!("failed to write {}", part_path.display()))?;
        let chunk_len = u64::try_from(chunk.len()).context("chunk length does not fit u64")?;
        downloaded = downloaded.saturating_add(chunk_len);
        bar.inc(chunk_len);
        aggregate_progress.set_file_bytes(&source.filepath, downloaded)?;
    }
    file.flush()
        .await
        .with_context(|| format!("failed to flush {}", part_path.display()))?;
    drop(file);

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
    bar.finish_with_message(format!("downloaded {}", source.filepath));
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

/// Builds a MassIVE direct HTTP download URL from an indexed filepath.
#[must_use]
pub fn massive_download_url(filepath: &str) -> String {
    massive_download_url_with_endpoint(MASSIVE_DOWNLOAD_ENDPOINT, filepath)
}

/// Builds a MassIVE direct HTTP download URL from an endpoint and indexed filepath.
fn massive_download_url_with_endpoint(endpoint: &str, filepath: &str) -> String {
    let descriptor = format!("f.{filepath}");
    format!(
        "{endpoint}?file={}&forceDownload=true",
        urlencoding::encode(&descriptor)
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Context;
    use axum::Router;
    use axum::body::Body;
    use axum::extract::{Query, State};
    use axum::http::header::RANGE;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    use super::{
        AggregateDownloadProgress, MassiveDownloadClient, download_pending_mzml_with_client,
        download_with_retries, massive_download_url, part_path,
    };
    use crate::config::Config;
    use crate::db::{SourceFileRow, StateDb};
    use crate::index::{OpenFormatRecord, SourceFileStatus};
    use crate::progress::ProgressReporter;

    /// In-process HTTP fixture server.
    struct TestServer {
        /// Download endpoint.
        endpoint: String,
        /// Shutdown signal.
        shutdown: Option<oneshot::Sender<()>>,
        /// Server task handle.
        handle: tokio::task::JoinHandle<()>,
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                match shutdown.send(()) {
                    Ok(()) | Err(()) => {}
                }
            }
            self.handle.abort();
        }
    }

    /// Fixture file served by the local HTTP server.
    #[derive(Clone)]
    struct TestFile {
        /// Response body.
        body: Vec<u8>,
        /// Whether range requests return HTTP 206.
        accepts_range: bool,
        /// Whether the response body is intentionally one byte short.
        truncate_body: bool,
    }

    /// Shared fixture state.
    #[derive(Clone)]
    struct TestServerState {
        /// Files keyed by indexed MassIVE filepath.
        files: Arc<BTreeMap<String, TestFile>>,
    }

    /// Confirms indexed file paths map to the MassIVE direct download endpoint.
    #[test]
    fn builds_download_url() {
        let url = massive_download_url("MSV000078547/ccms_peak/DLab/file with spaces.mzML");
        assert!(url.starts_with("https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file="));
        assert!(url.contains("f.MSV000078547%2Fccms_peak%2FDLab%2Ffile%20with%20spaces.mzML"));
        assert!(url.ends_with("&forceDownload=true"));
    }

    /// Fresh downloads write the exact expected bytes into the final path.
    #[tokio::test]
    async fn downloads_from_configurable_endpoint() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let filepath = "MSV000000001/ccms_peak/test.mzML";
        let body = b"fresh mzml bytes".to_vec();
        let server = start_server([(filepath, test_file(body.clone(), true, false))]).await?;
        let config = test_config(&temp, 1)?;
        let source = source_row(&config, filepath, body.len())?;
        let progress = ProgressReporter::hidden();
        let client = MassiveDownloadClient::with_endpoint(server.endpoint.clone())?;
        let aggregate_progress = AggregateDownloadProgress::new(None);

        let bytes =
            download_with_retries(&client, &config, &progress, &aggregate_progress, &source)
                .await?;

        assert_eq!(bytes, u64::try_from(body.len())?);
        assert_eq!(fs::read(source.local_path_buf())?, body);
        Ok(())
    }

    /// A valid partial file resumes with a range request and produces the full file.
    #[tokio::test]
    async fn resumes_partial_downloads() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let filepath = "MSV000000001/ccms_peak/resume.mzML";
        let body = b"resume mzml bytes".to_vec();
        let server = start_server([(filepath, test_file(body.clone(), true, false))]).await?;
        let config = test_config(&temp, 1)?;
        let source = source_row(&config, filepath, body.len())?;
        let destination = source.local_path_buf();
        create_parent(&destination)?;
        fs::write(part_path(&destination), &body[..6])?;
        let progress = ProgressReporter::hidden();
        let client = MassiveDownloadClient::with_endpoint(server.endpoint.clone())?;
        let aggregate_progress = AggregateDownloadProgress::new(None);

        let bytes =
            download_with_retries(&client, &config, &progress, &aggregate_progress, &source)
                .await?;

        assert_eq!(bytes, u64::try_from(body.len())?);
        assert_eq!(fs::read(destination)?, body);
        Ok(())
    }

    /// A server that ignores range requests causes the partial file to be restarted.
    #[tokio::test]
    async fn restarts_when_range_resume_is_refused() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let filepath = "MSV000000001/ccms_peak/restart.mzML";
        let body = b"restart mzml bytes".to_vec();
        let server = start_server([(filepath, test_file(body.clone(), false, false))]).await?;
        let config = test_config(&temp, 1)?;
        let source = source_row(&config, filepath, body.len())?;
        let destination = source.local_path_buf();
        create_parent(&destination)?;
        fs::write(part_path(&destination), &body[..7])?;
        let progress = ProgressReporter::hidden();
        let client = MassiveDownloadClient::with_endpoint(server.endpoint.clone())?;
        let aggregate_progress = AggregateDownloadProgress::new(None);

        let bytes =
            download_with_retries(&client, &config, &progress, &aggregate_progress, &source)
                .await?;

        assert_eq!(bytes, u64::try_from(body.len())?);
        assert_eq!(fs::read(destination)?, body);
        Ok(())
    }

    /// Short responses fail validation instead of being renamed into place.
    #[tokio::test]
    async fn rejects_short_downloads() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let filepath = "MSV000000001/ccms_peak/short.mzML";
        let body = b"short mzml bytes".to_vec();
        let server = start_server([(filepath, test_file(body.clone(), true, true))]).await?;
        let config = test_config(&temp, 1)?;
        let source = source_row(&config, filepath, body.len())?;
        let progress = ProgressReporter::hidden();
        let client = MassiveDownloadClient::with_endpoint(server.endpoint.clone())?;
        let aggregate_progress = AggregateDownloadProgress::new(None);

        let error =
            download_with_retries(&client, &config, &progress, &aggregate_progress, &source)
                .await
                .err()
                .context("download should have failed")?;

        assert!(format!("{error:#}").contains("expected"));
        assert!(!source.local_path_buf().exists());
        Ok(())
    }

    /// Mixed concurrent downloads update SQLite statuses after each worker result.
    #[tokio::test]
    async fn download_queue_records_successes_and_failures() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let successful_path = "MSV000000001/ccms_peak/success.mzML";
        let failing_path = "MSV000000001/ccms_peak/fail.mzML";
        let successful_body = b"successful mzml bytes".to_vec();
        let failing_body = b"failing mzml bytes".to_vec();
        let server = start_server([
            (
                successful_path,
                test_file(successful_body.clone(), true, false),
            ),
            (failing_path, test_file(failing_body.clone(), true, true)),
        ])
        .await?;
        let config = test_config(&temp, 2)?;
        let mut db = StateDb::connect(&config.database_url)?;
        db.initialize()?;
        insert_indexed_source(&mut db, &config, successful_path, successful_body.len(), 0)?;
        insert_indexed_source(&mut db, &config, failing_path, failing_body.len(), 1)?;
        let progress = ProgressReporter::hidden();
        let client = MassiveDownloadClient::with_endpoint(server.endpoint.clone())?;

        download_pending_mzml_with_client(&mut db, &config, &progress, client).await?;

        assert_eq!(db.count_status(SourceFileStatus::Downloaded)?, 1);
        assert_eq!(db.count_status(SourceFileStatus::DownloadFailed)?, 1);
        assert_eq!(db.count_status(SourceFileStatus::Indexed)?, 0);
        Ok(())
    }

    /// Starts a local MassIVE-like download endpoint.
    async fn start_server(
        files: impl IntoIterator<Item = (&'static str, TestFile)>,
    ) -> anyhow::Result<TestServer> {
        let files = files
            .into_iter()
            .map(|(path, file)| (path.to_owned(), file))
            .collect::<BTreeMap<_, _>>();
        let state = TestServerState {
            files: Arc::new(files),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let endpoint = format!("http://{address}/ProteoSAFe/DownloadResultFile");
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let app = Router::new()
            .route("/ProteoSAFe/DownloadResultFile", get(test_download_handler))
            .with_state(state);
        let handle = tokio::spawn(async move {
            let shutdown = async move {
                match shutdown_receiver.await {
                    Ok(()) | Err(_) => {}
                }
            };
            match axum::serve(listener, app)
                .with_graceful_shutdown(shutdown)
                .await
            {
                Ok(()) | Err(_) => {}
            }
        });
        Ok(TestServer {
            endpoint,
            shutdown: Some(shutdown_sender),
            handle,
        })
    }

    /// Serves one fixture file using MassIVE's `file=f.path` query shape.
    async fn test_download_handler(
        State(state): State<TestServerState>,
        Query(query): Query<BTreeMap<String, String>>,
        headers: HeaderMap,
    ) -> Response {
        let Some(descriptor) = query.get("file") else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        let Some(filepath) = descriptor.strip_prefix("f.") else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        let Some(file) = state.files.get(filepath) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        let range_start = headers
            .get(RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_range_start);
        let body_len = file.body.len();
        let start = if file.accepts_range {
            range_start.map_or(0, |value| value.min(body_len))
        } else {
            0
        };
        let mut body = file.body[start..].to_vec();
        if file.truncate_body {
            body.truncate(body.len().saturating_sub(1));
        }
        let mut response = Body::from(body).into_response();
        if file.accepts_range && range_start.is_some() {
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        }
        response
    }

    /// Parses a simple HTTP range header.
    fn parse_range_start(header: &str) -> Option<usize> {
        let range = header.strip_prefix("bytes=")?;
        let (start, _) = range.split_once('-')?;
        start.parse().ok()
    }

    /// Builds a fixture file.
    fn test_file(body: Vec<u8>, accepts_range: bool, truncate_body: bool) -> TestFile {
        TestFile {
            body,
            accepts_range,
            truncate_body,
        }
    }

    /// Builds a test runtime configuration.
    fn test_config(temp: &TempDir, download_workers: usize) -> anyhow::Result<Config> {
        Ok(Config {
            openformats_record_id: 4_549_746,
            openformats_index_dir: temp.path().join("openformats"),
            database_url: temp
                .path()
                .join("state.sqlite")
                .to_str()
                .context("temporary path is not UTF-8")?
                .to_owned(),
            mzml_download_dir: temp.path().join("mzml"),
            mgf_output_dir: temp.path().join("mgf"),
            target_ms2_spectra: 1,
            top_k_peaks: 256,
            mgf_shard_max_spectra: 1_000,
            source_selection_buffer: 1.0,
            source_selection_chunk_ms2: 1,
            download_workers,
            download_retry_attempts: 1,
            download_retry_delay: Duration::ZERO,
            http_request_timeout: Some(Duration::from_secs(10)),
            http_connect_timeout: Duration::from_secs(5),
            publish_to_zenodo: false,
            zenodo_deposition_id: None,
            zenodo_max_file_bytes: 50_000_000_000,
        })
    }

    /// Builds a source row for direct downloader tests.
    fn source_row(
        config: &Config,
        filepath: &str,
        size_bytes: usize,
    ) -> anyhow::Result<SourceFileRow> {
        Ok(SourceFileRow {
            filepath: filepath.to_owned(),
            dataset: "MSV000000001".to_owned(),
            collection: "ccms_peak".to_owned(),
            create_time: "2026-01-01".to_owned(),
            size_bytes: i64::try_from(size_bytes)?,
            spectra_ms1: Some(1.0),
            spectra_ms2: Some(1.0),
            instrument_vendor: "vendor".to_owned(),
            instrument_model: "model".to_owned(),
            extension: ".mzML".to_owned(),
            local_path: config
                .mzml_download_dir
                .join(filepath)
                .to_string_lossy()
                .into_owned(),
            status: SourceFileStatus::Indexed.as_str().to_owned(),
        })
    }

    /// Inserts one indexed source row into SQLite.
    fn insert_indexed_source(
        db: &mut StateDb,
        config: &Config,
        filepath: &str,
        size_bytes: usize,
        selected_order: u64,
    ) -> anyhow::Result<()> {
        let record = OpenFormatRecord {
            filepath: filepath.to_owned(),
            dataset: "MSV000000001".to_owned(),
            collection: "ccms_peak".to_owned(),
            create_time: "2026-01-01".to_owned(),
            size: u64::try_from(size_bytes)?,
            size_mb: 1,
            spectra_ms1: Some(1.0),
            spectra_ms2: Some(1.0),
            instrument_vendor: "vendor".to_owned(),
            instrument_model: "model".to_owned(),
            extension: ".mzML".to_owned(),
        };
        let local_path = config.mzml_download_dir.join(filepath);
        db.upsert_source_file(
            &record,
            &local_path,
            selected_order,
            SourceFileStatus::Indexed,
        )
    }

    /// Creates a parent directory for a fixture file path.
    fn create_parent(path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(())
    }
}
