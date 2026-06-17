//! FTPS production-path smoke test against the real `massive-ftp.ucsd.edu`.
//!
//! `#[ignore]`d so CI never runs it. Invoke manually with:
//!
//! ```text
//! cargo test --test ftps_spike -- --ignored --nocapture
//! ```
//!
//! Downloads three representative files sequentially through the same FTPS
//! backend used by the production downloader. Reports per-file throughput.
//! This is the only end-to-end coverage for the FTPS code path; the hermetic
//! unit-test mock that the original plan called for was deferred.

#![allow(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "throughput math intentionally uses u64-to-f64 casts; this is a measurement-only test"
)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, anyhow};
use massive::ftps_tofu::{TofuCertVerifier, rustls_config};
use suppaftp::tokio::{AsyncRustlsConnector, AsyncRustlsFtpStream};
use suppaftp::tokio_rustls::TlsConnector;
use suppaftp::types::FileType;
use tokio::io::AsyncReadExt;

/// FTPS server host.
const HOST: &str = "massive-ftp.ucsd.edu";
/// FTPS server port (control channel).
const PORT: u16 = 21;

/// Three real public files of varied sizes, all on `massive-ftp.ucsd.edu`.
const TEST_FILES: &[(&str, &str)] = &[
    ("x01", "MSV000079624/ccms_peak/RAW/C8.mzML"),
    ("v01", "MSV000079601/ccms_peak/100324_neg_micropoll_17.mzML"),
    (
        "v01",
        "MSV000079512/peak/mzML/Pale_Gamma1_2d_OrbiFull_30K_LTQMS_Run1_071808_08.mzML",
    ),
];

/// Builds the absolute server path for one test file.
fn server_path(entry: &(&str, &str)) -> String {
    format!("/{}/{}", entry.0, entry.1)
}

/// Connects, authenticates anonymously, RETRs `path`, returns bytes received.
async fn download_one(verifier: Arc<TofuCertVerifier>, path: String) -> anyhow::Result<u64> {
    let tls_config = rustls_config(verifier).context("failed to build rustls config")?;
    let connector = TlsConnector::from(Arc::new(tls_config));
    let stream = AsyncRustlsFtpStream::connect((HOST, PORT))
        .await
        .with_context(|| format!("failed to connect to {HOST}:{PORT}"))?;
    let mut stream = stream
        .into_secure(AsyncRustlsConnector::from(connector), HOST)
        .await
        .context("failed to upgrade to FTPS")?;
    stream
        .login("anonymous", "anonymous@example.com")
        .await
        .context("failed to authenticate anonymously")?;
    stream
        .transfer_type(FileType::Binary)
        .await
        .context("failed to switch to binary transfer type")?;

    let mut data = stream
        .retr_as_stream(&path)
        .await
        .with_context(|| format!("failed to start RETR for {path}"))?;
    let mut buf = vec![0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let read = data
            .read(&mut buf)
            .await
            .with_context(|| format!("failed to read from RETR stream for {path}"))?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(u64::try_from(read).unwrap_or_default());
    }
    stream
        .finalize_retr_stream(data)
        .await
        .with_context(|| format!("failed to finalize RETR for {path}"))?;
    stream.quit().await.context("failed to send QUIT")?;
    Ok(total)
}

/// Resolves the pin-file path under the working directory's `data/` dir.
fn pin_path() -> PathBuf {
    PathBuf::from("data/ftps_cert_pin.txt")
}

/// Hits the real server; only runs when explicitly opted into.
#[tokio::test]
#[ignore = "hits the real massive-ftp.ucsd.edu server; run manually with --ignored"]
async fn ftps_smoke_test() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_err| anyhow!("failed to install rustls ring crypto provider"))?;
    let verifier = TofuCertVerifier::new(pin_path());
    eprintln!("\n=== FTPS smoke test against {HOST} ===");
    eprintln!("Pin file: {}\n", verifier.pin_path().display());
    let overall_start = Instant::now();
    let mut overall_bytes = 0_u64;
    let mut overall_failures = 0_usize;
    for entry in TEST_FILES {
        let start = Instant::now();
        match download_one(Arc::clone(&verifier), server_path(entry)).await {
            Ok(bytes) => {
                let secs = start.elapsed().as_secs_f64();
                let mb = bytes as f64 / 1_000_000.0;
                let rate = if secs > 0.0 { mb / secs } else { 0.0 };
                eprintln!(
                    "  {}: {bytes} bytes ({mb:.1} MB) in {secs:.2}s = {rate:.2} MB/s",
                    entry.1,
                );
                overall_bytes = overall_bytes.saturating_add(bytes);
            }
            Err(error) => {
                overall_failures = overall_failures.saturating_add(1);
                eprintln!("  {}: FAILED: {error:#}", entry.1);
            }
        }
    }
    let overall_secs = overall_start.elapsed().as_secs_f64();
    let overall_mb = overall_bytes as f64 / 1_000_000.0;
    let overall_rate = if overall_secs > 0.0 {
        overall_mb / overall_secs
    } else {
        0.0
    };
    eprintln!(
        "\nTotal: {} files, {overall_mb:.1} MB in {overall_secs:.2}s = {overall_rate:.2} MB/s, {overall_failures} failure(s)\n",
        TEST_FILES.len(),
    );
    if overall_failures > 0 {
        anyhow::bail!("{overall_failures} FTPS smoke download(s) failed");
    }
    Ok(())
}
