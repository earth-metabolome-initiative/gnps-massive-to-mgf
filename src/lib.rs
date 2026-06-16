#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![allow(
    clippy::future_not_send,
    reason = "the binary uses #[tokio::main(flavor = \"current_thread\")] so async tasks never migrate across threads; the Send bound this lint enforces is not meaningful for our runtime choice"
)]

mod checksum;

/// Runtime configuration loaded from `.env` and the process environment.
pub mod config;
/// mzML to sharded MGF conversion.
pub mod conversion;
/// SQLite state store backed by Diesel.
pub mod db;
/// Trust-On-First-Use cert verifier for FTPS connections.
pub mod ftps_tofu;
/// Open-format index ingestion and row models.
pub mod index;
/// MassIVE file download helpers.
pub mod massive;
/// Terminal progress reporting.
pub mod progress;
/// Channel-based progress events emitted by the download path.
pub mod progress_sink;
/// Zenodo open-format index download.
pub mod zenodo_publish;

pub use config::Config;
pub use conversion::{ConversionSummary, convert_downloaded_mzml};
pub use db::{DownloadFilter, DownloadProgressTotals, StateDb};
pub use index::{
    OpenFormatRecord, SourceFileStatus, VolumeIngestReport, ingest_openformats,
    ingest_volume_manifest,
};
pub use massive::download_pending_mzml;
pub use progress::ProgressReporter;
pub use progress_sink::{ProgressEvent, ProgressSink};
pub use zenodo_publish::{IndexFreshness, ensure_openformats_indexes_fresh};
