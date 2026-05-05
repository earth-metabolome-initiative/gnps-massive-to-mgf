#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod checksum;

/// Runtime configuration loaded from `.env` and the process environment.
pub mod config;
/// mzML to sharded MGF conversion.
pub mod conversion;
/// SQLite state store backed by Diesel.
pub mod db;
/// Open-format index ingestion and row models.
pub mod index;
/// MassIVE file download helpers.
pub mod massive;
/// End-to-end pipeline orchestration.
pub mod pipeline;
/// Terminal progress reporting.
pub mod progress;
/// Read-only pipeline status reporting.
pub mod status;
/// Zenodo index download and result publication.
pub mod zenodo_publish;

pub use config::Config;
pub use conversion::{ConversionSummary, convert_downloaded_mzml};
pub use db::StateDb;
pub use index::{OpenFormatRecord, SourceFileStatus, ingest_openformats};
pub use massive::download_pending_mzml;
pub use pipeline::{PipelineDecision, decide_next_pipeline_step, run_pipeline};
pub use progress::ProgressReporter;
pub use status::{PipelineStatus, collect_pipeline_status};
pub use zenodo_publish::{
    PublicationDryRunSummary, download_openformats_indexes, publication_dry_run,
    publish_outputs_to_zenodo,
};
