//! Binary entrypoint for the MassIVE mzML to MGF pipeline.

use anyhow::{Context, bail};
use massive::{
    Config, DownloadFilter, IndexFreshness, ProgressReporter, StateDb, convert_downloaded_mzml,
    ensure_openformats_indexes_fresh, ingest_openformats, ingest_volume_manifest, run_download_tui,
};

/// Runs the requested pipeline subcommand.
///
/// Uses the `current_thread` runtime: the pipeline runs a single async task
/// at a time (one FTPS download in flight, one Zenodo index fetch, etc.) and
/// gains nothing from work-stealing across worker threads. Picking the
/// single-thread runtime drops the worker threadpool, removes the `Send`
/// bound on futures, and lets the download progress accounting use plain
/// `Cell` instead of atomics.
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let progress = ProgressReporter::new();
    let config = Config::from_env().context("failed to read runtime configuration")?;
    let mut db = StateDb::connect(&config.database_url).context("failed to open state database")?;
    db.initialize()
        .context("failed to initialize state database")?;

    match std::env::args().nth(1).as_deref() {
        Some("download") => {
            refresh_indexes_and_ingest(&mut db, &config, &progress).await?;
            run_download_tui(&mut db, &config, &DownloadFilter::default())
                .await
                .context("download TUI exited with an error")?;
        }
        Some("convert") => {
            let summary = convert_downloaded_mzml(&mut db, &config, &progress)
                .context("failed to convert downloaded mzML files")?;
            progress.println(format!(
                "conversion complete: {} spectra written across {} shards",
                summary.spectra_written, summary.shards_written
            ))?;
        }
        Some(command) => {
            bail!("unknown command {command}; expected one of: download, convert");
        }
        None => {
            bail!("no command provided; expected one of: download, convert");
        }
    }

    Ok(())
}

/// Refreshes the open-format indexes when the Zenodo record has been
/// modified since the local TSVs were last downloaded, then re-ingests
/// `source_files` if the TSVs were refreshed. Always re-ingests the volume
/// manifest, which is idempotent and fast.
async fn refresh_indexes_and_ingest(
    db: &mut StateDb,
    config: &Config,
    progress: &ProgressReporter,
) -> anyhow::Result<()> {
    let freshness = ensure_openformats_indexes_fresh(config, progress)
        .await
        .context("failed to refresh open-format index files")?;
    if matches!(freshness, IndexFreshness::Refreshed) {
        let report = ingest_openformats(db, config, progress).context("failed to ingest index")?;
        progress.println(format!(
            "indexed MassIVE mzML files: {} queued, {} candidates, {} indexed MS/MS spectra",
            report.inserted_mzml, report.candidate_mzml, report.target_ms2_spectra
        ))?;
    }
    let volume_report = ingest_volume_manifest(db, config, progress)
        .context("failed to ingest MassIVE volume manifest")?;
    progress.println(format!(
        "ingested MassIVE volume manifest: {} updated, {} skipped, {} planned download(s) still missing a volume",
        volume_report.rows_updated,
        volume_report.rows_skipped,
        volume_report.missing_volume_after_ingest
    ))?;
    Ok(())
}
