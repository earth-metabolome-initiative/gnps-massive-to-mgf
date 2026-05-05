//! Binary entrypoint for the MassIVE mzML to MGF pipeline.

use anyhow::{Context, bail};
use gnps_massive_to_mgf::{
    Config, ProgressReporter, StateDb, collect_pipeline_status, convert_downloaded_mzml,
    download_openformats_indexes, download_pending_mzml, ingest_openformats, publication_dry_run,
    publish_outputs_to_zenodo, run_pipeline,
};

/// Runs the requested pipeline subcommand.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let progress = ProgressReporter::new();
    let config = Config::from_env().context("failed to read runtime configuration")?;
    let mut db = StateDb::connect(&config.database_url).context("failed to open state database")?;
    db.initialize()
        .context("failed to initialize state database")?;

    match std::env::args().nth(1).as_deref() {
        Some("index") => {
            download_openformats_indexes(&config, &progress)
                .await
                .context("failed to download open-format index files")?;
            let report = ingest_openformats(&mut db, &config, &progress)
                .context("failed to ingest index")?;
            progress.println(format!(
                "indexed MassIVE mzML files: {} queued, {} candidates, {} indexed MS/MS spectra",
                report.inserted_mzml, report.candidate_mzml, report.target_ms2_spectra
            ))?;
        }
        Some("download") => {
            download_pending_mzml(&mut db, &config, &progress)
                .await
                .context("failed to download pending mzML files")?;
        }
        Some("convert") => {
            let summary = convert_downloaded_mzml(&mut db, &config, &progress)
                .context("failed to convert downloaded mzML files")?;
            progress.println(format!(
                "conversion complete: {} spectra written across {} shards",
                summary.spectra_written, summary.shards_written
            ))?;
        }
        Some("publish") => {
            let record = Box::pin(publish_outputs_to_zenodo(&mut db, &config, &progress))
                .await
                .context("failed to publish outputs to Zenodo")?;
            progress.println(format!("published Zenodo record: {}", record.record.id))?;
        }
        Some("publish-dry-run") => {
            let summary = publication_dry_run(&mut db, &config)
                .context("failed to prepare Zenodo publication dry run")?;
            for line in summary.lines() {
                println!("{line}");
            }
        }
        Some("status") => {
            let status = collect_pipeline_status(&mut db, &config)
                .context("failed to collect pipeline status")?;
            for line in status.lines() {
                println!("{line}");
            }
        }
        Some("run") | None => {
            run_pipeline(&mut db, &config, &progress).await?;
        }
        Some(command) => {
            bail!(
                "unknown command {command}; expected one of: index, download, convert, publish, publish-dry-run, status, run"
            );
        }
    }

    Ok(())
}
