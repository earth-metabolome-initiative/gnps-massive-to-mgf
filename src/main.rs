//! Binary entrypoint for the MassIVE mzML to MGF pipeline.

use anyhow::{Context, bail};
use massive::{
    Config, DownloadFilter, IndexFreshness, ProgressReporter, StateDb, checksum,
    convert_downloaded_mzml, ensure_openformats_indexes_fresh, ingest_openformats,
    ingest_volume_manifest, run_download_tui,
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

/// Refreshes the open-format indexes when either the Zenodo record has been
/// modified since the last fetch or the user has changed an ingest-relevant
/// config knob (target MS2, source-selection buffer, keyword allowlist). The
/// volume manifest is always re-ingested since it is fast and idempotent.
async fn refresh_indexes_and_ingest(
    db: &mut StateDb,
    config: &Config,
    progress: &ProgressReporter,
) -> anyhow::Result<()> {
    let freshness = ensure_openformats_indexes_fresh(config, progress)
        .await
        .context("failed to refresh open-format index files")?;
    let current_signature = ingest_config_signature(config);
    let stored_signature = db
        .read_ingest_state()
        .context("failed to read prior ingest state")?;
    let signature_changed = stored_signature.as_deref() != Some(current_signature.as_str());
    let needs_reingest = matches!(freshness, IndexFreshness::Refreshed) || signature_changed;
    if needs_reingest {
        if signature_changed {
            progress.println(
                "ingest configuration changed (target MS2, buffer, or keyword allowlist), re-ingesting openformats index",
            )?;
        }
        let report = ingest_openformats(db, config, progress).context("failed to ingest index")?;
        progress.println(format!(
            "indexed MassIVE mzML files: {} queued, {} candidates, {} indexed MS/MS spectra",
            report.inserted_mzml, report.candidate_mzml, report.target_ms2_spectra
        ))?;
        db.write_ingest_state(&current_signature)
            .context("failed to persist new ingest state")?;
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

/// Fingerprints the ingest-relevant pieces of `Config` as a lowercase
/// SHA-256 hex string. Used by [`refresh_indexes_and_ingest`] to detect
/// changes to `TARGET_MS2_SPECTRA`, `SOURCE_SELECTION_BUFFER`, or
/// `INGEST_KEYWORD_ALLOWLIST` between runs.
fn ingest_config_signature(config: &Config) -> String {
    let mut sorted_allowlist = config.ingest_keyword_allowlist.clone();
    sorted_allowlist.sort();
    let payload = format!(
        "target_ms2={};source_selection_buffer={};ingest_keyword_allowlist={}",
        config.target_ms2_spectra,
        config.source_selection_buffer,
        sorted_allowlist.join(","),
    );
    checksum::sha256_hex(payload.as_bytes())
}
