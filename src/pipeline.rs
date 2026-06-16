//! End-to-end pipeline orchestration.

use anyhow::Context;

use crate::config::Config;
use crate::conversion::convert_downloaded_mzml;
use crate::db::StateDb;
use crate::index::{ingest_openformats, ingest_volume_manifest};
use crate::massive::download_pending_mzml;
use crate::progress::ProgressReporter;
use crate::zenodo_publish::{IndexFreshness, ensure_openformats_indexes_fresh};

/// Decision made after one download-conversion pipeline round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineDecision {
    /// The finalized corpus reached the configured target.
    TargetReached,
    /// More source files were promoted and another round should run.
    PromoteMoreSources,
    /// The target was not reached, but no more candidates were available.
    NoMoreCandidates,
}

/// Runs indexing, download, and conversion until the target is reached.
///
/// # Errors
///
/// Returns an error if any pipeline stage fails.
pub async fn run_pipeline(
    db: &mut StateDb,
    config: &Config,
    progress: &ProgressReporter,
) -> anyhow::Result<()> {
    let pipeline_bar = progress.count_bar(2, "pipeline | indexing open-format files")?;
    let freshness = ensure_openformats_indexes_fresh(config, progress)
        .await
        .context("failed to refresh open-format index files")?;
    if matches!(freshness, IndexFreshness::Refreshed) {
        ingest_openformats(db, config, progress).context("failed to ingest index")?;
    }
    ingest_volume_manifest(db, config, progress)
        .context("failed to ingest MassIVE volume manifest")?;
    pipeline_bar.inc(1);
    let mut round = 1_u64;
    loop {
        pipeline_bar.set_message(format!("pipeline | corpus round {round}: downloading"));
        download_pending_mzml(db, config, progress)
            .await
            .context("failed to download pending mzML files")?;
        pipeline_bar.set_message(format!("pipeline | corpus round {round}: converting"));
        convert_downloaded_mzml(db, config, progress)
            .context("failed to convert downloaded mzML files")?;
        let summary = db
            .conversion_summary(config.top_k_peaks)
            .context("failed to read conversion summary")?;
        let promoted = if summary.spectra_written >= config.target_ms2_spectra {
            0
        } else {
            db.promote_candidates_by_ms2(config.source_selection_chunk_ms2)
                .context("failed to promote more MassIVE mzML candidates")?
        };
        match decide_next_pipeline_step(
            summary.spectra_written,
            config.target_ms2_spectra,
            promoted,
        ) {
            PipelineDecision::TargetReached => {
                pipeline_bar.inc(1);
                break;
            }
            PipelineDecision::NoMoreCandidates => {
                progress.println(format!(
                    "unique spectra target not reached: {} written, {} requested, no more candidates",
                    summary.spectra_written, config.target_ms2_spectra
                ))?;
                pipeline_bar.inc(1);
                break;
            }
            PipelineDecision::PromoteMoreSources => {
                progress.println(format!(
                    "unique spectra target not reached: {} written, {} requested; promoted {promoted} more mzML file(s)",
                    summary.spectra_written, config.target_ms2_spectra
                ))?;
                round = round.saturating_add(1);
            }
        }
    }
    pipeline_bar.finish_with_message("pipeline complete");
    Ok(())
}

/// Decides what the pipeline should do after one conversion round.
#[must_use]
pub const fn decide_next_pipeline_step(
    spectra_written: u64,
    target_ms2_spectra: u64,
    promoted_sources: u64,
) -> PipelineDecision {
    if spectra_written >= target_ms2_spectra {
        PipelineDecision::TargetReached
    } else if promoted_sources == 0 {
        PipelineDecision::NoMoreCandidates
    } else {
        PipelineDecision::PromoteMoreSources
    }
}

#[cfg(test)]
mod tests {
    use super::{PipelineDecision, decide_next_pipeline_step};

    /// Confirms reaching the target stops the pipeline.
    #[test]
    fn decision_stops_when_target_is_reached() {
        assert_eq!(
            decide_next_pipeline_step(200, 200, 0),
            PipelineDecision::TargetReached
        );
        assert_eq!(
            decide_next_pipeline_step(201, 200, 5),
            PipelineDecision::TargetReached
        );
    }

    /// Confirms a short corpus with promoted candidates starts another round.
    #[test]
    fn decision_promotes_when_target_is_short_and_candidates_exist() {
        assert_eq!(
            decide_next_pipeline_step(199, 200, 1),
            PipelineDecision::PromoteMoreSources
        );
    }

    /// Confirms a short corpus with no candidates stops cleanly.
    #[test]
    fn decision_stops_when_no_more_candidates_exist() {
        assert_eq!(
            decide_next_pipeline_step(199, 200, 0),
            PipelineDecision::NoMoreCandidates
        );
    }
}
