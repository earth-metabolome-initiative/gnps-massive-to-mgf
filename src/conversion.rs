//! mzML to sharded MGF conversion.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use indicatif::ProgressBar;
use mascot_rs::prelude::{
    Instrument as MascotInstrument, IonMode as MascotIonMode, MascotGenericFormat,
    MascotGenericFormatMetadata, SpectrumSplash,
};
use mzdata::MzMLReader;
use mzdata::params::{ControlledVocabulary, Param, ParamDescribed, ParamValue, Unit};
use mzdata::prelude::{
    IonMobilityMeasure, IonProperties, MZFileReader, PrecursorSelection,
    SpectrumLike as MzSpectrumLike,
};
use mzdata::spectrum::{IsolationWindowState, MultiLayerSpectrum, ScanPolarity};
use serde::Serialize;

use crate::checksum::sha256_file;
use crate::config::Config;
use crate::db::{
    FinalizedShardRow, SourceConversionRecord, SourceFileRow, SpectrumSeenRecord, StateDb,
    spectrum_dedupe_key,
};
use crate::progress::ProgressReporter;

/// Minimum number of fragment peaks retained for a writable MGF record.
const MIN_FRAGMENT_PEAKS: usize = 2;
/// Number of source files loaded from the state DB per conversion batch.
const CONVERSION_BATCH_SIZE: usize = 1;
/// Conversion report file name.
const CONVERSION_REPORT: &str = "conversion_report.json";
/// Dataset README file name.
const DATASET_README: &str = "README.md";
/// Manifest file name.
const MANIFEST: &str = "manifest.csv";

/// Full conversion summary.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConversionSummary {
    /// Number of source files visited.
    pub files_visited: u64,
    /// Number of source files converted successfully.
    pub files_converted: u64,
    /// Number of source files that failed conversion.
    pub files_failed: u64,
    /// Number of unique MGF records written.
    pub spectra_written: u64,
    /// Number of duplicate `(SPLASH, PEPMASS)` records skipped.
    pub duplicates_skipped: u64,
    /// Number of MS/MS spectra skipped because precursor metadata was missing.
    pub missing_precursor_skipped: u64,
    /// Number of MS/MS spectra skipped because too few valid fragment peaks survived filtering.
    pub low_peak_skipped: u64,
    /// Number of compressed MGF shards written.
    pub shards_written: u64,
    /// Configured top-k fragment peak cap.
    pub top_k_peaks: usize,
    /// Observed MSn counters across all visited source files.
    pub msn_counts: BTreeMap<u8, u64>,
}

/// Summary for one generated MGF shard.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct ShardSummary {
    /// Zero-based shard index.
    pub shard_index: u64,
    /// Shard path.
    pub path: PathBuf,
    /// Number of MGF records written.
    pub spectra_written: u64,
    /// Number of compressed bytes written.
    pub bytes_written: u64,
    /// SHA256 checksum of the compressed shard.
    pub sha256: String,
}

/// Per-source conversion counters.
#[derive(Debug, Default, Clone, Serialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct SourceConversionStats {
    /// Number of MS1 spectra observed.
    pub ms1_count: u64,
    /// Number of MS2 spectra observed.
    pub ms2_count: u64,
    /// Observed MSn counters.
    pub msn_counts: BTreeMap<u8, u64>,
    /// Number of unique MGF records written.
    pub spectra_written: u64,
    /// Number of duplicate records skipped.
    pub duplicates_skipped: u64,
    /// MS/MS spectra skipped because precursor metadata was missing.
    pub missing_precursor_skipped: u64,
    /// MS/MS spectra skipped because too few peaks survived filtering.
    pub low_peak_skipped: u64,
}

/// Converts all downloaded mzML files that are not converted for the configured top-k.
///
/// # Errors
///
/// Returns an error if output directories cannot be created or sidecar reports
/// cannot be written. Per-file conversion errors are recorded in the state DB
/// and do not stop later files.
pub fn convert_downloaded_mzml(
    db: &mut StateDb,
    config: &Config,
    progress: &ProgressReporter,
) -> anyhow::Result<ConversionSummary> {
    fs::create_dir_all(&config.mgf_output_dir)
        .with_context(|| format!("failed to create {}", config.mgf_output_dir.display()))?;
    cleanup_stale_temporary_shards(&config.mgf_output_dir, config.top_k_peaks)?;
    quarantine_untracked_final_shards(db, &config.mgf_output_dir, config.top_k_peaks)?;
    let mut summary = ConversionSummary {
        files_visited: 0,
        files_converted: 0,
        files_failed: 0,
        spectra_written: 0,
        duplicates_skipped: 0,
        missing_precursor_skipped: 0,
        low_peak_skipped: 0,
        shards_written: 0,
        top_k_peaks: config.top_k_peaks,
        msn_counts: BTreeMap::new(),
    };
    let mut writer = ShardWriterSet::new(
        db,
        &config.mgf_output_dir,
        config.top_k_peaks,
        config.mgf_shard_max_spectra,
    )?;
    let mut progress_bars = ConversionProgressBars::new(db, config, progress)?;

    loop {
        let batch = db.downloaded_unconverted(config.top_k_peaks, CONVERSION_BATCH_SIZE)?;
        if batch.is_empty() {
            break;
        }
        for source in batch {
            summary.files_visited = summary.files_visited.saturating_add(1);
            db.mark_conversion_running(&source.filepath, config.top_k_peaks)?;
            let result = convert_source_file(db, config, progress, &source, &mut writer);
            match result {
                Ok(stats) => {
                    db.mark_converted(
                        &source.filepath,
                        config.top_k_peaks,
                        &SourceConversionRecord {
                            ms1_count: stats.ms1_count,
                            ms2_count: stats.ms2_count,
                            msn_counts_json: serde_json::to_string(&stats.msn_counts)?,
                            converted_spectra: stats.spectra_written,
                            duplicate_spectra: stats.duplicates_skipped,
                            missing_precursor_skipped: stats.missing_precursor_skipped,
                            low_peak_skipped: stats.low_peak_skipped,
                        },
                    )?;
                    summary.files_converted = summary.files_converted.saturating_add(1);
                    merge_stats(&mut summary, &stats);
                    progress_bars.record_converted(&stats);
                }
                Err(error) => {
                    db.mark_conversion_failed(
                        &source.filepath,
                        config.top_k_peaks,
                        &format!("{error:#}"),
                    )?;
                    summary.files_failed = summary.files_failed.saturating_add(1);
                    progress.println(format!(
                        "conversion failed: {} | {error:#}",
                        source.filepath
                    ))?;
                    progress_bars.record_failed();
                }
            }
        }
    }

    writer.finish(db)?;
    let shards = finalized_shard_summaries(db, config.top_k_peaks)?;
    let db_summary = db.conversion_summary(config.top_k_peaks)?;
    summary.files_converted = db_summary.files_converted;
    summary.files_failed = db_summary.files_failed;
    summary.spectra_written = db_summary.spectra_written;
    summary.duplicates_skipped = db_summary.duplicates_skipped;
    summary.missing_precursor_skipped = db_summary.missing_precursor_skipped;
    summary.low_peak_skipped = db_summary.low_peak_skipped;
    summary.shards_written = db_summary.shards_written;
    summary.files_visited = summary.files_converted.saturating_add(summary.files_failed);
    summary.msn_counts = aggregate_msn_counts(db.converted_msn_counts_json(config.top_k_peaks)?)?;
    progress_bars.finish(&summary);
    write_manifest(&config.mgf_output_dir, &shards)?;
    write_conversion_report(&config.mgf_output_dir, &summary)?;
    write_dataset_readme(&config.mgf_output_dir, &summary)?;
    Ok(summary)
}

struct ConversionProgressBars {
    files_bar: Option<ProgressBar>,
    spectra_bar: Option<ProgressBar>,
    total_files: u64,
    processed_files: u64,
    converted_this_run: u64,
    failed_this_run: u64,
    unique_spectra: u64,
    target_spectra: u64,
}

impl ConversionProgressBars {
    fn new(db: &mut StateDb, config: &Config, progress: &ProgressReporter) -> anyhow::Result<Self> {
        let total_files = db.count_downloaded_unconverted(config.top_k_peaks)?;
        let initial_summary = db.conversion_summary(config.top_k_peaks)?;
        let files_bar = (total_files > 0)
            .then(|| {
                progress.count_bar(
                    total_files,
                    format!("conversion files | remaining={total_files}"),
                )
            })
            .transpose()?;
        let spectra_bar = (config.target_ms2_spectra > 0)
            .then(|| {
                progress.count_bar(
                    config.target_ms2_spectra,
                    format!(
                        "unique MS/MS spectra | {}/{}",
                        initial_summary.spectra_written, config.target_ms2_spectra
                    ),
                )
            })
            .transpose()?;
        if let Some(bar) = &spectra_bar {
            bar.set_position(
                initial_summary
                    .spectra_written
                    .min(config.target_ms2_spectra),
            );
        }
        Ok(Self {
            files_bar,
            spectra_bar,
            total_files,
            processed_files: 0,
            converted_this_run: 0,
            failed_this_run: 0,
            unique_spectra: initial_summary.spectra_written,
            target_spectra: config.target_ms2_spectra,
        })
    }

    fn record_converted(&mut self, stats: &SourceConversionStats) {
        self.converted_this_run = self.converted_this_run.saturating_add(1);
        self.unique_spectra = self.unique_spectra.saturating_add(stats.spectra_written);
        if let Some(bar) = &self.spectra_bar {
            bar.set_position(self.unique_spectra.min(self.target_spectra));
            bar.set_message(format!(
                "unique MS/MS spectra | {}/{}",
                self.unique_spectra, self.target_spectra
            ));
        }
        self.record_file();
    }

    fn record_failed(&mut self) {
        self.failed_this_run = self.failed_this_run.saturating_add(1);
        self.record_file();
    }

    fn record_file(&mut self) {
        self.processed_files = self.processed_files.saturating_add(1);
        if let Some(bar) = &self.files_bar {
            let remaining = self.total_files.saturating_sub(self.processed_files);
            bar.inc(1);
            bar.set_message(format!(
                "conversion files | converted={} failed={} remaining={remaining}",
                self.converted_this_run, self.failed_this_run
            ));
        }
    }

    fn finish(self, summary: &ConversionSummary) {
        if let Some(bar) = self.files_bar {
            bar.finish_with_message(format!(
                "conversion files | converted_this_run={} failed_this_run={}",
                self.converted_this_run, self.failed_this_run
            ));
        }
        if let Some(bar) = self.spectra_bar {
            bar.set_position(summary.spectra_written.min(self.target_spectra));
            bar.finish_with_message(format!(
                "unique MS/MS spectra | {}/{}",
                summary.spectra_written, self.target_spectra
            ));
        }
    }
}

/// Converts one source mzML file.
fn convert_source_file(
    db: &mut StateDb,
    config: &Config,
    progress: &ProgressReporter,
    source: &SourceFileRow,
    writer: &mut ShardWriterSet,
) -> anyhow::Result<SourceConversionStats> {
    let path = source.local_path_buf();
    let mut reader = MzMLReader::open_path(&path)
        .with_context(|| format!("failed to open mzML file {}", path.display()))?;
    let expected = source.spectra_ms2.and_then(f64_to_u64).unwrap_or_default();
    let bar = progress.row_bar(expected, format!("converting {}", source.filepath))?;
    let mut stats = SourceConversionStats::default();
    let mut last_ms1 = None;

    for mut spectrum in &mut reader {
        let level = spectrum.ms_level();
        *stats.msn_counts.entry(level).or_default() += 1;
        if level == 1 {
            stats.ms1_count = stats.ms1_count.saturating_add(1);
            last_ms1 = Some(Ms1Context::from_spectrum(&spectrum));
            continue;
        }
        if level != 2 {
            continue;
        }
        stats.ms2_count = stats.ms2_count.saturating_add(1);
        bar.inc(1);
        let candidate = match prepare_ms2_record(&mut spectrum, source, last_ms1.as_ref(), config)?
        {
            PreparedSpectrum::Ready(candidate) => *candidate,
            PreparedSpectrum::MissingPrecursor => {
                stats.missing_precursor_skipped = stats.missing_precursor_skipped.saturating_add(1);
                continue;
            }
            PreparedSpectrum::LowPeakCount => {
                stats.low_peak_skipped = stats.low_peak_skipped.saturating_add(1);
                continue;
            }
        };
        let mut record = candidate.record;
        let splash = SpectrumSplash::splash(&record)?;
        writer.rotate_if_full(db)?;
        let dedupe_key = spectrum_dedupe_key(&splash, candidate.precursor_mz);
        if db.spectrum_seen(config.top_k_peaks, &splash, candidate.precursor_mz)?
            || writer.contains_pending(&dedupe_key)
        {
            stats.duplicates_skipped = stats.duplicates_skipped.saturating_add(1);
            continue;
        }
        record
            .metadata_mut()
            .insert_arbitrary_metadata("SPLASH", splash.clone());
        writer.write_record(
            &record,
            dedupe_key,
            SpectrumSeenRecord {
                splash,
                pepmass: candidate.precursor_mz,
                source_filepath: source.filepath.clone(),
                spectrum_id: candidate.spectrum_id,
            },
            db,
        )?;
        stats.spectra_written = stats.spectra_written.saturating_add(1);
    }
    bar.finish_with_message(format!(
        "converted {} | written={} duplicates={}",
        source.filepath, stats.spectra_written, stats.duplicates_skipped
    ));
    Ok(stats)
}

/// Prepares a single MS2 MGF record.
fn prepare_ms2_record(
    spectrum: &mut MultiLayerSpectrum,
    source: &SourceFileRow,
    ms1_context: Option<&Ms1Context>,
    config: &Config,
) -> anyhow::Result<PreparedSpectrum> {
    let Some(precursor_metadata) = precursor_metadata(spectrum) else {
        return Ok(PreparedSpectrum::MissingPrecursor);
    };
    let peaks = top_k_peaks(spectrum, config.top_k_peaks)?;
    if peaks.len() < MIN_FRAGMENT_PEAKS {
        return Ok(PreparedSpectrum::LowPeakCount);
    }
    let (mzs, intensities): (Vec<_>, Vec<_>) = peaks.into_iter().unzip();
    let ion_mode = ion_mode(spectrum.polarity());
    let signed_charge = signed_charge(precursor_metadata.charge, spectrum.polarity())?;
    let mut metadata = MascotGenericFormatMetadata::new_with_smiles_and_ion_mode(
        Some(format!("{}:{}", source.dataset, spectrum.index())),
        2,
        retention_seconds(spectrum),
        signed_charge,
        Some(source.filepath.clone()),
        None,
        ion_mode,
    )?
    .with_scans(Some(spectrum.id().to_owned()))
    .with_source_instrument(source.instrument_model.parse::<MascotInstrument>().ok())
    .with_arbitrary_metadata(arbitrary_metadata(
        spectrum,
        source,
        ms1_context,
        precursor_metadata.selected_ion_intensity,
        precursor_metadata.precursor_id.as_deref(),
    ));
    metadata.insert_arbitrary_metadata("TOP_K_PEAKS", config.top_k_peaks.to_string());
    let record = MascotGenericFormat::new(metadata, precursor_metadata.mz, mzs, intensities)?;
    Ok(PreparedSpectrum::Ready(Box::new(PreparedReadySpectrum {
        record,
        precursor_mz: precursor_metadata.mz,
        spectrum_id: spectrum.id().to_owned(),
    })))
}

/// Precursor metadata needed to write an MGF record.
struct PrecursorMetadata {
    /// Precursor m/z.
    mz: f64,
    /// Precursor charge.
    charge: Option<i32>,
    /// Selected-ion intensity.
    selected_ion_intensity: Option<f32>,
    /// Precursor spectrum identifier.
    precursor_id: Option<String>,
}

/// Extracts precursor metadata needed for an MGF record.
fn precursor_metadata(spectrum: &MultiLayerSpectrum) -> Option<PrecursorMetadata> {
    let precursor = spectrum.precursor()?;
    let ion = precursor.ion()?;
    let mz = ion.mz();
    if !(mz.is_finite() && mz > 0.0) {
        return None;
    }
    Some(PrecursorMetadata {
        mz,
        charge: ion.charge(),
        selected_ion_intensity: Some(ion.intensity),
        precursor_id: precursor.precursor_id().cloned(),
    })
}

/// Extracts, filters, ranks, and m/z-sorts the top-k fragment peaks.
fn top_k_peaks(spectrum: &mut MultiLayerSpectrum, top_k: usize) -> anyhow::Result<Vec<(f64, f64)>> {
    spectrum
        .try_build_peaks()
        .context("failed to build mzML peak layer")?;
    let mut peaks = spectrum
        .peaks()
        .iter()
        .filter_map(|peak| {
            let mz = peak.mz;
            let intensity = f64::from(peak.intensity);
            (mz.is_finite() && mz > 0.0 && intensity.is_finite() && intensity > 0.0)
                .then_some((mz, intensity))
        })
        .collect::<Vec<_>>();
    peaks.sort_by(|left, right| right.1.total_cmp(&left.1));
    peaks.truncate(top_k);
    peaks.sort_by(|left, right| left.0.total_cmp(&right.0));
    Ok(peaks)
}

/// Builds arbitrary metadata carried into the MGF header.
fn arbitrary_metadata(
    spectrum: &MultiLayerSpectrum,
    source: &SourceFileRow,
    ms1_context: Option<&Ms1Context>,
    selected_ion_intensity: Option<f32>,
    precursor_id: Option<&str>,
) -> Vec<(String, String)> {
    let mut metadata = BTreeMap::new();
    insert_metadata(&mut metadata, "SOURCE_DATASET", &source.dataset);
    insert_metadata(&mut metadata, "SOURCE_COLLECTION", &source.collection);
    insert_metadata(&mut metadata, "SOURCE_PATH", &source.filepath);
    insert_metadata(&mut metadata, "SOURCE_SIZE", source.size_bytes.to_string());
    insert_metadata(&mut metadata, "SOURCE_CREATE_TIME", &source.create_time);
    insert_metadata(
        &mut metadata,
        "INSTRUMENT_VENDOR",
        &source.instrument_vendor,
    );
    insert_metadata(&mut metadata, "INSTRUMENT_MODEL", &source.instrument_model);
    insert_metadata(&mut metadata, "SOURCE_EXTENSION", &source.extension);
    insert_metadata(&mut metadata, "SOURCE_SPECTRUM_ID", spectrum.id());
    insert_metadata(
        &mut metadata,
        "SOURCE_SPECTRUM_INDEX",
        spectrum.index().to_string(),
    );
    if let Some(precursor_id) = precursor_id {
        insert_metadata(&mut metadata, "PRECURSOR_SCAN_ID", precursor_id);
    }
    if let Some(intensity) = selected_ion_intensity {
        insert_metadata(
            &mut metadata,
            "PRECURSOR_SELECTED_ION_INTENSITY",
            intensity.to_string(),
        );
    }
    if let Some(ms1) = ms1_context {
        insert_metadata(&mut metadata, "ASSOCIATED_MS1_ID", &ms1.id);
        insert_metadata(&mut metadata, "ASSOCIATED_MS1_INDEX", ms1.index.to_string());
        if let Some(seconds) = ms1.retention_time_seconds {
            insert_metadata(
                &mut metadata,
                "ASSOCIATED_MS1_RTINSECONDS",
                seconds.to_string(),
            );
        }
    }
    append_structured_mzml_metadata(&mut metadata, spectrum);
    metadata.into_iter().collect()
}

/// Adds mzML metadata that should survive conversion to MGF headers.
fn append_structured_mzml_metadata(
    metadata: &mut BTreeMap<String, String>,
    spectrum: &MultiLayerSpectrum,
) {
    insert_metadata(
        metadata,
        "SIGNAL_CONTINUITY",
        spectrum.signal_continuity().to_string(),
    );
    append_known_params(metadata, spectrum.params());
    append_known_params(metadata, spectrum.acquisition().params());
    append_scan_metadata(metadata, spectrum);
    if let Some(precursor) = spectrum.precursor() {
        append_precursor_mzml_metadata(metadata, precursor);
    }
}

/// Adds scan-level mzML metadata to the MGF header map.
fn append_scan_metadata(metadata: &mut BTreeMap<String, String>, spectrum: &MultiLayerSpectrum) {
    let acquisition = spectrum.acquisition();
    for (scan_index, scan) in acquisition.iter().enumerate() {
        if scan.injection_time.is_finite() && scan.injection_time > 0.0 {
            insert_metadata(
                metadata,
                scan_key("INJECTION_TIME_MS", acquisition.len(), scan_index),
                scan.injection_time.to_string(),
            );
        }
        if let Some(filter_string) = scan.filter_string() {
            insert_metadata(
                metadata,
                scan_key("FILTER_STRING", acquisition.len(), scan_index),
                filter_string.as_ref(),
            );
        }
        if let Some(ion_mobility_type) = scan.ion_mobility_type() {
            append_known_param(metadata, ion_mobility_type);
        } else if let Some(ion_mobility) = scan.ion_mobility() {
            insert_metadata(
                metadata,
                scan_key("ION_MOBILITY", acquisition.len(), scan_index),
                ion_mobility.to_string(),
            );
        }
        append_scan_windows(metadata, acquisition.len(), scan_index, &scan.scan_windows);
        append_known_params(metadata, scan.params());
    }
}

/// Adds scan-window bounds to the MGF header map.
fn append_scan_windows(
    metadata: &mut BTreeMap<String, String>,
    scan_count: usize,
    scan_index: usize,
    scan_windows: &[mzdata::spectrum::ScanWindow],
) {
    for (window_index, window) in scan_windows.iter().enumerate() {
        if window.lower_bound.is_finite() && window.lower_bound > 0.0 {
            insert_metadata(
                metadata,
                scan_window_key(
                    "SCAN_WINDOW_LOWER_MZ",
                    scan_count,
                    scan_index,
                    scan_windows.len(),
                    window_index,
                ),
                window.lower_bound.to_string(),
            );
        }
        if window.upper_bound.is_finite() && window.upper_bound > 0.0 {
            insert_metadata(
                metadata,
                scan_window_key(
                    "SCAN_WINDOW_UPPER_MZ",
                    scan_count,
                    scan_index,
                    scan_windows.len(),
                    window_index,
                ),
                window.upper_bound.to_string(),
            );
        }
    }
}

/// Adds precursor isolation, activation, and selected-ion metadata to the MGF header map.
fn append_precursor_mzml_metadata(
    metadata: &mut BTreeMap<String, String>,
    precursor: &mzdata::spectrum::Precursor,
) {
    let activation = precursor.activation();
    if activation.energy.is_finite() && activation.energy > 0.0 {
        insert_metadata(metadata, "COLLISION_ENERGY", activation.energy.to_string());
    }
    if let Some(methods) = joined_metadata_values(
        activation
            .methods()
            .iter()
            .map(mzdata::meta::DissociationMethodTerm::name),
    ) {
        insert_metadata(metadata, "ACTIVATION_METHOD", methods);
    }
    append_known_params(metadata, activation.params());

    append_isolation_window_metadata(metadata, precursor.isolation_window());
    if let Some(product_id) = precursor.product_id() {
        insert_metadata(metadata, "PRODUCT_SCAN_ID", product_id);
    }

    for selected_ion in precursor.iter() {
        if let Some(ion_mobility_type) = selected_ion.ion_mobility_type() {
            append_known_param(metadata, ion_mobility_type);
        }
        append_known_params(metadata, selected_ion.params());
    }
}

/// Adds precursor isolation-window metadata to the MGF header map.
fn append_isolation_window_metadata(
    metadata: &mut BTreeMap<String, String>,
    isolation_window: &mzdata::spectrum::IsolationWindow,
) {
    if isolation_window.is_empty() {
        return;
    }
    insert_metadata(
        metadata,
        "ISOLATION_WINDOW_STATE",
        isolation_window.flags.to_string(),
    );
    if isolation_window.target.is_finite() && isolation_window.target > 0.0 {
        insert_metadata(
            metadata,
            "ISOLATION_WINDOW_TARGET_MZ",
            isolation_window.target.to_string(),
        );
    }
    match isolation_window.flags {
        IsolationWindowState::Offset => {
            insert_metadata(
                metadata,
                "ISOLATION_WINDOW_LOWER_OFFSET",
                isolation_window.lower_bound.to_string(),
            );
            insert_metadata(
                metadata,
                "ISOLATION_WINDOW_UPPER_OFFSET",
                isolation_window.upper_bound.to_string(),
            );
        }
        IsolationWindowState::Unknown
        | IsolationWindowState::Explicit
        | IsolationWindowState::Complete => {
            if isolation_window.lower_bound.is_finite() && isolation_window.lower_bound > 0.0 {
                insert_metadata(
                    metadata,
                    "ISOLATION_WINDOW_LOWER_MZ",
                    isolation_window.lower_bound.to_string(),
                );
            }
            if isolation_window.upper_bound.is_finite() && isolation_window.upper_bound > 0.0 {
                insert_metadata(
                    metadata,
                    "ISOLATION_WINDOW_UPPER_MZ",
                    isolation_window.upper_bound.to_string(),
                );
            }
        }
    }
}

/// Adds known mzML params using canonical MGF header names.
fn append_known_params(metadata: &mut BTreeMap<String, String>, params: &[Param]) {
    for param in params {
        append_known_param(metadata, param);
    }
}

/// Adds a known mzML param using its canonical MGF header name.
fn append_known_param(metadata: &mut BTreeMap<String, String>, param: &Param) {
    let Some(key) = canonical_mzml_metadata_header(param) else {
        return;
    };
    let Some(value) = param_metadata_value(param) else {
        return;
    };
    insert_metadata(metadata, key, value);
    if param.unit != Unit::Unknown {
        insert_metadata(metadata, format!("{key}_UNIT"), param.unit.to_string());
    }
}

/// Returns the canonical MGF header for a known mzML param.
fn canonical_mzml_metadata_header(param: &Param) -> Option<&'static str> {
    if param.controlled_vocabulary == Some(ControlledVocabulary::MS)
        && let Some(header) = canonical_ms_accession_metadata_header(param.accession?)
    {
        return Some(header);
    }
    canonical_metadata_name_alias(&normalized_metadata_name(&param.name))
}

/// Returns the canonical MGF header for important PSI-MS accessions.
const fn canonical_ms_accession_metadata_header(accession: u32) -> Option<&'static str> {
    match accession {
        1_000_011 => Some("MASS_RESOLUTION"),
        1_000_016 => Some("SCAN_START_TIME"),
        1_000_041 => Some("PRECURSOR_CHARGE"),
        1_000_042 => Some("PRECURSOR_SELECTED_ION_INTENSITY"),
        1_000_045 => Some("COLLISION_ENERGY"),
        1_000_285 => Some("TOTAL_ION_CURRENT"),
        1_000_499 => Some("SCAN_TITLE"),
        1_000_500 => Some("SCAN_WINDOW_UPPER_MZ"),
        1_000_501 => Some("SCAN_WINDOW_LOWER_MZ"),
        1_000_504 => Some("BASE_PEAK_MZ"),
        1_000_505 => Some("BASE_PEAK_INTENSITY"),
        1_000_512 => Some("FILTER_STRING"),
        1_000_527 => Some("HIGHEST_OBSERVED_MZ"),
        1_000_528 => Some("LOWEST_OBSERVED_MZ"),
        1_000_616 => Some("PRESET_SCAN_CONFIGURATION"),
        1_000_744 => Some("PRECURSOR_SELECTED_ION_MZ"),
        1_000_827 => Some("ISOLATION_WINDOW_TARGET_MZ"),
        1_000_828 => Some("ISOLATION_WINDOW_LOWER_OFFSET"),
        1_000_829 => Some("ISOLATION_WINDOW_UPPER_OFFSET"),
        1_000_927 => Some("INJECTION_TIME_MS"),
        1_000_138 => Some("NORMALIZED_COLLISION_ENERGY"),
        1_001_581 => Some("COMPENSATION_VOLTAGE"),
        1_002_013 => Some("COLLISION_ENERGY_RAMP_START"),
        1_002_014 => Some("COLLISION_ENERGY_RAMP_END"),
        1_002_218 => Some("PERCENT_COLLISION_ENERGY_RAMP_START"),
        1_002_219 => Some("PERCENT_COLLISION_ENERGY_RAMP_END"),
        1_002_476 => Some("ION_MOBILITY_DRIFT_TIME"),
        1_002_680 => Some("SUPPLEMENTAL_COLLISION_ENERGY"),
        1_002_815 => Some("INVERSE_REDUCED_ION_MOBILITY"),
        1_003_371 => Some("SELEXION_COMPENSATION_VOLTAGE"),
        1_003_410 => Some("ELECTRON_BEAM_ENERGY"),
        _ => None,
    }
}

/// Returns the canonical MGF header for known non-standard name spellings.
fn canonical_metadata_name_alias(normalized_name: &str) -> Option<&'static str> {
    match normalized_name {
        "activationenergy" | "ce" | "collisionenergy" | "collisionenergysetting" => {
            Some("COLLISION_ENERGY")
        }
        "nce" | "ncesetting" | "normalizedce" | "normalizedcollisionenergy" => {
            Some("NORMALIZED_COLLISION_ENERGY")
        }
        "supplementalce" | "supplementalcollisionenergy" => Some("SUPPLEMENTAL_COLLISION_ENERGY"),
        "collisionenergyrampstart" => Some("COLLISION_ENERGY_RAMP_START"),
        "collisionenergyrampend" => Some("COLLISION_ENERGY_RAMP_END"),
        "percentcollisionenergyrampstart" => Some("PERCENT_COLLISION_ENERGY_RAMP_START"),
        "percentcollisionenergyrampend" => Some("PERCENT_COLLISION_ENERGY_RAMP_END"),
        "electronbeamenergy" => Some("ELECTRON_BEAM_ENERGY"),
        "filterstring" | "scanfilterstring" => Some("FILTER_STRING"),
        "injectiontime" | "ioninjectiontime" | "maximuminjectiontime" | "maxioninjectiontime" => {
            Some("INJECTION_TIME_MS")
        }
        "drifttime" | "ionmobility" | "ionmobilitydrifttime" => Some("ION_MOBILITY_DRIFT_TIME"),
        "inversereducedionmobility" | "oneoverk0" | "ook0" => Some("INVERSE_REDUCED_ION_MOBILITY"),
        "compensationvoltage" | "faimscompensationvoltage" => Some("COMPENSATION_VOLTAGE"),
        "selexioncompensationvoltage" => Some("SELEXION_COMPENSATION_VOLTAGE"),
        "isolationwindowtargetmz" => Some("ISOLATION_WINDOW_TARGET_MZ"),
        "isolationwindowloweroffset" => Some("ISOLATION_WINDOW_LOWER_OFFSET"),
        "isolationwindowupperoffset" => Some("ISOLATION_WINDOW_UPPER_OFFSET"),
        "isolationwindowlowerlimit" => Some("ISOLATION_WINDOW_LOWER_MZ"),
        "isolationwindowupperlimit" => Some("ISOLATION_WINDOW_UPPER_MZ"),
        "scanwindowlowerlimit" => Some("SCAN_WINDOW_LOWER_MZ"),
        "scanwindowupperlimit" => Some("SCAN_WINDOW_UPPER_MZ"),
        "basepeakmz" => Some("BASE_PEAK_MZ"),
        "basepeakintensity" => Some("BASE_PEAK_INTENSITY"),
        "totalioncurrent" => Some("TOTAL_ION_CURRENT"),
        "lowestobservedmz" => Some("LOWEST_OBSERVED_MZ"),
        "highestobservedmz" => Some("HIGHEST_OBSERVED_MZ"),
        "massresolution" => Some("MASS_RESOLUTION"),
        "scantitle" | "spectrumtitle" => Some("SCAN_TITLE"),
        "adduct" | "precursoradduct" => Some("ADDUCT"),
        "compoundname" | "name" | "spectrumname" => Some("NAME"),
        "formula" | "molecularformula" => Some("FORMULA"),
        "inchi" => Some("INCHI"),
        "inchikey" => Some("INCHIKEY"),
        "peptidesequence" | "sequence" => Some("PEPTIDE_SEQUENCE"),
        "smiles" => Some("SMILES"),
        _ => None,
    }
}

/// Normalizes metadata names for alias lookup.
fn normalized_metadata_name(name: &str) -> String {
    name.chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect()
}

/// Returns an MGF-safe string value for a mzML parameter.
fn param_metadata_value(param: &Param) -> Option<String> {
    if param.is_empty() {
        return None;
    }
    non_empty_metadata_value(param.value.to_string())
}

/// Inserts one sanitized MGF metadata header value.
fn insert_metadata(
    metadata: &mut BTreeMap<String, String>,
    key: impl Into<String>,
    value: impl AsRef<str>,
) {
    let Some(value) = non_empty_metadata_value(value.as_ref()) else {
        return;
    };
    match metadata.entry(key.into()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(value);
        }
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            if entry.get() != &value && !entry.get().split(';').any(|seen| seen == value) {
                entry.get_mut().push(';');
                entry.get_mut().push_str(&value);
            }
        }
    }
}

/// Returns a cleaned metadata value, dropping empty strings.
fn non_empty_metadata_value(value: impl AsRef<str>) -> Option<String> {
    let value = value.as_ref().replace(['\r', '\n'], " ");
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

/// Joins non-empty metadata values with a semicolon.
fn joined_metadata_values<'a>(values: impl IntoIterator<Item = &'a str>) -> Option<String> {
    let mut joined = String::new();
    for value in values {
        let Some(value) = non_empty_metadata_value(value) else {
            continue;
        };
        if !joined.is_empty() {
            joined.push(';');
        }
        joined.push_str(&value);
    }
    (!joined.is_empty()).then_some(joined)
}

/// Returns an indexed scan-level metadata key when more than one scan exists.
fn scan_key(base: &str, scan_count: usize, scan_index: usize) -> String {
    if scan_count <= 1 {
        base.to_owned()
    } else {
        format!("SCAN_{}_{}", scan_index + 1, base)
    }
}

/// Returns an indexed scan-window metadata key when more than one window exists.
fn scan_window_key(
    base: &str,
    scan_count: usize,
    scan_index: usize,
    window_count: usize,
    window_index: usize,
) -> String {
    if scan_count <= 1 && window_count <= 1 {
        base.to_owned()
    } else {
        format!(
            "SCAN_{}_WINDOW_{}_{}",
            scan_index + 1,
            window_index + 1,
            base
        )
    }
}

/// Returns the spectrum retention time in seconds.
fn retention_seconds(spectrum: &MultiLayerSpectrum) -> Option<f64> {
    let minutes = spectrum.start_time();
    (minutes.is_finite() && minutes > 0.0).then_some(minutes * 60.0)
}

/// Maps mzdata polarity onto mascot-rs ion mode metadata.
const fn ion_mode(polarity: ScanPolarity) -> Option<MascotIonMode> {
    match polarity {
        ScanPolarity::Positive => Some(MascotIonMode::Positive),
        ScanPolarity::Negative => Some(MascotIonMode::Negative),
        ScanPolarity::Unknown => None,
    }
}

/// Converts a mzdata charge into the signed charge expected by `mascot-rs`.
fn signed_charge(charge: Option<i32>, polarity: ScanPolarity) -> anyhow::Result<Option<i8>> {
    let Some(charge) = charge else {
        return Ok(None);
    };
    let signed = match polarity {
        ScanPolarity::Negative if charge > 0 => -charge,
        _ => charge,
    };
    Ok(Some(
        i8::try_from(signed).context("precursor charge does not fit i8")?,
    ))
}

/// MGF candidate prepared for deduplication and writing.
enum PreparedSpectrum {
    /// The MS2 spectrum had no usable precursor m/z.
    MissingPrecursor,
    /// The MS2 spectrum had too few usable fragment peaks.
    LowPeakCount,
    /// The MS2 spectrum is ready for deduplication and writing.
    Ready(Box<PreparedReadySpectrum>),
}

/// MGF candidate prepared for deduplication and writing.
struct PreparedReadySpectrum {
    /// MGF record.
    record: MascotGenericFormat,
    /// Precursor m/z.
    precursor_mz: f64,
    /// Source spectrum id.
    spectrum_id: String,
}

/// Lightweight MS1 context attached to later MS2 spectra.
#[derive(Debug, Clone)]
struct Ms1Context {
    /// MS1 native id.
    id: String,
    /// MS1 source index.
    index: usize,
    /// MS1 retention time in seconds.
    retention_time_seconds: Option<f64>,
}

impl Ms1Context {
    /// Builds MS1 context from a spectrum.
    fn from_spectrum(spectrum: &MultiLayerSpectrum) -> Self {
        Self {
            id: spectrum.id().to_owned(),
            index: spectrum.index(),
            retention_time_seconds: retention_seconds(spectrum),
        }
    }
}

/// Removes stale temporary shards for the active top-k setting.
fn cleanup_stale_temporary_shards(output_dir: &Path, top_k_peaks: usize) -> anyhow::Result<()> {
    if !output_dir.exists() {
        return Ok(());
    }
    let prefix = shard_prefix(top_k_peaks);
    for entry in fs::read_dir(output_dir)
        .with_context(|| format!("failed to read {}", output_dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read {}", output_dir.display()))?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.starts_with(&prefix) && name.ends_with(".mgf.zst.tmp") {
            fs::remove_file(&path)
                .with_context(|| format!("failed to remove stale {}", path.display()))?;
        }
    }
    Ok(())
}

/// Quarantines finalized shard-looking files that are not recorded in SQLite.
fn quarantine_untracked_final_shards(
    db: &mut StateDb,
    output_dir: &Path,
    top_k_peaks: usize,
) -> anyhow::Result<()> {
    if !output_dir.exists() {
        return Ok(());
    }
    let prefix = shard_prefix(top_k_peaks);
    let tracked = db
        .finalized_shards(top_k_peaks)?
        .into_iter()
        .map(|row| row.shard_path)
        .collect::<HashSet<_>>();
    for entry in fs::read_dir(output_dir)
        .with_context(|| format!("failed to read {}", output_dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read {}", output_dir.display()))?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if !(name.starts_with(&prefix) && name.ends_with(".mgf.zst")) {
            continue;
        }
        let path_string = path.to_string_lossy().into_owned();
        if tracked.contains(&path_string) {
            continue;
        }
        let quarantine = quarantine_path(&path);
        fs::rename(&path, &quarantine).with_context(|| {
            format!(
                "failed to quarantine untracked shard {} as {}",
                path.display(),
                quarantine.display()
            )
        })?;
    }
    Ok(())
}

/// Builds summaries from finalized shard rows.
fn finalized_shard_summaries(
    db: &mut StateDb,
    top_k_peaks: usize,
) -> anyhow::Result<Vec<ShardSummary>> {
    db.finalized_shards(top_k_peaks)?
        .into_iter()
        .map(finalized_shard_summary)
        .collect()
}

/// Converts a database shard row into a conversion shard summary.
fn finalized_shard_summary(row: FinalizedShardRow) -> anyhow::Result<ShardSummary> {
    Ok(ShardSummary {
        shard_index: u64::try_from(row.shard_index).context("shard index does not fit u64")?,
        path: PathBuf::from(row.shard_path),
        spectra_written: u64::try_from(row.spectra_written)
            .context("spectra_written does not fit u64")?,
        bytes_written: u64::try_from(row.bytes_written)
            .context("bytes_written does not fit u64")?,
        sha256: row.sha256.unwrap_or_default(),
    })
}

/// Returns the shard filename prefix for a top-k setting.
fn shard_prefix(top_k_peaks: usize) -> String {
    format!("massive_ms2.top-{top_k_peaks:04}.part-")
}

/// Builds the temporary path for an active shard.
fn temporary_shard_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".tmp");
    PathBuf::from(value)
}

/// Builds a non-clobbering quarantine path for an untracked shard.
fn quarantine_path(path: &Path) -> PathBuf {
    let mut index = 0_u64;
    loop {
        let mut value = path.as_os_str().to_owned();
        if index == 0 {
            value.push(".orphaned");
        } else {
            value.push(format!(".orphaned-{index}"));
        }
        let candidate = PathBuf::from(value);
        if !candidate.exists() {
            return candidate;
        }
        index = index.saturating_add(1);
    }
}

/// Rollover-aware collection of compressed MGF shard writers.
struct ShardWriterSet {
    /// Output directory.
    output_dir: PathBuf,
    /// Number of fragment peaks retained in generated shards.
    top_k_peaks: usize,
    /// Maximum spectra per shard.
    max_spectra: u64,
    /// Next shard index to create.
    next_index: u64,
    /// Current open shard writer.
    writer: Option<ShardWriter>,
}

impl ShardWriterSet {
    /// Creates a new shard writer set.
    fn new(
        db: &mut StateDb,
        output_dir: &Path,
        top_k_peaks: usize,
        max_spectra: u64,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            output_dir: output_dir.to_path_buf(),
            top_k_peaks,
            max_spectra,
            next_index: db.next_shard_index(top_k_peaks)?,
            writer: None,
        })
    }

    /// Returns whether the current temporary shard already has a dedupe key.
    fn contains_pending(&self, key: &str) -> bool {
        self.writer
            .as_ref()
            .is_some_and(|writer| writer.pending_keys.contains(key))
    }

    /// Rotates to a fresh shard when the current shard is full.
    fn rotate_if_full(&mut self, db: &mut StateDb) -> anyhow::Result<()> {
        let is_full = self
            .writer
            .as_ref()
            .is_some_and(|writer| writer.spectra_written >= self.max_spectra);
        if !is_full {
            return Ok(());
        }

        let writer = self.writer.take().context("shard writer is not open")?;
        writer.finish(db, self.top_k_peaks)?;
        Ok(())
    }

    /// Writes one MGF record to the current shard.
    fn write_record(
        &mut self,
        record: &MascotGenericFormat,
        dedupe_key: String,
        seen_record: SpectrumSeenRecord,
        db: &mut StateDb,
    ) -> anyhow::Result<()> {
        self.rotate_if_full(db)?;
        if self.writer.is_none() {
            let shard_index = self.next_index;
            self.next_index = self.next_index.saturating_add(1);
            self.writer = Some(ShardWriter::new(
                &self.output_dir,
                self.top_k_peaks,
                shard_index,
            )?);
        }
        self.writer
            .as_mut()
            .context("shard writer is not open")?
            .write_record(record, dedupe_key, seen_record)
    }

    /// Finalizes the current shard, when one is open.
    fn finish(mut self, db: &mut StateDb) -> anyhow::Result<()> {
        if let Some(writer) = self.writer.take() {
            if writer.spectra_written > 0 {
                writer.finish(db, self.top_k_peaks)?;
            } else {
                writer.discard_empty()?;
            }
        }
        Ok(())
    }
}

/// Compressed MGF shard writer.
struct ShardWriter {
    /// Final shard path.
    path: PathBuf,
    /// Temporary shard path.
    temp_path: PathBuf,
    /// Shard index.
    shard_index: u64,
    /// Number of spectra written to this shard.
    spectra_written: u64,
    /// Deduplication rows pending shard finalization.
    pending_records: Vec<SpectrumSeenRecord>,
    /// Deduplication keys pending shard finalization.
    pending_keys: HashSet<String>,
    /// Zstd encoder.
    encoder: zstd::stream::write::Encoder<'static, BufWriter<File>>,
}

impl ShardWriter {
    /// Creates a new shard writer.
    fn new(output_dir: &Path, top_k_peaks: usize, shard_index: u64) -> anyhow::Result<Self> {
        let path = output_dir.join(format!(
            "massive_ms2.top-{top_k_peaks:04}.part-{shard_index:06}.mgf.zst"
        ));
        if path.exists() {
            bail!("refusing to overwrite finalized shard {}", path.display());
        }
        let temp_path = temporary_shard_path(&path);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .with_context(|| format!("failed to create {}", temp_path.display()))?;
        let writer = BufWriter::new(file);
        let mut encoder = zstd::stream::write::Encoder::new(writer, 3).with_context(|| {
            format!("failed to create zstd encoder for {}", temp_path.display())
        })?;
        encoder
            .multithread(zstd_worker_count())
            .context("failed to enable zstd multithreading")?;
        Ok(Self {
            path,
            temp_path,
            shard_index,
            spectra_written: 0,
            pending_records: Vec::new(),
            pending_keys: HashSet::new(),
            encoder,
        })
    }

    /// Writes one MGF record.
    fn write_record(
        &mut self,
        record: &MascotGenericFormat,
        dedupe_key: String,
        seen_record: SpectrumSeenRecord,
    ) -> anyhow::Result<()> {
        record.write_to(&mut self.encoder)?;
        writeln!(&mut self.encoder).context("failed to write MGF record separator")?;
        self.spectra_written = self.spectra_written.saturating_add(1);
        self.pending_keys.insert(dedupe_key);
        self.pending_records.push(seen_record);
        Ok(())
    }

    /// Finalizes the compressed shard and records it in the state database.
    fn finish(self, db: &mut StateDb, top_k_peaks: usize) -> anyhow::Result<ShardSummary> {
        let path = self.path.clone();
        let temp_path = self.temp_path.clone();
        let shard_index = self.shard_index;
        let spectra_written = self.spectra_written;
        let pending_records = self.pending_records;
        self.encoder
            .finish()
            .with_context(|| format!("failed to finish {}", temp_path.display()))?;
        let bytes_written = fs::metadata(&temp_path)
            .with_context(|| format!("failed to stat {}", temp_path.display()))?
            .len();
        let sha256 = sha256_file(&temp_path)?;
        fs::rename(&temp_path, &path).with_context(|| {
            format!(
                "failed to rename finalized shard {} to {}",
                temp_path.display(),
                path.display()
            )
        })?;
        db.transaction(|db| {
            db.upsert_shard(
                &path,
                shard_index,
                top_k_peaks,
                spectra_written,
                bytes_written,
                Some(&sha256),
            )?;
            db.insert_seen_spectra(top_k_peaks, &path, &pending_records)?;
            Ok(())
        })?;
        Ok(ShardSummary {
            shard_index,
            path,
            spectra_written,
            bytes_written,
            sha256,
        })
    }

    /// Removes an empty shard file.
    fn discard_empty(self) -> anyhow::Result<()> {
        let path = self.temp_path.clone();
        self.encoder
            .finish()
            .with_context(|| format!("failed to finish {}", path.display()))?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => {
                Err(error).with_context(|| format!("failed to remove {}", path.display()))
            }
        }
    }
}

/// Writes the generated shard manifest.
fn write_manifest(output_dir: &Path, shards: &[ShardSummary]) -> anyhow::Result<()> {
    let path = output_dir.join(MANIFEST);
    let mut writer = csv::Writer::from_path(&path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    writer.write_record([
        "shard_index",
        "path",
        "spectra_written",
        "bytes_written",
        "sha256",
    ])?;
    for shard in shards {
        writer.write_record([
            shard.shard_index.to_string(),
            shard.path.to_string_lossy().into_owned(),
            shard.spectra_written.to_string(),
            shard.bytes_written.to_string(),
            shard.sha256.clone(),
        ])?;
    }
    writer.flush()?;
    Ok(())
}

/// Writes the conversion summary report.
fn write_conversion_report(output_dir: &Path, summary: &ConversionSummary) -> anyhow::Result<()> {
    let path = output_dir.join(CONVERSION_REPORT);
    let json = serde_json::to_string_pretty(summary)?;
    fs::write(&path, json).with_context(|| format!("failed to write {}", path.display()))
}

/// Writes a small dataset README sidecar.
fn write_dataset_readme(output_dir: &Path, summary: &ConversionSummary) -> anyhow::Result<()> {
    let path = output_dir.join(DATASET_README);
    let text = format!(
        "\
# Deduplicated MassIVE MS/MS MGF Corpus

This directory contains zstd-compressed Mascot Generic Format shards generated from deduplicated MassIVE mzML public open-format files. The conversion keeps MS/MS spectra only, carries source, associated MS1, precursor, collision-energy, activation, isolation-window, scan-window, filter-string, injection-time, ion-mobility, and annotation-like metadata in MGF headers when mzML provides it, caps fragment peaks to the top {} by intensity, and deduplicates records by `(SPLASH, PEPMASS)`.

Spectra written: {}
Duplicate spectra skipped: {}
Files converted: {}
Files failed: {}
",
        summary.top_k_peaks,
        summary.spectra_written,
        summary.duplicates_skipped,
        summary.files_converted,
        summary.files_failed
    );
    fs::write(&path, text).with_context(|| format!("failed to write {}", path.display()))
}

/// Merges source counters into the full summary.
fn merge_stats(summary: &mut ConversionSummary, stats: &SourceConversionStats) {
    summary.spectra_written = summary
        .spectra_written
        .saturating_add(stats.spectra_written);
    summary.duplicates_skipped = summary
        .duplicates_skipped
        .saturating_add(stats.duplicates_skipped);
    summary.missing_precursor_skipped = summary
        .missing_precursor_skipped
        .saturating_add(stats.missing_precursor_skipped);
    summary.low_peak_skipped = summary
        .low_peak_skipped
        .saturating_add(stats.low_peak_skipped);
    for (level, count) in &stats.msn_counts {
        let value = summary.msn_counts.entry(*level).or_default();
        *value = value.saturating_add(*count);
    }
}

/// Aggregates stored per-source MSn counter JSON values.
fn aggregate_msn_counts(values: Vec<String>) -> anyhow::Result<BTreeMap<u8, u64>> {
    let mut output: BTreeMap<u8, u64> = BTreeMap::new();
    for value in values {
        let counts = serde_json::from_str::<BTreeMap<u8, u64>>(&value)
            .with_context(|| format!("failed to parse stored MSn counters: {value}"))?;
        for (level, count) in counts {
            let entry = output.entry(level).or_default();
            *entry = entry.saturating_add(count);
        }
    }
    Ok(output)
}

/// Returns a zstd worker count that leaves one CPU for parsing.
fn zstd_worker_count() -> u32 {
    let workers = std::thread::available_parallelism()
        .map_or(1, |available| available.get().saturating_sub(1).max(1));
    u32::try_from(workers).map_or(u32::MAX, |worker_count| worker_count)
}

/// Converts a finite non-negative `f64` to `u64` after bounds checking.
fn f64_to_u64(value: f64) -> Option<u64> {
    if !(value.is_finite() && value >= 0.0) {
        return None;
    }
    let text = format!("{value:.0}");
    text.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use mascot_rs::prelude::{
        IonMode as MascotIonMode, MascotGenericFormat, MascotGenericFormatMetadata,
    };
    use mzdata::meta::DissociationMethodTerm;
    use mzdata::params::{ControlledVocabulary, Param, Unit};
    use mzdata::spectrum::{
        Acquisition, Activation, IsolationWindow, IsolationWindowState, MultiLayerSpectrum,
        Precursor, ScanCombination, ScanEvent, ScanPolarity, ScanWindow, SelectedIon,
    };
    use tempfile::tempdir;

    use crate::config::Config;
    use crate::db::{SourceFileRow, StateDb};
    use crate::index::{OpenFormatRecord, SourceFileStatus};
    use crate::progress::ProgressReporter;

    use super::{
        arbitrary_metadata, canonical_mzml_metadata_header, convert_downloaded_mzml, ion_mode,
        signed_charge,
    };

    /// Confirms negative-mode charges are signed for MGF metadata.
    #[test]
    fn signs_negative_mode_charge() -> anyhow::Result<()> {
        assert_eq!(signed_charge(Some(2), ScanPolarity::Negative)?, Some(-2));
        assert_eq!(signed_charge(Some(2), ScanPolarity::Positive)?, Some(2));
        assert!(ion_mode(ScanPolarity::Unknown).is_none());
        Ok(())
    }

    /// Confirms critical precursor metadata is literally emitted in MGF headers.
    #[test]
    fn written_mgf_contains_precursor_charge_and_retention_time_headers() -> anyhow::Result<()> {
        let metadata = MascotGenericFormatMetadata::new_with_smiles_and_ion_mode(
            Some("MSV000000001:7".to_owned()),
            2,
            Some(12.5),
            Some(-2),
            Some("MSV000000001/path/source.mzML".to_owned()),
            None,
            Some(MascotIonMode::Negative),
        )?
        .with_scans(Some(
            "controllerType=0 controllerNumber=1 scan=7".to_owned(),
        ));
        let record = MascotGenericFormat::new(
            metadata,
            445.2,
            vec![100.0_f64, 200.0_f64],
            vec![10.0_f64, 20.0_f64],
        )?;
        let mut bytes = Vec::new();
        record.write_to(&mut bytes)?;
        let text = String::from_utf8(bytes)?;

        assert!(text.contains("\nPEPMASS=445.2\n"), "{text}");
        assert!(text.contains("\nCHARGE=-2\n"), "{text}");
        assert!(text.contains("\nRTINSECONDS=12.5\n"), "{text}");
        assert!(
            text.contains("\nFILENAME=MSV000000001/path/source.mzML\n"),
            "{text}"
        );
        assert!(
            text.contains("\nSCANS=controllerType=0 controllerNumber=1 scan=7\n"),
            "{text}"
        );
        Ok(())
    }

    /// Confirms differently written mzML metadata names resolve to stable MGF headers.
    #[test]
    fn normalizes_known_mzml_metadata_aliases() {
        assert_eq!(
            canonical_mzml_metadata_header(&test_user_param("activation energy", 30.0)),
            Some("COLLISION_ENERGY")
        );
        assert_eq!(
            canonical_mzml_metadata_header(&test_user_param("NCE", 28.0)),
            Some("NORMALIZED_COLLISION_ENERGY")
        );
        assert_eq!(
            canonical_mzml_metadata_header(&test_ms_param("instrument value", 30.0, 1_000_045)),
            Some("COLLISION_ENERGY")
        );
        assert_eq!(
            canonical_mzml_metadata_header(&test_ms_param("instrument value", 28.0, 1_000_138)),
            Some("NORMALIZED_COLLISION_ENERGY")
        );
        assert_eq!(
            canonical_mzml_metadata_header(&test_user_param("compound name", "caffeine")),
            Some("NAME")
        );
    }

    /// Confirms structured mzML acquisition and precursor metadata reaches MGF headers.
    #[test]
    fn arbitrary_metadata_includes_structured_mzml_metadata() -> anyhow::Result<()> {
        let metadata_entries = structured_mzml_metadata_entries();
        let metadata = metadata_entries.iter().cloned().collect::<BTreeMap<_, _>>();

        assert_eq!(
            metadata.get("COLLISION_ENERGY").map(String::as_str),
            Some("31.5")
        );
        assert_eq!(
            metadata
                .get("SUPPLEMENTAL_COLLISION_ENERGY")
                .map(String::as_str),
            Some("7")
        );
        assert_eq!(
            metadata.get("ACTIVATION_METHOD").map(String::as_str),
            Some("higher energy beam-type collision-induced dissociation")
        );
        assert_eq!(
            metadata
                .get("ISOLATION_WINDOW_TARGET_MZ")
                .map(String::as_str),
            Some("445.2")
        );
        assert_eq!(
            metadata
                .get("ISOLATION_WINDOW_LOWER_MZ")
                .map(String::as_str),
            Some("444.7")
        );
        assert_eq!(
            metadata.get("SCAN_WINDOW_UPPER_MZ").map(String::as_str),
            Some("1500")
        );
        assert_eq!(
            metadata.get("INJECTION_TIME_MS").map(String::as_str),
            Some("12.5")
        );
        assert_eq!(
            metadata.get("FILTER_STRING").map(String::as_str),
            Some("FTMS + p ESI Full ms2")
        );
        assert_eq!(
            metadata.get("ION_MOBILITY_DRIFT_TIME").map(String::as_str),
            Some("4.2")
        );
        assert_eq!(metadata.get("ADDUCT").map(String::as_str), Some("[M+H]+"));
        assert_structured_metadata_is_written_to_mgf(metadata_entries)
    }

    /// Confirms structured metadata headers are literally written in MGF text.
    fn assert_structured_metadata_is_written_to_mgf(
        metadata_entries: Vec<(String, String)>,
    ) -> anyhow::Result<()> {
        let mgf_metadata = MascotGenericFormatMetadata::new_with_smiles_and_ion_mode(
            Some("MSV000000001:8".to_owned()),
            2,
            Some(120.0),
            Some(2),
            Some("MSV000000001/peak/test.mzML".to_owned()),
            None,
            Some(MascotIonMode::Positive),
        )?
        .with_arbitrary_metadata(metadata_entries);
        let record = MascotGenericFormat::new(
            mgf_metadata,
            445.2,
            vec![100.0_f64, 200.0_f64],
            vec![10.0_f64, 20.0_f64],
        )?;
        let mut bytes = Vec::new();
        record.write_to(&mut bytes)?;
        let text = String::from_utf8(bytes)?;
        assert!(text.contains("\nCOLLISION_ENERGY=31.5\n"), "{text}");
        assert!(
            text.contains(
                "\nACTIVATION_METHOD=higher energy beam-type collision-induced dissociation\n"
            ),
            "{text}"
        );
        assert!(
            text.contains("\nFILTER_STRING=FTMS + p ESI Full ms2\n"),
            "{text}"
        );
        Ok(())
    }

    /// Builds mzML-derived metadata entries covering important header families.
    fn structured_mzml_metadata_entries() -> Vec<(String, String)> {
        let spectrum = structured_test_spectrum();
        arbitrary_metadata(
            &spectrum,
            &test_source_row(),
            None,
            Some(1_234.0),
            Some("controllerType=0 controllerNumber=1 scan=8"),
        )
    }

    /// Builds a spectrum with activation, scan, isolation, ion mobility, and annotation params.
    fn structured_test_spectrum() -> MultiLayerSpectrum {
        let mut spectrum: MultiLayerSpectrum = MultiLayerSpectrum::default();
        spectrum.description.id = "controllerType=0 controllerNumber=1 scan=9".to_owned();
        spectrum.description.index = 8;
        spectrum.description.ms_level = 2;
        spectrum.description.acquisition = structured_test_acquisition();
        spectrum.description.precursor = vec![structured_test_precursor()];
        spectrum
    }

    /// Builds acquisition metadata for structured metadata tests.
    fn structured_test_acquisition() -> Acquisition {
        Acquisition {
            scans: vec![ScanEvent::new(
                2.0,
                12.5,
                vec![ScanWindow::new(100.0, 1_500.0)],
                1,
                Some(Box::new(vec![
                    test_ms_param("filter string", "FTMS + p ESI Full ms2", 1_000_512),
                    test_ms_param("ion mobility drift time", 4.2, 1_002_476),
                ])),
            )],
            combination: ScanCombination::default(),
            params: None,
        }
    }

    /// Builds precursor metadata for structured metadata tests.
    fn structured_test_precursor() -> Precursor {
        Precursor {
            ions: vec![SelectedIon {
                mz: 445.2,
                intensity: 1_234.0,
                charge: Some(2),
                params: Some(Box::new(vec![test_user_param("adduct", "[M+H]+")])),
            }],
            isolation_window: IsolationWindow::new(
                445.2,
                444.7,
                445.7,
                IsolationWindowState::Complete,
            ),
            precursor_id: Some("controllerType=0 controllerNumber=1 scan=8".to_owned()),
            product_id: None,
            activation: structured_test_activation(),
        }
    }

    /// Builds activation metadata for structured metadata tests.
    fn structured_test_activation() -> Activation {
        let mut activation = Activation::default();
        activation.energy = 31.5;
        activation
            .methods_mut()
            .push(DissociationMethodTerm::HigherEnergyBeamTypeCollisionInducedDissociation);
        activation.params.push(test_ms_param(
            "supplemental collision energy",
            7.0,
            1_002_680,
        ));
        activation
    }

    /// Confirms an empty conversion rerun does not truncate tracked finalized shards.
    #[test]
    fn empty_convert_preserves_tracked_shard() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let shard_path = tempdir
            .path()
            .join("massive_ms2.top-0256.part-000000.mgf.zst");
        std::fs::write(&shard_path, b"already finalized")?;
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        db.upsert_shard(&shard_path, 0, 256, 1, 17, Some("not_checked_here"))?;
        let config = test_config(tempdir.path());
        let summary = convert_downloaded_mzml(&mut db, &config, &ProgressReporter::hidden())?;
        assert_eq!(summary.shards_written, 1);
        assert_eq!(std::fs::read(&shard_path)?, b"already finalized");
        Ok(())
    }

    /// Confirms a bad mzML is marked failed once instead of retried forever.
    #[test]
    fn bad_downloaded_mzml_marks_failed_without_looping() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let source_path = tempdir
            .path()
            .join("mzml")
            .join("MSV000000001")
            .join("missing.mzML");
        let mut db = StateDb::connect(":memory:")?;
        db.initialize()?;
        let record = test_record("MSV000000001/missing.mzML", 1.0);
        db.upsert_source_file(&record, &source_path, 0, SourceFileStatus::Downloaded)?;
        let config = test_config(tempdir.path());
        let summary = convert_downloaded_mzml(&mut db, &config, &ProgressReporter::hidden())?;
        assert_eq!(summary.files_failed, 1);
        assert_eq!(db.count_conversion_failed(config.top_k_peaks)?, 1);
        assert_eq!(db.count_downloaded_unconverted(config.top_k_peaks)?, 0);
        assert!(
            db.downloaded_unconverted(config.top_k_peaks, 10)?
                .is_empty()
        );
        Ok(())
    }

    /// Builds a minimal test configuration rooted in a temporary directory.
    fn test_config(root: &std::path::Path) -> Config {
        Config {
            openformats_record_id: 4_549_746,
            openformats_index_dir: root.join("openformats"),
            database_url: ":memory:".to_owned(),
            mzml_download_dir: root.join("mzml"),
            mgf_output_dir: root.to_path_buf(),
            target_ms2_spectra: 1,
            top_k_peaks: 256,
            mgf_shard_max_spectra: 1_000,
            source_selection_buffer: 1.25,
            source_selection_chunk_ms2: 50_000_000,
            download_workers: 1,
            download_retry_attempts: 1,
            download_retry_delay: Duration::from_secs(0),
            http_request_timeout: None,
            http_connect_timeout: Duration::from_secs(1),
            publish_to_zenodo: false,
            zenodo_deposition_id: None,
            zenodo_max_file_bytes: 50_000_000_000,
        }
    }

    /// Builds a small test open-format record.
    fn test_record(filepath: &str, spectra_ms2: f64) -> OpenFormatRecord {
        OpenFormatRecord {
            filepath: filepath.to_owned(),
            dataset: "MSV000000001".to_owned(),
            collection: "peak".to_owned(),
            create_time: "2021-01-01 00:00:00".to_owned(),
            size: 1,
            size_mb: 1,
            spectra_ms1: Some(1.0),
            spectra_ms2: Some(spectra_ms2),
            instrument_vendor: "vendor".to_owned(),
            instrument_model: "model".to_owned(),
            extension: ".mzML".to_owned(),
        }
    }

    /// Builds a source-file row for conversion metadata tests.
    fn test_source_row() -> SourceFileRow {
        SourceFileRow {
            filepath: "MSV000000001/peak/test.mzML".to_owned(),
            dataset: "MSV000000001".to_owned(),
            collection: "peak".to_owned(),
            create_time: "2021-01-01 00:00:00".to_owned(),
            size_bytes: 1,
            spectra_ms1: Some(1.0),
            spectra_ms2: Some(1.0),
            instrument_vendor: "vendor".to_owned(),
            instrument_model: "model".to_owned(),
            extension: ".mzML".to_owned(),
            local_path: "/tmp/test.mzML".to_owned(),
            status: "downloaded".to_owned(),
        }
    }

    /// Builds a PSI-MS parameter for metadata normalization tests.
    fn test_ms_param(name: &str, value: impl Into<mzdata::params::Value>, accession: u32) -> Param {
        Param {
            name: name.to_owned(),
            value: value.into(),
            accession: Some(accession),
            controlled_vocabulary: Some(ControlledVocabulary::MS),
            unit: Unit::Unknown,
        }
    }

    /// Builds an uncontrolled mzML parameter for metadata normalization tests.
    fn test_user_param(name: &str, value: impl Into<mzdata::params::Value>) -> Param {
        Param {
            name: name.to_owned(),
            value: value.into(),
            accession: None,
            controlled_vocabulary: None,
            unit: Unit::Unknown,
        }
    }
}
