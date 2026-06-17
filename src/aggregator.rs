//! Streaming spectrum aggregator for the download TUI.
//!
//! A dedicated OS thread parses freshly-downloaded mzML files with `mzdata`
//! and accumulates running aggregates (peak counts, precursor m/z, charges,
//! per-instrument file counts). The TUI consumes `AggregateEvent` values
//! from a tokio mpsc receiver and re-renders on its normal tick cadence.
//! The thread is single-purpose, so `mzdata`'s synchronous parsing never
//! blocks the event loop or the FTPS download future.
//!
//! The TUI hands files in by sending `AggregatorJob` values on a
//! `std::sync::mpsc` channel. Dropping the `AggregatorHandle` closes the
//! channel, which the worker treats as a clean shutdown.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use anyhow::Context;
use mzdata::MzMLReader;
use mzdata::io::MZFileReader;
use mzdata::prelude::{IonProperties, PrecursorSelection, SpectrumLike};
use mzdata::spectrum::ScanPolarity;
use tokio::sync::mpsc as tokio_mpsc;

/// Number of histogram buckets used for peak-count and pepmass distributions.
const HISTOGRAM_BUCKETS: usize = 50;
/// Upper bound (exclusive) of the peak-count histogram.
const PEAK_COUNT_HIGH: f64 = 5_000.0;
/// Upper bound (exclusive) of the pepmass histogram in m/z.
const PEPMASS_HIGH: f64 = 2_000.0;
/// Upper bound (exclusive) of the MS2-per-file histogram.
const MS2_PER_FILE_HIGH: f64 = 100_000.0;

/// One downloaded file handed to the aggregator.
#[derive(Debug, Clone)]
pub struct AggregatorJob {
    /// Local on-disk path of the freshly-downloaded mzML file.
    pub local_path: PathBuf,
    /// Instrument vendor recorded for the source file.
    pub instrument_vendor: String,
    /// Instrument model recorded for the source file.
    pub instrument_model: String,
}

/// One update pushed from the worker thread to the TUI.
#[derive(Debug, Clone)]
pub enum AggregateEvent {
    /// Latest aggregate snapshot after a file has been parsed.
    Snapshot(Box<AggregateSnapshot>),
    /// Non-fatal parse error for one file.
    Error(String),
}

/// Running aggregate statistics across every parsed file.
#[derive(Debug, Clone)]
pub struct AggregateSnapshot {
    /// Files successfully opened by `mzdata` (regardless of MS level mix).
    pub files_parsed: u64,
    /// Spectra observed across every MS level.
    pub spectra_total: u64,
    /// MS2 spectra observed.
    pub ms2_total: u64,
    /// Peak-count distribution across every spectrum.
    pub peaks: Histogram,
    /// Precursor m/z distribution across MS2 spectra with a finite precursor.
    pub pepmass: Histogram,
    /// MS2 spectrum counts per parsed precursor charge.
    pub charges: BTreeMap<i32, u64>,
    /// File counts per `(vendor, model)` pair.
    pub instruments: BTreeMap<(String, String), u64>,
    /// One data point per parsed file: the number of MS2 spectra it contains.
    pub ms2_per_file: Histogram,
    /// Per-spectrum polarity counts (`positive`, `negative`, `unknown`).
    pub polarities: BTreeMap<String, u64>,
    /// Per-MS2-spectrum dissociation-method counts (`HigherEnergy...`, etc.).
    pub activations: BTreeMap<String, u64>,
}

impl Default for AggregateSnapshot {
    fn default() -> Self {
        Self {
            files_parsed: 0,
            spectra_total: 0,
            ms2_total: 0,
            peaks: Histogram::new(0.0, PEAK_COUNT_HIGH, HISTOGRAM_BUCKETS),
            pepmass: Histogram::new(0.0, PEPMASS_HIGH, HISTOGRAM_BUCKETS),
            charges: BTreeMap::new(),
            instruments: BTreeMap::new(),
            ms2_per_file: Histogram::new(0.0, MS2_PER_FILE_HIGH, HISTOGRAM_BUCKETS),
            polarities: BTreeMap::new(),
            activations: BTreeMap::new(),
        }
    }
}

/// Fixed-width bucket histogram with running mean and variance via Welford.
///
/// Quantiles are computed from the cumulative bucket distribution and are
/// therefore exact to within `(high - low) / buckets.len()`. For the TUI's
/// purposes (one-decimal display, no statistical inference) that resolution
/// is more than enough.
#[derive(Debug, Clone)]
#[allow(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "histogram bookkeeping uses u64<->f64 conversions for Welford mean/variance and bucket index math; this is descriptive statistics on a display widget, not a numerical kernel"
)]
pub struct Histogram {
    /// Number of values added.
    pub count: u64,
    /// Running mean (Welford).
    pub mean: f64,
    /// Sum of squared differences from the running mean (Welford).
    pub m2: f64,
    /// Smallest value observed.
    pub min: f64,
    /// Largest value observed.
    pub max: f64,
    /// Inclusive lower bound of the bucket range.
    bucket_low: f64,
    /// Exclusive upper bound of the bucket range.
    bucket_high: f64,
    /// Counts per bucket; values above `bucket_high` fall into the last bucket.
    pub buckets: Vec<u64>,
}

impl Histogram {
    /// Builds an empty histogram over `[low, high)` with `n_buckets` buckets.
    #[must_use]
    pub fn new(low: f64, high: f64, n_buckets: usize) -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            bucket_low: low,
            bucket_high: high,
            buckets: vec![0; n_buckets.max(1)],
        }
    }

    /// Records one finite value.
    #[allow(
        clippy::as_conversions,
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "Welford-style accumulator and bucket index math need u64<->f64 conversions"
    )]
    pub fn add(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }
        self.count = self.count.saturating_add(1);
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 = delta.mul_add(delta2, self.m2);
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        if self.bucket_high > self.bucket_low && !self.buckets.is_empty() {
            let span = self.bucket_high - self.bucket_low;
            let len = self.buckets.len();
            let raw = ((value - self.bucket_low) / span * len as f64).floor();
            let last = len.saturating_sub(1);
            let idx = if raw.is_finite() {
                (raw as isize).clamp(0, last.cast_signed()) as usize
            } else {
                last
            };
            self.buckets[idx] = self.buckets[idx].saturating_add(1);
        }
    }

    /// Returns the requested quantile in the closed range `[0, 1]`, or `None`
    /// when no values have been added.
    #[must_use]
    #[allow(
        clippy::as_conversions,
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "quantile interpolation needs u64<->f64 and float-to-int conversions"
    )]
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 || self.buckets.is_empty() {
            return None;
        }
        let target = (q.clamp(0.0, 1.0) * self.count as f64).ceil().max(1.0) as u64;
        let span = (self.bucket_high - self.bucket_low) / self.buckets.len() as f64;
        let mut cumulative = 0_u64;
        for (index, bucket) in self.buckets.iter().enumerate() {
            cumulative = cumulative.saturating_add(*bucket);
            if cumulative >= target {
                return Some((index as f64 + 0.5).mul_add(span, self.bucket_low));
            }
        }
        Some(self.bucket_high)
    }
}

/// Channel pair owned by the TUI to talk to the aggregator worker thread.
#[derive(Debug)]
pub struct AggregatorHandle {
    /// File-job sender; dropping it shuts the worker down cleanly.
    pub tx_jobs: mpsc::Sender<AggregatorJob>,
    /// Snapshot/error receiver consumed by the TUI's `tokio::select!` loop.
    pub rx_updates: tokio_mpsc::UnboundedReceiver<AggregateEvent>,
}

/// Spawns the aggregator worker thread and returns its handle.
///
/// # Errors
///
/// Returns an error if the operating system rejects the thread spawn, which
/// only happens under severe resource exhaustion.
pub fn spawn() -> anyhow::Result<AggregatorHandle> {
    let (tx_jobs, rx_jobs) = mpsc::channel::<AggregatorJob>();
    let (tx_updates, rx_updates) = tokio_mpsc::unbounded_channel();
    thread::Builder::new()
        .name("spectrum-aggregator".to_owned())
        .spawn(move || run(&rx_jobs, &tx_updates))
        .context("failed to spawn spectrum-aggregator thread")?;
    Ok(AggregatorHandle {
        tx_jobs,
        rx_updates,
    })
}

/// Worker loop: drain incoming jobs until the channel closes.
fn run(rx: &mpsc::Receiver<AggregatorJob>, tx: &tokio_mpsc::UnboundedSender<AggregateEvent>) {
    let mut state = AggregateSnapshot::default();
    while let Ok(job) = rx.recv() {
        if let Err(error) = process_file(&job, &mut state) {
            drop(tx.send(AggregateEvent::Error(format!(
                "spectrum aggregator failed on {}: {error:#}",
                job.local_path.display(),
            ))));
        }
        state.files_parsed = state.files_parsed.saturating_add(1);
        *state
            .instruments
            .entry((job.instrument_vendor.clone(), job.instrument_model.clone()))
            .or_insert(0) = state
            .instruments
            .get(&(job.instrument_vendor.clone(), job.instrument_model.clone()))
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        drop(tx.send(AggregateEvent::Snapshot(Box::new(state.clone()))));
    }
}

/// Opens `job.local_path` with `mzdata` and updates `state` in place.
fn process_file(job: &AggregatorJob, state: &mut AggregateSnapshot) -> anyhow::Result<()> {
    let mut reader = MzMLReader::open_path(&job.local_path)
        .with_context(|| format!("failed to open {}", job.local_path.display()))?;
    let mut file_ms2_count: u64 = 0;
    #[allow(
        clippy::as_conversions,
        clippy::cast_precision_loss,
        reason = "peak count is bounded by spectrum size; lossy display-only conversion is acceptable"
    )]
    for mut spectrum in &mut reader {
        state.spectra_total = state.spectra_total.saturating_add(1);
        if spectrum.try_build_peaks().is_ok() {
            let peak_count = spectrum.peaks().iter().count();
            state.peaks.add(peak_count as f64);
        }
        let polarity_label = match spectrum.polarity() {
            ScanPolarity::Positive => "positive",
            ScanPolarity::Negative => "negative",
            ScanPolarity::Unknown => "unknown",
        };
        increment_label(&mut state.polarities, polarity_label);
        if spectrum.ms_level() != 2 {
            continue;
        }
        state.ms2_total = state.ms2_total.saturating_add(1);
        file_ms2_count = file_ms2_count.saturating_add(1);
        if let Some(precursor) = spectrum.precursor() {
            if let Some(method) = precursor.activation().method() {
                increment_label(&mut state.activations, &format!("{method:?}"));
            }
            if let Some(ion) = precursor.ion() {
                let mz = ion.mz();
                if mz.is_finite() && mz > 0.0 {
                    state.pepmass.add(mz);
                }
                if let Some(charge) = ion.charge() {
                    *state.charges.entry(charge).or_insert(0) = state
                        .charges
                        .get(&charge)
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(1);
                }
            }
        }
    }
    #[allow(
        clippy::as_conversions,
        clippy::cast_precision_loss,
        reason = "MS2-per-file is bounded by parsed spectra count; lossy display-only cast"
    )]
    state.ms2_per_file.add(file_ms2_count as f64);
    Ok(())
}

/// Increments the counter associated with `label` in `map`, inserting `1` on
/// first sight.
fn increment_label(map: &mut BTreeMap<String, u64>, label: &str) {
    let next = map.get(label).copied().unwrap_or(0).saturating_add(1);
    map.insert(label.to_owned(), next);
}

#[cfg(test)]
mod tests {
    use super::Histogram;

    /// Confirms the histogram tracks running mean and exposes coarse quantiles.
    #[test]
    fn histogram_tracks_mean_and_quantiles() {
        let mut histogram = Histogram::new(0.0, 100.0, 10);
        for value in 0..100 {
            histogram.add(f64::from(value));
        }
        assert_eq!(histogram.count, 100);
        let mean = histogram.mean;
        assert!((mean - 49.5).abs() < 1e-9);
        let median = histogram.quantile(0.5).unwrap_or(f64::NAN);
        // 10-bucket histogram over [0, 100) places the median anywhere in the
        // 40-50 bucket, so the bucket midpoint is 45.
        assert!((median - 45.0).abs() < 1e-9);
        let p95 = histogram.quantile(0.95).unwrap_or(f64::NAN);
        assert!(p95 >= 90.0);
    }

    /// Confirms an empty histogram refuses to produce quantiles.
    #[test]
    fn histogram_empty_quantile_is_none() {
        let histogram = Histogram::new(0.0, 1.0, 4);
        assert!(histogram.quantile(0.5).is_none());
        assert_eq!(histogram.count, 0);
    }

    /// Confirms NaN and infinite inputs are silently ignored.
    #[test]
    fn histogram_rejects_non_finite_values() {
        let mut histogram = Histogram::new(0.0, 10.0, 5);
        histogram.add(f64::NAN);
        histogram.add(f64::INFINITY);
        histogram.add(f64::NEG_INFINITY);
        assert_eq!(histogram.count, 0);
        assert!(histogram.quantile(0.5).is_none());
    }
}
