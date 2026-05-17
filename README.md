# gnps-massive-to-mgf

Rust pipeline to build a large deduplicated MassIVE mzML-derived MS/MS corpus in sharded Mascot Generic Format. The first target is roughly 200M retained MS/MS spectra, deduplicated by `(SPLASH, PEPMASS)`, with top-k fragment peak retention configurable through `.env`.

The pipeline downloads the GNPS/MassIVE public open-format index from Zenodo record `4549746`, ingests only the deduplicated MassIVE mzML subset for now, downloads raw mzML files under `MZML_DOWNLOAD_DIR`, converts downloaded files into zstd-compressed MGF shards under `MGF_OUTPUT_DIR`, records restartable state in SQLite through Diesel, and publishes to production Zenodo when `ZENODO_TOKEN` is present.

Conversion writes temporary shard files first, renames only finalized shards, and records deduplication keys only after a shard is complete. A repeated `convert` run preserves existing finalized shards. Publishing is blocked unless the finalized unique spectrum count reaches `TARGET_MS2_SPECTRA`, no selected downloads or conversions are still pending or failed, no temporary shards remain, and every upload file is within `ZENODO_MAX_FILE_BYTES`.

MGF headers include precursor m/z, charge, MS2 retention time, associated MS1 scan context, SPLASH, source MassIVE path, instrument vendor/model, collision energy, activation method, isolation window, scan window, filter string, injection time, ion mobility when present, and normalized annotation-like fields such as `NAME`, `ADDUCT`, `FORMULA`, `SMILES`, `INCHI`, and `PEPTIDE_SEQUENCE` when mzML carries them as params.

```bash
cp .env.example .env
cargo run --release -- index
cargo run --release -- download
cargo run --release -- convert
cargo run --release -- status
cargo run --release -- publish-dry-run
cargo run --release -- publish
```

`cargo run --release -- run` executes the same stages in order. Downloads use `DOWNLOAD_WORKERS` concurrent MassIVE transfers. Keep it at `1` for the initial production run, then raise it only after the restart behavior and network load look stable. MassIVE runs Apache `mod_qos` and rejects with `HTTP 429 Too Many Requests` once a per-IP threshold is hit; empirically `DOWNLOAD_WORKERS=8` sustains cleanly while `16` triggers `429`s within the first batch, so do not exceed `8`.

Set `HTTP_REQUEST_TIMEOUT_SECONDS=0` to disable the full-transfer timeout for very large mzML files. The downloader retries failed files on the next run and resumes `.part` files when MassIVE accepts byte-range requests.

For the first pilot, run the stages manually and inspect progress between them:

```bash
cargo run --release -- index
cargo run --release -- status
cargo run --release -- download
cargo run --release -- status
cargo run --release -- convert
cargo run --release -- status
cargo run --release -- publish-dry-run
```

`status` is read-only and summarizes source counts, conversion counters, finalized shard bytes, and publication readiness. `publish-dry-run` performs the same local validation and upload-spec preparation as `publish`, but it does not require `ZENODO_TOKEN` and does not contact Zenodo.
