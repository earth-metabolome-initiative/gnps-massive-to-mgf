# gnps-massive-to-mgf

Rust pipeline to build a large deduplicated MassIVE mzML-derived MS/MS corpus in sharded Mascot Generic Format. The first target is roughly 200M retained MS/MS spectra, deduplicated by `(SPLASH, PEPMASS)`, with top-k fragment peak retention configurable through `.env`.

The pipeline downloads the GNPS/MassIVE public open-format index from Zenodo record `4549746`, ingests only the deduplicated MassIVE mzML subset for now, downloads raw mzML files under `MZML_DOWNLOAD_DIR`, converts downloaded files into zstd-compressed MGF shards under `MGF_OUTPUT_DIR`, and records restartable state in SQLite through Diesel.

Conversion writes temporary shard files first, renames only finalized shards, and records deduplication keys only after a shard is complete. A repeated `convert` run preserves existing finalized shards.

MGF headers include precursor m/z, charge, MS2 retention time, associated MS1 scan context, SPLASH, source MassIVE path, instrument vendor/model, collision energy, activation method, isolation window, scan window, filter string, injection time, ion mobility when present, and normalized annotation-like fields such as `NAME`, `ADDUCT`, `FORMULA`, `SMILES`, `INCHI`, and `PEPTIDE_SEQUENCE` when mzML carries them as params.

```bash
cp .env.example .env
cargo run --release -- download
cargo run --release -- convert
cargo run --release -- status
```

`cargo run --release -- run` executes the same stages in order. The first step of each `download` (and of `run`) checks Zenodo for a newer version of the open-format index record `OPENFORMATS_RECORD_ID` (default `4549746`). If the upstream record has been modified since the local TSVs were last fetched, the pipeline re-downloads them and re-ingests `source_files` before proceeding. Otherwise it skips straight to the file downloads. Downloads use anonymous FTPS against `massive-ftp.ucsd.edu`, which is the bulk-download endpoint MassIVE's administrators designate for public data. Files are retrieved one at a time. MassIVE fair-shares bandwidth across concurrent connections from the same IP and additionally throttles bursts (HTTPS via Apache `mod_qos`, FTPS via SFTPGo per-IP caps), so adding workers does not increase aggregate throughput and risks triggering an abuser cool-down that degrades all subsequent connections.

The MassIVE FTPS server presents a self-signed certificate. The pipeline pins it via Trust-On-First-Use: the first connection records the cert's SHA-256 hash to `FTPS_CERT_PIN_PATH` (default `data/ftps_cert_pin.txt`) and every subsequent connection verifies the hash matches. If MassIVE rotates the cert, downloads will fail with a clear `pin mismatch` error; delete the pin file to re-trust the server.

`source_files.volume` is populated from the dataset-level manifest at `MASSIVE_VOLUMES_PATH` at the start of every `download` run. Datasets absent from that manifest (typically new MSVs added after the manifest snapshot) cannot have their FTPS path constructed and fail fast at download time with a clear error. To refresh the mapping, drop a newer TSV at `MASSIVE_VOLUMES_PATH` and re-run `cargo run --release -- download`. The volume ingest is idempotent and only updates rows whose volume actually changed.

Set `FTPS_TRANSFER_TIMEOUT_SECONDS=0` to disable the full-transfer timeout for very large mzML files. The downloader retries failed files on the next run and resumes `.part` files via the FTP `REST` command when the server accepts it.

For the first pilot, run the stages manually and inspect progress between them:

```bash
cargo run --release -- download
cargo run --release -- status
cargo run --release -- convert
cargo run --release -- status
```

`status` is read-only and summarizes source counts, conversion counters, and finalized shard bytes.
