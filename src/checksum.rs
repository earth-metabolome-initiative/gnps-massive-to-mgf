//! File checksum helpers shared by conversion and publication.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::Context;
use sha2::{Digest, Sha256};

/// Computes the lowercase SHA-256 checksum of a local file.
///
/// # Errors
///
/// Returns an error if the file cannot be opened or any read fails.
pub fn sha256_file(path: &Path) -> anyhow::Result<String> {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut buffer = vec![0_u8; 1_048_576];
    let mut file = BufReader::new(
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?,
    );
    let mut hasher = Sha256::new();
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    let mut output = String::with_capacity(64);
    for &byte in &digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(output)
}

/// Computes the lowercase SHA-256 hex of a byte slice. Used by
/// `refresh_indexes_and_ingest` to fingerprint the ingest-relevant pieces of
/// [`crate::config::Config`].
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for &byte in &digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
