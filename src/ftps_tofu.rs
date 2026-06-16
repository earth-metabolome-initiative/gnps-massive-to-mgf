//! Trust-On-First-Use cert verifier for FTPS connections.
//!
//! `massive-ftp.ucsd.edu` serves its FTPS data channel with a self-signed
//! certificate (the MassIVE administrator confirmed this is intentional). The
//! standard public-CA trust store therefore cannot validate it.
//!
//! We pin the cert via Trust-On-First-Use:
//!
//! 1. On the first connection, the SHA-256 hash of the server's leaf
//!    certificate is written to a pin file (e.g. `data/ftps_cert_pin.txt`).
//! 2. On every subsequent connection, the leaf cert is hashed again and
//!    compared against the persisted hash. A mismatch aborts the handshake
//!    with a clear error and a suggested remediation: delete the pin file to
//!    re-trust the server (mirroring `ssh known_hosts` semantics).
//!
//! TLS signature checks delegate to the rustls `ring` crypto provider, which
//! is the same backend used by `reqwest`'s `rustls` feature. No `unsafe` code.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::Context;
use rustls::DigitallySignedStruct;
use rustls::SignatureScheme;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use sha2::{Digest, Sha256};

/// Length in bytes of a SHA-256 digest.
const SHA256_DIGEST_LEN: usize = 32;

/// Trust-On-First-Use server cert verifier backed by a pin file on disk.
///
/// Cloneable via `Arc`. See the module docs for the trust model.
pub struct TofuCertVerifier {
    /// Path the pin file is stored at.
    pin_path: PathBuf,
    /// Crypto provider used to verify TLS handshake signatures.
    provider: Arc<CryptoProvider>,
    /// Lazily-populated cache of the pinned hash, so we read the pin file
    /// once per process rather than once per connection.
    cached_pin: Mutex<Option<[u8; SHA256_DIGEST_LEN]>>,
}

impl fmt::Debug for TofuCertVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TofuCertVerifier")
            .field("pin_path", &self.pin_path)
            .finish_non_exhaustive()
    }
}

impl TofuCertVerifier {
    /// Builds a TOFU verifier that persists the pin under `pin_path`.
    ///
    /// The pin file is read (or created) lazily on the first TLS handshake.
    /// The parent directory of `pin_path` must already exist.
    #[must_use]
    pub fn new(pin_path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            pin_path,
            provider: Arc::new(rustls::crypto::ring::default_provider()),
            cached_pin: Mutex::new(None),
        })
    }

    /// Returns the path the verifier persists its pin at.
    #[must_use]
    pub fn pin_path(&self) -> &Path {
        &self.pin_path
    }

    /// Reads the pinned digest from disk, if a pin file is present.
    ///
    /// The file may contain leading `#`-prefixed comment lines and blank
    /// lines. The first non-blank, non-comment line must be the hex-encoded
    /// SHA-256 digest. Returns `Ok(None)` when no pin file exists yet.
    fn load_pin(&self) -> anyhow::Result<Option<[u8; SHA256_DIGEST_LEN]>> {
        let contents = match fs::read_to_string(&self.pin_path) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to read FTPS pin file {}", self.pin_path.display())
                });
            }
        };
        let Some(digest_line) = contents
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty() && !line.starts_with('#'))
        else {
            return Ok(None);
        };
        let bytes = hex_decode(digest_line).with_context(|| {
            format!(
                "FTPS pin file {} does not contain a hex-encoded SHA-256 digest",
                self.pin_path.display(),
            )
        })?;
        let mut array = [0_u8; SHA256_DIGEST_LEN];
        if bytes.len() != SHA256_DIGEST_LEN {
            anyhow::bail!(
                "FTPS pin file {} contains {} bytes, expected {SHA256_DIGEST_LEN}",
                self.pin_path.display(),
                bytes.len(),
            );
        }
        array.copy_from_slice(&bytes);
        Ok(Some(array))
    }

    /// Persists `digest` as the new pin, replacing any existing file.
    fn save_pin(&self, digest: &[u8; SHA256_DIGEST_LEN]) -> anyhow::Result<()> {
        if let Some(parent) = self.pin_path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create directory for FTPS pin file {}",
                    self.pin_path.display(),
                )
            })?;
        }
        let body = format!(
            "# FTPS server certificate pin (SHA-256 over the leaf cert, hex).\n\
             # Recorded automatically by the gnps-massive-to-mgf TOFU verifier.\n\
             # Delete this file to re-trust the server on the next connection.\n\
             {}\n",
            hex_encode(digest),
        );
        fs::write(&self.pin_path, body).with_context(|| {
            format!("failed to write FTPS pin file {}", self.pin_path.display())
        })?;
        Ok(())
    }

    /// Returns the expected digest, lazily-loading or recording it.
    ///
    /// On first call: if `load_pin` returns a digest, that becomes the cached
    /// expectation. Otherwise the observed digest is written to disk and
    /// adopted as the expectation. Subsequent calls hit the cache.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the mutex guard is intentionally held across save_pin so two threads cannot race to write conflicting pin files"
    )]
    fn expected_digest(
        &self,
        observed: &[u8; SHA256_DIGEST_LEN],
    ) -> anyhow::Result<[u8; SHA256_DIGEST_LEN]> {
        let mut guard = self
            .cached_pin
            .lock()
            .map_err(|error| anyhow::anyhow!("FTPS pin cache mutex poisoned: {error}"))?;
        if let Some(cached) = *guard {
            return Ok(cached);
        }
        let resolved = if let Some(stored) = self.load_pin()? {
            stored
        } else {
            self.save_pin(observed)?;
            *observed
        };
        *guard = Some(resolved);
        Ok(resolved)
    }
}

impl ServerCertVerifier for TofuCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let mut hasher = Sha256::new();
        hasher.update(end_entity.as_ref());
        let observed: [u8; SHA256_DIGEST_LEN] = hasher.finalize().into();
        let expected = self
            .expected_digest(&observed)
            .map_err(|error| rustls::Error::General(format!("FTPS pin handling: {error:#}")))?;
        if expected == observed {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "FTPS server cert SHA-256 {} does not match pin {} at {}; \
                 delete the pin file to re-trust",
                hex_encode(&observed),
                hex_encode(&expected),
                self.pin_path.display(),
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Builds a `rustls::ClientConfig` backed by `TofuCertVerifier`.
///
/// The returned config uses the rustls `ring` crypto provider and is suitable
/// for passing directly to `suppaftp`'s tokio-rustls connector.
///
/// # Errors
///
/// Returns an error if the crypto provider rejects the no-client-auth config
/// (this only happens if rustls is built without TLS 1.2 or 1.3 support).
pub fn rustls_config(verifier: Arc<TofuCertVerifier>) -> anyhow::Result<rustls::ClientConfig> {
    let provider = rustls::crypto::ring::default_provider();
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .context("failed to build rustls config with safe defaults")?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Ok(config)
}

/// Hex-encodes `bytes` as a lowercase string without separators.
fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(nibble_to_hex_char(byte >> 4));
        out.push(nibble_to_hex_char(byte & 0x0f));
    }
    out
}

/// Decodes a lowercase or uppercase hex string into bytes.
fn hex_decode(input: &str) -> anyhow::Result<Vec<u8>> {
    let trimmed = input.trim();
    if !trimmed.len().is_multiple_of(2) {
        anyhow::bail!("hex input must have even length");
    }
    let mut out = Vec::with_capacity(trimmed.len() / 2);
    let bytes = trimmed.as_bytes();
    for chunk in bytes.chunks_exact(2) {
        let hi = hex_char_to_nibble(chunk[0])?;
        let lo = hex_char_to_nibble(chunk[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

/// Maps a nibble (0..=15) to its lowercase hex character.
fn nibble_to_hex_char(nibble: u8) -> char {
    match nibble {
        0..=9 => char::from(b'0' + nibble),
        10..=15 => char::from(b'a' + (nibble - 10)),
        _ => '?',
    }
}

/// Maps a hex byte to its nibble value.
fn hex_char_to_nibble(byte: u8) -> anyhow::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        other => anyhow::bail!("non-hex byte {other:#x}"),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{TofuCertVerifier, hex_decode, hex_encode};

    /// Hex round-trip preserves the input bytes.
    #[test]
    fn hex_round_trip_preserves_bytes() -> anyhow::Result<()> {
        let bytes = [0x00, 0x01, 0x0f, 0x10, 0xab, 0xff];
        assert_eq!(hex_encode(&bytes), "00010f10abff");
        assert_eq!(hex_decode("00010F10AbFf")?, bytes);
        Ok(())
    }

    /// Hex decode rejects odd lengths and non-hex characters.
    #[test]
    fn hex_decode_rejects_invalid_input() {
        assert!(hex_decode("abc").is_err());
        assert!(hex_decode("xy").is_err());
    }

    /// On first use the verifier writes the observed digest to the pin file.
    #[test]
    fn tofu_first_use_writes_pin_file() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let pin = dir.path().join("ftps_cert_pin.txt");
        let verifier = TofuCertVerifier::new(pin.clone());
        let observed = [0x11_u8; 32];
        let expected = verifier.expected_digest(&observed)?;
        assert_eq!(expected, observed);
        let contents = fs::read_to_string(&pin)?;
        assert!(contents.contains(&hex_encode(&observed)));
        Ok(())
    }

    /// On subsequent calls the verifier loads the cached pin from disk.
    #[test]
    fn tofu_subsequent_use_loads_existing_pin() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let pin = dir.path().join("ftps_cert_pin.txt");
        // Pre-populate a pin file with a known digest.
        let stored = [0x22_u8; 32];
        fs::write(&pin, format!("{}\n", hex_encode(&stored)))?;
        let verifier = TofuCertVerifier::new(pin);
        let expected = verifier.expected_digest(&[0xff_u8; 32])?;
        // The stored digest wins; the observed one is not adopted.
        assert_eq!(expected, stored);
        Ok(())
    }

    /// Pin files written by the verifier itself include `#`-prefixed comments
    /// before the digest line, and the loader must skip them.
    #[test]
    fn tofu_load_skips_comments_and_blank_lines() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let pin = dir.path().join("ftps_cert_pin.txt");
        let stored = [0x33_u8; 32];
        fs::write(
            &pin,
            format!(
                "# header comment\n\
                 \n\
                 # second comment\n\
                 {}\n",
                hex_encode(&stored),
            ),
        )?;
        let verifier = TofuCertVerifier::new(pin);
        let expected = verifier.expected_digest(&[0xee_u8; 32])?;
        assert_eq!(expected, stored);
        Ok(())
    }
}
