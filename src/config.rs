use anyhow::Context;
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub mod preconfigured_secrets;

pub type HashedAuthSecret = [u8; 16];
pub fn compute_auth_hash_from_raw(raw: &str) -> HashedAuthSecret {
    let hash = Sha256::digest(raw.as_bytes());
    let mut res = [0u8; 16];
    res.copy_from_slice(&hash.0[..16]);
    res
}

pub struct CertificateBundle {
    pub certificate: CertificateDer<'static>,
    pub certificate_priv_key: PrivateKeyDer<'static>,
}

impl CertificateBundle {
    pub fn new<T: AsRef<Path>>(cert_path: T, priv_key_path: T) -> anyhow::Result<Self> {
        let certificate =
            CertificateDer::from_pem_file(cert_path).context("can't parse certificate")?;
        let certificate_priv_key =
            PrivateKeyDer::from_pem_file(priv_key_path).context("can't parse private key")?;
        Ok(Self {
            certificate,
            certificate_priv_key,
        })
    }
}

pub struct SecurityConfig {
    pub self_cert_bundle: CertificateBundle,
    pub ca_cert: CertificateDer<'static>,
    pub auth_secret: HashedAuthSecret,
}

impl SecurityConfig {
    pub fn new<T: AsRef<Path>>(
        cert_path: T,
        key_path: T,
        ca_path: T,
        shared_secret_raw: &str,
    ) -> anyhow::Result<Self> {
        let self_cert_bundle = CertificateBundle::new(cert_path, key_path)?;
        let ca_cert = CertificateDer::from_pem_file(ca_path)?;
        let auth_secret = compute_auth_hash_from_raw(shared_secret_raw);
        Ok(Self {
            self_cert_bundle,
            ca_cert,
            auth_secret,
        })
    }
}

