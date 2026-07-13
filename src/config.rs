use std::path::Path;
use anyhow::Context;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub mod preconfigured_secrets;

pub struct CertificateBundle {
    pub certificate: CertificateDer<'static>,
    pub certificate_priv_key: PrivateKeyDer<'static>,
}

impl CertificateBundle {
    pub fn new<T: AsRef<Path>>(cert_path: T, priv_key_path: T) -> anyhow::Result<Self> {
        let certificate = CertificateDer::from_pem_file(cert_path).context("can't parse certificate")?;
        let certificate_priv_key = PrivateKeyDer::from_pem_file(priv_key_path).context("can't parse private key")?;
        Ok(Self{
            certificate,
            certificate_priv_key
        })
    }
}

pub struct SecurityConfig {
    pub self_cert_bundle: CertificateBundle,
    pub ca_cert: CertificateDer<'static>,
    pub auth_secret: String,
}
