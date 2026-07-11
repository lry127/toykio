use anyhow::Context;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub mod preconfigured_secrets;

pub struct CertificateBundle {
    pub certificate_data: String,
    pub certificate_priv_key: Option<String>,
}

impl CertificateBundle {
    pub fn parse_certificate(&self) -> anyhow::Result<CertificateDer<'static>> {
        Ok(CertificateDer::from_pem_slice(self.certificate_data.as_bytes())
            .context("failed to public key")
            ?.into_owned())
    }

    pub fn parse_priv_key(&self) -> anyhow::Result<PrivateKeyDer<'static>> {
        let key_str = self.certificate_priv_key.as_ref().context("key is none")?;
        Ok(PrivateKeyDer::from_pem_slice(key_str.as_bytes()).context("failed to parse private key")?.clone_key())
    }
}

pub struct SecurityConfig {
    pub server_bundle: CertificateBundle,
    pub client_bundle: CertificateBundle,
    pub auth_secret: String,
}
