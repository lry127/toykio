use anyhow::Context;
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use crate::config::{CertificateBundle, SecurityConfig};

const AUTH_SECRET: &str = "hello_world+123###@@@QwQ";


pub fn get_server_config() -> anyhow::Result<SecurityConfig> {
    let server_bundle = CertificateBundle::new("certs/server/server.crt",
    "certs/server/server.key")?;

    let ca_cert = CertificateDer::from_pem_file("certs/ca/ca.crt").context("failed to load ca")?;

    Ok(SecurityConfig {
        self_cert_bundle: server_bundle,
        auth_secret: AUTH_SECRET.into(),
        ca_cert
    })
}

pub fn get_client_config() -> anyhow::Result<SecurityConfig> {
    let client_bundle = CertificateBundle::new("certs/client/client.crt",
    "certs/client/client.key")?;

    let ca_cert = CertificateDer::from_pem_file("certs/ca/ca.crt").context("failed to load ca")?;

    Ok(SecurityConfig {
        self_cert_bundle: client_bundle,
        auth_secret: AUTH_SECRET.into(),
        ca_cert
    })
}

