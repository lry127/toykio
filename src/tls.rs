use anyhow::Context;
use std::sync::Arc;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{version, ClientConfig, RootCertStore, ServerConfig};

pub fn build_server_tls_config(
    certificate: CertificateDer<'static>, priv_key: PrivateKeyDer<'static>,
    ca_cert: CertificateDer<'static>
) -> anyhow::Result<ServerConfig> {
    let mut pqc_provider = rustls_post_quantum::provider();
    pqc_provider.kx_groups = vec![rustls_post_quantum::X25519MLKEM768];

    let mut roots = RootCertStore::empty();
    roots.add(ca_cert)?;
    let client_cert_verifier =
        WebPkiClientVerifier::builder(Arc::new(roots)).build()?;

    ServerConfig::builder_with_provider(Arc::new(pqc_provider))
        .with_protocol_versions(&[&version::TLS13])?
        .with_client_cert_verifier(client_cert_verifier)
        .with_single_cert(vec![certificate], priv_key, )
        .context("can't build server config")
}


pub fn build_client_tls_config(
    certificate: CertificateDer<'static>, priv_key: PrivateKeyDer<'static>,
    ca_cert: CertificateDer<'static>
) -> anyhow::Result<ClientConfig> {
    let mut pqc_provider = rustls_post_quantum::provider();
    pqc_provider.kx_groups = vec![rustls_post_quantum::X25519MLKEM768];

    let mut roots = RootCertStore::empty();
    roots.add(ca_cert)?;

    ClientConfig::builder_with_provider(Arc::new(pqc_provider))
        .with_protocol_versions(&[&version::TLS13])?
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![certificate], priv_key)
        .context("can't build client config")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::preconfigured_secrets::{get_client_config, get_server_config};
    use rustls::pki_types::ServerName;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    #[tokio::test]
    async fn test_pqc_mtls_connection() -> anyhow::Result<()> {
        let server_sec_config = get_server_config()?;
        let client_sec_config = get_client_config()?;

        let server_cert = server_sec_config.self_cert_bundle.certificate;
        let server_key = server_sec_config.self_cert_bundle.certificate_priv_key;

        let client_cert = client_sec_config.self_cert_bundle.certificate;
        let client_key = client_sec_config.self_cert_bundle.certificate_priv_key;


        let server_config = build_server_tls_config(
            server_cert,
            server_key,
            server_sec_config.ca_cert
        )?;

        let client_config = build_client_tls_config(
            client_cert,
            client_key,
            client_sec_config.ca_cert
        )?;

        let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));
        let tls_connector = TlsConnector::from(Arc::new(client_config));

        let listener = TcpListener::bind("127.0.0.1:12340").await?;
        let addr = listener.local_addr()?;

        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("Failed to accept tcp connection");
            let mut tls_stream = tls_acceptor.accept(stream).await.expect("TLS handshake failed on server");

            tls_stream.write_all(b"Handshake successful").await.expect("Failed to write to stream");
            tls_stream.shutdown().await.expect("Failed to shutdown stream");
        });

        let stream = TcpStream::connect(addr).await?;
        let domain = ServerName::try_from("localhost")?;
        let mut tls_stream = tls_connector.connect(domain, stream).await?;

        let mut buf = vec![0; 20];
        tls_stream.read_exact(&mut buf).await?;

        assert_eq!(&buf, b"Handshake successful");

        let _ = server_task.await;
        Ok(())
    }
}
