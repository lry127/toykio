use crate::net::{StreamConnection, StreamHandler};
use anyhow::Context;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig, version};

pub fn build_server_tls_config(
    certificate: CertificateDer<'static>,
    priv_key: PrivateKeyDer<'static>,
    ca_cert: CertificateDer<'static>,
) -> anyhow::Result<ServerConfig> {
    let mut pqc_provider = rustls_post_quantum::provider();
    pqc_provider.kx_groups = vec![rustls_post_quantum::X25519MLKEM768];

    let mut roots = RootCertStore::empty();
    roots.add(ca_cert)?;
    let client_cert_verifier = WebPkiClientVerifier::builder(Arc::new(roots)).build()?;

    ServerConfig::builder_with_provider(Arc::new(pqc_provider))
        .with_protocol_versions(&[&version::TLS13])?
        .with_client_cert_verifier(client_cert_verifier)
        .with_single_cert(vec![certificate], priv_key)
        .context("can't build server config")
}

pub fn build_client_tls_config(
    certificate: CertificateDer<'static>,
    priv_key: PrivateKeyDer<'static>,
    ca_cert: CertificateDer<'static>,
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

#[derive(Clone)]
pub struct ServerTlsStreamHandler<T: StreamHandler> {
    accept_max_time: Arc<Duration>,
    tls_acceptor: Arc<TlsAcceptor>,
    inner_handler: T,
}

impl<U: StreamHandler + Sync> StreamHandler for ServerTlsStreamHandler<U> {
    async fn handle_stream<T: StreamConnection + 'static>(
        &self,
        stream: T,
        addr: SocketAddr,
    ) -> anyhow::Result<()> {
        let stream = timeout(*self.accept_max_time, self.tls_acceptor.accept(stream))
            .await
            .context("tls handshake timeout")?
            .context("tls handshake failed")?;
        self.inner_handler.handle_stream(stream, addr).await
    }
}

impl<T: StreamHandler> ServerTlsStreamHandler<T> {
    pub fn new(
        server_config: Arc<ServerConfig>,
        handshake_timeout: Duration,
        inner_handler: T,
    ) -> Self {
        let tls_acceptor = TlsAcceptor::from(server_config);
        Self {
            accept_max_time: Arc::new(handshake_timeout),
            tls_acceptor: Arc::new(tls_acceptor),
            inner_handler,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::preconfigured_secrets::{get_client_config, get_server_config};
    use crate::net::{ConnectionManager, KcpStreamAcceptor, StreamAcceptor, TcpStreamAcceptor};
    use crate::test_helpers::SimpleEchoHandler;
    use kcp_tokio::{KcpConfig, KcpStream};
    use rustls::pki_types::ServerName;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    fn get_server_tls_cfg() -> anyhow::Result<ServerConfig> {
        let server_sec_config = get_server_config()?;
        let server_cert = server_sec_config.self_cert_bundle.certificate;
        let server_key = server_sec_config.self_cert_bundle.certificate_priv_key;
        let server_config =
            build_server_tls_config(server_cert, server_key, server_sec_config.ca_cert)?;
        Ok(server_config)
    }

    fn get_client_tls_cfg() -> anyhow::Result<ClientConfig> {
        let client_sec_config = get_client_config()?;
        let client_cert = client_sec_config.self_cert_bundle.certificate;
        let client_key = client_sec_config.self_cert_bundle.certificate_priv_key;
        let client_config =
            build_client_tls_config(client_cert, client_key, client_sec_config.ca_cert)?;
        Ok(client_config)
    }

    #[tokio::test]
    async fn test_pqc_mtls_connection() -> anyhow::Result<()> {
        let tls_acceptor = TlsAcceptor::from(Arc::new(get_server_tls_cfg()?));
        let tls_connector = TlsConnector::from(Arc::new(get_client_tls_cfg()?));

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let server_task = tokio::spawn(async move {
            let (stream, _) = listener
                .accept()
                .await
                .expect("Failed to accept tcp connection");
            let mut tls_stream = tls_acceptor
                .accept(stream)
                .await
                .expect("TLS handshake failed on server");

            tls_stream
                .write_all(b"Handshake successful")
                .await
                .expect("Failed to write to stream");
            tls_stream
                .shutdown()
                .await
                .expect("Failed to shutdown stream");
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

    #[tokio::test]
    async fn tls_handler_works_with_manager() -> anyhow::Result<()> {
        let addr = "127.0.0.1:0";
        let msg = b"hello async world";

        let tls_echo_handler = ServerTlsStreamHandler::new(
            Arc::new(get_server_tls_cfg()?),
            Duration::from_secs(10),
            SimpleEchoHandler,
        );

        // tls
        {
            let acceptor = TcpStreamAcceptor::bind(addr).await?;
            let local_addr = acceptor.get_local_addr().unwrap();
            println!("tcp: {local_addr}");

            let manager = ConnectionManager::new(acceptor, tls_echo_handler.clone());
            tokio::spawn(async move {
                manager.run_accept_loop().await;
            });

            let stream = TcpStream::connect(local_addr).await?;
            let domain = ServerName::try_from("localhost")?;
            let tls_connector = TlsConnector::from(Arc::new(get_client_tls_cfg()?));
            let mut client = tls_connector.connect(domain, stream).await?;

            client.write_all(msg).await?;
            let mut buf = vec![0; msg.len()];
            client.read_exact(&mut buf).await?;

            assert_eq!(&buf, msg);
        }

        // kcp
        {
            let acceptor = KcpStreamAcceptor::bind(addr, KcpConfig::realtime()).await?;
            let local_addr = acceptor.get_local_addr().unwrap();
            println!("kcp: {local_addr}");

            let manager = ConnectionManager::new(acceptor, tls_echo_handler.clone());
            tokio::spawn(async move {
                manager.run_accept_loop().await;
            });

            let stream = KcpStream::connect(local_addr, KcpConfig::realtime()).await?;
            let domain = ServerName::try_from("localhost")?;
            let tls_connector = TlsConnector::from(Arc::new(get_client_tls_cfg()?));
            let mut client = tls_connector.connect(domain, stream).await?;

            client.write_all(msg).await?;
            let mut buf = vec![0; msg.len()];
            client.read_exact(&mut buf).await?;

            let _ = client.shutdown().await;
            assert_eq!(&buf, msg);
        }
        Ok(())
    }
}
