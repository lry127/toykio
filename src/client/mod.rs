use crate::client::socks5::{consume_client_hello, handle_target_addr_negotiation};
use crate::config::{HashedAuthSecret, SecurityConfig};
use crate::net::{ConnectionManager, StreamConnection, StreamHandler, TcpStreamAcceptor};
use crate::tls::build_client_tls_config;
use anyhow::{Context, bail};
use bytes::BytesMut;
use rustls::pki_types::ServerName;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::ToSocketAddrs;
use tokio_rustls::TlsConnector;
use tracing::{debug, instrument};

pub(super) mod socks5;

pub struct Socks5Processor {
    socks5_connection_manager: ConnectionManager<TcpStreamAcceptor, Socks5Handler>,
    tls_connector: Arc<TlsConnector>,
    auth_secret: Arc<HashedAuthSecret>,
    server_addr: Arc<SocketAddr>,
    server_hostname: ServerName<'static>,
}

impl Socks5Processor {
    pub async fn new<T: ToSocketAddrs>(
        addr: T,
        security_config: SecurityConfig,
        server_host: &str,
        server_port: u16,
    ) -> anyhow::Result<Self> {
        let tcp_listener = TcpStreamAcceptor::bind(addr).await?;
        let socks5_client_handler = Socks5Handler {};
        let socks5_connection_manager = ConnectionManager::new(tcp_listener, socks5_client_handler);

        let server_addr = tokio::net::lookup_host((server_host, server_port))
            .await?
            .next()
            .context("can't resolve server addr")?;

        let tls_config = build_client_tls_config(
            security_config.self_cert_bundle.certificate,
            security_config.self_cert_bundle.certificate_priv_key,
            security_config.ca_cert,
        )?;

        let servername = ServerName::try_from(server_host)?.to_owned();

        let tls_connector = TlsConnector::from(Arc::new(tls_config));

        Ok(Self {
            socks5_connection_manager,
            auth_secret: Arc::new(security_config.auth_secret),
            tls_connector: Arc::new(tls_connector),
            server_addr: Arc::new(server_addr),
            server_hostname: servername,
        })
    }

    pub async fn run_socks5_loop(self) {
        self.socks5_connection_manager.run_accept_loop().await;
    }
}

struct Socks5Handler;

impl StreamHandler for Socks5Handler {
    #[instrument(skip(self, stream))]
    async fn handle_stream<T: StreamConnection + 'static>(
        &self,
        mut stream: T,
        addr: SocketAddr,
    ) -> anyhow::Result<()> {
        debug!("client connected");
        let mut proxy_read_buf = BytesMut::with_capacity(64);
        let mut proxy_write_buf = BytesMut::with_capacity(16);

        if let Err(err) =
            consume_client_hello(&mut stream, &mut proxy_read_buf, &mut proxy_write_buf).await
        {
            stream.shutdown().await.ok();
            bail!(err.context("socks5 client hello failed"));
        }

        debug!("client hello successful");

        let (target_host, target_port) = match handle_target_addr_negotiation(
            &mut stream,
            &mut proxy_read_buf,
            &mut proxy_write_buf,
        )
        .await
        {
            Ok(res) => res,
            Err(err) => {
                stream.shutdown().await.ok();
                bail!(err.context("socks5 connection establishment request failed"));
            }
        };

        Ok(())
    }
}
