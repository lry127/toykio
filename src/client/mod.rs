use crate::cli::TransportType;
use crate::cli::TransportType::Kcp;
use crate::client::c2s_connections::{H2MultiplexerHandle, H2ToServerMultiplexer};
use crate::client::socks5::{consume_client_hello, handle_target_addr_negotiation};
use crate::config::{HashedAuthSecret, SecurityConfig};
use crate::net::{
    ConnectionManager, KcpConnector, StreamConnection, StreamConnector, StreamHandler,
    TcpConnector, TcpStreamAcceptor, TlsStreamConnector,
};
use crate::tls::build_client_tls_config;
use anyhow::{Context, bail};
use bytes::BytesMut;
use kcp_tokio::KcpConfig;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;
use tokio::net::ToSocketAddrs;
use tracing::{debug, instrument};

pub(super) mod socks5;

pub(super) mod c2s_connections;

pub struct Socks5Processor {
    client_config: ClientConfig,
    auth_secret: HashedAuthSecret,
    server_addr: SocketAddr,
    server_hostname: ServerName<'static>,
    transport_type: TransportType,
}

impl Socks5Processor {
    pub async fn new(
        server_host: &str,
        server_port: u16,
        transport_type: TransportType,
        security_config: SecurityConfig,
    ) -> anyhow::Result<Self> {
        let server_addr = tokio::net::lookup_host((server_host, server_port))
            .await?
            .next()
            .context("can't resolve server addr")?;

        let client_config = build_client_tls_config(
            security_config.self_cert_bundle.certificate,
            security_config.self_cert_bundle.certificate_priv_key,
            security_config.ca_cert,
        )?;

        let servername = ServerName::try_from(server_host)?.to_owned();

        Ok(Self {
            auth_secret: security_config.auth_secret,
            client_config,
            server_addr,
            server_hostname: servername,
            transport_type,
        })
    }
    pub async fn run_processor<T: ToSocketAddrs>(self, bind_addr: T) -> anyhow::Result<()> {
        match self.transport_type {
            TransportType::Tcp => self.run_with_raw_connector(bind_addr, TcpConnector).await,
            Kcp => {
                self.run_with_raw_connector(
                    bind_addr,
                    KcpConnector {
                        kcp_config: KcpConfig::file_transfer(),
                    },
                )
                .await
            }
        }
    }

    async fn run_with_raw_connector<
        T: ToSocketAddrs,
        U: StreamConnector + Send + Sync + 'static,
    >(
        self,
        bind_addr: T,
        connector: U,
    ) -> anyhow::Result<()> {
        let tls_connector =
            TlsStreamConnector::new(self.client_config, self.server_hostname, connector);
        let h2_multiplexer = H2ToServerMultiplexer::new(tls_connector, self.server_addr);
        let multiplexer_handler = h2_multiplexer.run_multiplexer_loop();

        let socks5_listener = TcpStreamAcceptor::bind(bind_addr).await?;
        let socks5_client_handler = Socks5Handler {
            h2_multiplexer: multiplexer_handler,
        };
        let socks5_connection_manager =
            ConnectionManager::new(socks5_listener, socks5_client_handler);
        socks5_connection_manager.run_accept_loop().await;
        Ok(())
    }
}

struct Socks5Handler {
    h2_multiplexer: H2MultiplexerHandle,
}

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
