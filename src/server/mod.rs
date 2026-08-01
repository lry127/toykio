mod proxy_manager;
mod server_connection_handler;

use crate::cli::TransportType;
use crate::config::{HashedAuthSecret, SecurityConfig};
use crate::net::{ConnectionManager, KcpStreamAcceptor, StreamAcceptor, TcpStreamAcceptor};
use crate::server::server_connection_handler::ServerConnectionHandler;
use crate::tls::{ServerTlsStreamHandler, build_server_tls_config};
use kcp_tokio::KcpConfig;
use rustls::ServerConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::ToSocketAddrs;

pub struct ProxyServer;

impl ProxyServer {
    pub async fn run_server_loop<T: ToSocketAddrs>(
        bind_addr: T,
        transport_type: TransportType,
        security_config: SecurityConfig,
    ) -> anyhow::Result<()> {
        let (server_config, hashed_secret) = Self::extract_from_security_config(security_config)?;

        match transport_type {
            TransportType::Tcp => {
                let listener = TcpStreamAcceptor::bind(bind_addr).await?;
                Self::server_loop_inner(listener, server_config, hashed_secret).await;
            }
            TransportType::Kcp => {
                let config = KcpConfig::file_transfer();
                let listener = KcpStreamAcceptor::bind(bind_addr, config).await?;
                Self::server_loop_inner(listener, server_config, hashed_secret).await;
            }
        }
        Ok(())
    }

    fn extract_from_security_config(
        security_config: SecurityConfig,
    ) -> anyhow::Result<(ServerConfig, HashedAuthSecret)> {
        let tls_config = build_server_tls_config(
            security_config.self_cert_bundle.certificate,
            security_config.self_cert_bundle.certificate_priv_key,
            security_config.ca_cert,
        )?;

        Ok((tls_config, security_config.auth_secret))
    }

    async fn server_loop_inner<T: StreamAcceptor + Send + Sync + 'static>(
        listener: T,
        tls_config: ServerConfig,
        auth_secret: HashedAuthSecret,
    ) {
        let server_connection_handler = ServerConnectionHandler::new(auth_secret);

        let tls_wrapped_handler = ServerTlsStreamHandler::new(
            Arc::new(tls_config),
            Duration::from_secs(10),
            server_connection_handler,
        );

        let connection_manager = ConnectionManager::new(listener, tls_wrapped_handler);
        connection_manager.run_accept_loop().await;
    }
}
