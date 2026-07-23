pub mod proxy_manager;
pub mod server_connection_handler;

use crate::config::SecurityConfig;
use crate::net::{ConnectionManager, KcpConfig, KcpStreamAcceptor, TcpStreamAcceptor};
use crate::server::server_connection_handler::ServerConnectionHandler;
use crate::tls::{ServerTlsStreamHandler, build_server_tls_config};
use std::sync::Arc;
use std::time::Duration;

pub struct ProxyServer;

impl ProxyServer {
    pub async fn run_tcp(bind_addr: &str, security_config: SecurityConfig) -> anyhow::Result<()> {
        let tcp_acceptor = TcpStreamAcceptor::bind(bind_addr).await?;
        Self::run_with_acceptor(tcp_acceptor, security_config).await
    }

    pub async fn run_kcp(
        bind_addr: &str,
        security_config: SecurityConfig,
        kcp_config: KcpConfig,
    ) -> anyhow::Result<()> {
        let kcp_acceptor = KcpStreamAcceptor::bind(bind_addr, kcp_config).await?;
        Self::run_with_acceptor(kcp_acceptor, security_config).await
    }

    async fn run_with_acceptor<A>(
        acceptor: A,
        security_config: SecurityConfig,
    ) -> anyhow::Result<()>
    where
        A: crate::net::StreamAcceptor + Send + Sync + 'static,
    {
        let tls_config = build_server_tls_config(
            security_config.self_cert_bundle.certificate,
            security_config.self_cert_bundle.certificate_priv_key,
            security_config.ca_cert,
        )?;

        let h2_handler = ServerConnectionHandler::new(security_config.auth_secret);
        let tls_handler =
            ServerTlsStreamHandler::new(Arc::new(tls_config), Duration::from_secs(10), h2_handler);

        let manager = ConnectionManager::new(acceptor, tls_handler);
        manager.run_accept_loop().await;
        Ok(())
    }
}
