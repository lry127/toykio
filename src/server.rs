use crate::config::SecurityConfig;
use anyhow::Context;
use log::{info, log, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{ServerConfig, version};

struct ProxyHandler {
    tcp_stream: TcpStream,
    client_addr: SocketAddr,
    tls_acceptor: TlsAcceptor,
    auth_secret: Arc<String>,
}


impl ProxyHandler {
    async fn handle_client(self) -> anyhow::Result<()> {
        info!("get client from {}, secret is {}", self.client_addr, self.auth_secret);
        Ok(())
    }
}


pub struct ProxyServer {
    tls_acceptor: TlsAcceptor,
    listener: TcpListener,
    auth_secret: Arc<String>,
}

impl ProxyServer {
    pub async fn bind<T: ToSocketAddrs>(
        bind_addr: T,
        security_config: SecurityConfig,
    ) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(bind_addr).await?;
        let tls_config = Self::build_tls_config(&security_config)?;
        let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config));
        Ok(Self {
            listener,
            tls_acceptor,
            auth_secret: Arc::new(security_config.auth_secret),
        })
    }

    pub async fn server_loop(&mut self) {
        loop {
            match self.listener.accept().await {
                Ok((tcp_stream, addr)) => {
                    let acceptor = self.tls_acceptor.clone();
                    let auth_secret = self.auth_secret.clone();
                    let proxy_handler = ProxyHandler {
                        tcp_stream, client_addr:addr, tls_acceptor: acceptor, auth_secret
                    };
                    tokio::spawn(async move {
                        let res =  proxy_handler.handle_client().await;
                        if res.is_err() {
                            warn!("handle client failed: {res:?}")
                        }
                    });
                }
                Err(err) => warn!("can't accept stream: {err}"),
            }
        }
    }



    fn build_tls_config(security_config: &SecurityConfig) -> anyhow::Result<ServerConfig> {
        let mut pqc_provider = rustls_post_quantum::provider();
        pqc_provider.kx_groups = vec![rustls_post_quantum::X25519MLKEM768];

        ServerConfig::builder_with_provider(Arc::new(pqc_provider))
            .with_protocol_versions(&[&version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(
                vec![security_config.server_bundle.parse_certificate()?],
                security_config.server_bundle.parse_priv_key()?,
            )
            .context("can't build server config")
    }
}
