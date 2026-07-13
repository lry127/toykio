use crate::config::SecurityConfig;
use crate::protocol::{
    compute_auth_hash_from_raw, ConnectionEstablishMessageC2S, ConnectionEstablishResponseS2C, HashedAuthSecret,
    WireMessage,
};
use anyhow::{anyhow, bail, Context};
use bytes::BytesMut;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddrV4;
use std::sync::Arc;
use tokio::io::{copy_bidirectional, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_rustls::rustls::{version, ServerConfig};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, instrument, warn};

struct ProxyHandler {
    tcp_stream: TcpStream,
    tls_acceptor: TlsAcceptor,
    auth_secret: Arc<HashedAuthSecret>,
}

impl ProxyHandler {
    fn new(
        tcp_stream: TcpStream,
        tls_acceptor: TlsAcceptor,
        auth_secret: Arc<HashedAuthSecret>,
    ) -> Self {
        Self {
            tcp_stream,
            tls_acceptor,
            auth_secret,
        }
    }
}

impl Debug for ProxyHandler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyHandler")
            .field("peer_addr", &self.tcp_stream.peer_addr())
            .finish()
    }
}

impl ProxyHandler {
    #[instrument]
    async fn handle_client(self) -> anyhow::Result<()> {
        let mut tls_stream = self.tls_acceptor.accept(self.tcp_stream).await?;
        debug!("tls handshake ok");

        let mut target_stream =
            Self::handle_connection_establishment(&self.auth_secret, &mut tls_stream).await?;
        if let Err(err) = copy_bidirectional(&mut target_stream, &mut tls_stream).await {
            warn!("err copy bidirectional: {err}")
        } else {
            debug!("copy bidirectional done")
        }

        Ok(())
    }

    #[instrument(skip(stream))]
    async fn handle_connection_establishment<T: AsyncRead + AsyncWrite + Unpin>(
        auth_secret: &Arc<HashedAuthSecret>,
        stream: &mut T,
    ) -> anyhow::Result<TcpStream> {
        match ConnectionEstablishMessageC2S::read_from_stream(stream).await {
            Ok(msg) => {
                if msg.hashed_auth_secret != **auth_secret {
                    warn!("invalid auth secret, connection aborted");
                    debug!(
                        "correct: {:?}, provided: {:?}",
                        *auth_secret, msg.hashed_auth_secret
                    );
                    let _ = Self::write_message(
                        stream,
                        &ConnectionEstablishResponseS2C { error_type: 1u16 },
                    )
                    .await;
                    let _ = stream.shutdown().await;
                    bail!("incorrect auth secret");
                }
                let target_socket_addr = SocketAddrV4::new(msg.ip.into(), msg.port);
                let target = TcpStream::connect(target_socket_addr).await?;
                debug!("remote connected: {target:?}");
                Ok(target)
            }
            Err(err) => {
                let _ = stream.shutdown().await;
                Err(anyhow!("can't read establishment msg: {err}"))
            }
        }
    }

    async fn write_message<T: AsyncWrite + Unpin, M: WireMessage>(
        tx: &mut T,
        message: &M,
    ) -> anyhow::Result<()> {
        let mut buf = BytesMut::with_capacity(32);
        message.serialize_to_bytes(&mut buf);
        tx.write_all_buf(&mut buf)
            .await
            .context("write message failed")
    }
}

pub struct ProxyServer {
    tls_acceptor: TlsAcceptor,
    listener: TcpListener,
    auth_secret: Arc<HashedAuthSecret>,
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
            auth_secret: Arc::new(compute_auth_hash_from_raw(&security_config.auth_secret)),
        })
    }

    pub async fn server_loop(&mut self) {
        loop {
            match self.listener.accept().await {
                Ok((tcp_stream, _)) => {
                    let acceptor = self.tls_acceptor.clone();
                    let auth_secret = self.auth_secret.clone();
                    let proxy_handler = ProxyHandler::new(tcp_stream, acceptor, auth_secret);
                    tokio::spawn(async move {
                        let res = proxy_handler.handle_client().await;
                        if res.is_err() {
                            warn!("handle client failed: {res:?}")
                        }
                    });
                }
                Err(err) => warn!(%err, "can't accept stream"),
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
