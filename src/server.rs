use crate::config::{HashedAuthSecret, SecurityConfig};
use crate::protocol::{
    ConnectionEstablishErrorType, ConnectionEstablishMessageC2S, ConnectionEstablishResponseS2C,
    WireMessage,
};
use crate::tls::build_server_tls_config;
use anyhow::{anyhow, bail, Context};
use bytes::BytesMut;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{copy_bidirectional, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, instrument, warn};

struct ProxyHandler {
    tcp_stream: TcpStream,
    tls_acceptor: TlsAcceptor,
    auth_secret: Arc<HashedAuthSecret>,
}

impl Debug for ProxyHandler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyHandler")
            .field("peer_addr", &self.tcp_stream.peer_addr())
            .finish()
    }
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

    #[instrument]
    async fn handle_client(self) -> anyhow::Result<()> {
        let mut client_stream = timeout(
            Duration::from_secs(10),
            self.tls_acceptor.accept(self.tcp_stream),
        )
        .await
        .context("tls handshake timeout")?
        .context("tls handshake failed")?;

        debug!("tls handshake ok");

        let mut target_stream = match Self::handle_connection_establishment(
            &self.auth_secret,
            &mut client_stream,
        )
        .await
        {
            Ok(stream) => stream,
            Err(err) => {
                debug!("connection establishment failed: {err}");
                let _ = client_stream.shutdown().await;
                return Err(err);
            }
        };

        if let Err(err) = copy_bidirectional(&mut target_stream, &mut client_stream).await {
            warn!("err copy bidirectional: {err}")
        } else {
            debug!("copy bidirectional done")
        }
        let _ = target_stream.shutdown().await;
        let _ = client_stream.shutdown().await;

        Ok(())
    }

    #[instrument(skip(client_stream))]
    async fn handle_connection_establishment<T: AsyncRead + AsyncWrite + Unpin>(
        auth_secret: &HashedAuthSecret,
        client_stream: &mut T,
    ) -> anyhow::Result<TcpStream> {
        let msg = timeout(
            Duration::from_secs(10),
            ConnectionEstablishMessageC2S::read_from_stream(client_stream),
        )
        .await
        .context("ConnectionEstablishMessageC2S timeout")?
        .context("can't read establishment msg")?;

        if msg.hashed_auth_secret != *auth_secret {
            warn!("invalid auth secret, connection aborted");
            debug!(
                "correct: {:?}, provided: {:?}",
                *auth_secret, msg.hashed_auth_secret
            );
            let _ = Self::write_message(
                client_stream,
                &ConnectionEstablishResponseS2C {
                    error_type: ConnectionEstablishErrorType::AuthError,
                },
            )
            .await;
            bail!("incorrect auth secret");
        }

        let target_socket_addr = SocketAddrV4::new(msg.ip.into(), msg.port);
        debug!("target is {target_socket_addr}");

        let target_stream = timeout(
            Duration::from_secs(10),
            TcpStream::connect(target_socket_addr),
        )
        .await;
        let error_type = match target_stream {
            Ok(Ok(_)) => ConnectionEstablishErrorType::Success,
            _ => ConnectionEstablishErrorType::TargetError,
        };

        let write_res = Self::write_message(
            client_stream,
            &ConnectionEstablishResponseS2C { error_type },
        )
        .await;
        match (target_stream, write_res) {
            (Ok(Ok(target)), Ok(_)) => Ok(target),
            (Err(_), _) => Err(anyhow!("timeout when establish connection to target")),
            (Ok(Err(err)), _) => Err(err).context("can't establish connection to target"),
            (_, Err(err)) => Err(err).context("can't send establish reply to client"),
        }
    }

    async fn write_message<T: AsyncWrite + Unpin, M: WireMessage>(
        tx: &mut T,
        message: &M,
    ) -> anyhow::Result<()> {
        let mut buf = BytesMut::with_capacity(32);
        message.serialize_to_bytes(&mut buf);
        timeout(Duration::from_secs(5), tx.write_all_buf(&mut buf))
            .await
            .context("write timeout")?
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
        let tls_config = build_server_tls_config(
            security_config.self_cert_bundle.certificate,
            security_config.self_cert_bundle.certificate_priv_key,
            security_config.ca_cert,
        )?;
        let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config));
        Ok(Self {
            listener,
            tls_acceptor,
            auth_secret: Arc::new(security_config.auth_secret),
        })
    }

    pub async fn server_loop(&mut self) {
        loop {
            let client_stream = match self.listener.accept().await {
                Ok((stream, _)) => stream,
                Err(err) => {
                    warn!(%err, "can't accept stream");
                    continue;
                }
            };

            let acceptor = self.tls_acceptor.clone();
            let auth_secret = self.auth_secret.clone();
            let proxy_handler = ProxyHandler::new(client_stream, acceptor, auth_secret);
            tokio::spawn(async move {
                let res = proxy_handler.handle_client().await;
                if let Err(err) = res {
                    warn!("handle client failed: {err:#}")
                }
            });
        }
    }
}
