use crate::config::{HashedAuthSecret, SecurityConfig};
use crate::protocol::{
    ConnectionEstablishErrorType, ConnectionEstablishMessageC2S, ConnectionEstablishResponseS2C,
    WireMessage,
};
use crate::socks5::ConnectionServerReplyCode::{ConnectionRefused, GeneralFailure, Success};
use crate::socks5::{
    VariableHostRepr, construct_connection_server_reply, consume_client_hello,
    handle_target_addr_negotiation,
};
use crate::tls::build_client_tls_config;
use anyhow::{Context, bail};
use bytes::{BufMut, BytesMut};
use kcp_tokio::{KcpConfig, KcpStream};
use rustls::pki_types::ServerName;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, copy_bidirectional};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_rustls::TlsConnector;
use tracing::{debug, instrument, warn};

pub struct Socks5Processor {
    tcp_listener: TcpListener,
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
        let tcp_listener = TcpListener::bind(addr).await?;
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
            tcp_listener,
            auth_secret: Arc::new(security_config.auth_secret),
            tls_connector: Arc::new(tls_connector),
            server_addr: Arc::new(server_addr),
            server_hostname: servername,
        })
    }

    pub async fn run_socks5_loop(self) {
        loop {
            let (client, addr) = match self.tcp_listener.accept().await {
                Ok((client, addr)) => (client, addr),
                Err(err) => {
                    warn!("failed accept: {err}");
                    continue;
                }
            };
            let auth_secret = self.auth_secret.clone();
            let server_addr = self.server_addr.clone();
            let servername = self.server_hostname.clone();
            let tls_connector = self.tls_connector.clone();
            tokio::spawn(async move {
                let result = Self::handle_socks_client(
                    client,
                    &auth_secret,
                    &server_addr,
                    servername,
                    &tls_connector,
                    &addr,
                )
                .await;
                if let Err(err) = result {
                    warn!("handle client {addr} failed: {err}");
                } else {
                    debug!("handle client {addr} done");
                }
            });
        }
    }

    #[instrument(skip(proxy_client_stream, hashed_auth_secret, server_addr, tls_connector))]
    async fn handle_socks_client(
        mut proxy_client_stream: TcpStream,
        hashed_auth_secret: &HashedAuthSecret,
        server_addr: &SocketAddr,
        server_name: ServerName<'static>,
        tls_connector: &TlsConnector,
        _socks_req_addr: &SocketAddr,
    ) -> anyhow::Result<()> {
        debug!("client connected");
        let mut proxy_read_buf = BytesMut::with_capacity(512);
        let mut proxy_write_buf = BytesMut::with_capacity(512);

        if let Err(err) = consume_client_hello(
            &mut proxy_client_stream,
            &mut proxy_read_buf,
            &mut proxy_write_buf,
        )
        .await
        {
            proxy_client_stream.shutdown().await.ok();
            bail!(err.context("socks5 client hello failed"));
        }

        debug!("client hello successful");

        let (target_host, target_port) =
            match handle_target_addr_negotiation(&mut proxy_client_stream, &mut proxy_write_buf)
                .await
            {
                Ok(res) => res,
                Err(err) => {
                    proxy_client_stream.shutdown().await.ok();
                    bail!(err.context("socks5 connection establishment request failed"));
                }
            };

        let connection_result = async {
            let config = KcpConfig::file_transfer();
            let config = config.stream_mode(true);
            let tcp_stream = KcpStream::connect(*server_addr, config).await?;

            let tls_stream = tls_connector.connect(server_name, tcp_stream).await?;
            Ok::<_, anyhow::Error>(tls_stream)
        }
        .await;

        let mut server_stream = match connection_result {
            Ok(stream) => stream,
            Err(err) => {
                debug!("failed to connect to server {err}");
                proxy_write_buf.put_slice(&construct_connection_server_reply(GeneralFailure));
                proxy_client_stream
                    .write_all_buf(&mut proxy_write_buf)
                    .await
                    .ok();
                proxy_client_stream.shutdown().await.ok();
                Err(err).context("can't connect to remote proxy server")?
            }
        };

        debug!("tls handshake done");
        if let Err(err) = Self::establish_target_connection(
            &mut server_stream,
            hashed_auth_secret,
            target_host,
            target_port,
        )
        .await
        {
            debug!("proxy server to target connection failed: {}", err);
            server_stream.shutdown().await.ok();
            proxy_write_buf.put_slice(&construct_connection_server_reply(ConnectionRefused));
            proxy_client_stream
                .write_all_buf(&mut proxy_write_buf)
                .await
                .ok();
            proxy_client_stream.shutdown().await.ok();
            Err(err).context("remote server to target failed")?;
        }

        debug!("proxy server to target success");
        proxy_write_buf.put_slice(&construct_connection_server_reply(Success));
        proxy_client_stream
            .write_all_buf(&mut proxy_write_buf)
            .await
            .ok();

        debug!("begin proxy application data");
        let res = copy_bidirectional(&mut server_stream, &mut proxy_client_stream).await;

        debug!("proxy done: result: {res:?}");

        server_stream.shutdown().await.ok();
        proxy_client_stream.shutdown().await.ok();
        Ok(())
    }

    async fn establish_target_connection<T: AsyncRead + AsyncWrite + Unpin>(
        server_stream: &mut T,
        auth_secret: &HashedAuthSecret,
        target_host: VariableHostRepr,
        port: u16,
    ) -> anyhow::Result<()> {
        let c2s_conn_msg = ConnectionEstablishMessageC2S {
            hashed_auth_secret: *auth_secret,
            target_host,
            port,
        };
        let mut buf = BytesMut::with_capacity(32);
        c2s_conn_msg.serialize_to_bytes(&mut buf);
        server_stream.write_all_buf(&mut buf).await?;

        let resp = ConnectionEstablishResponseS2C::read_from_stream(server_stream).await?;
        if resp.error_type != ConnectionEstablishErrorType::Success {
            bail!(
                "failed to establish connection to target {:?}",
                resp.error_type
            );
        }
        Ok(())
    }
}
