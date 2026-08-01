use crate::cli::TransportType;
use crate::cli::TransportType::Kcp;
use crate::client::c2s_connections::{H2MultiplexerHandle, H2ToServerMultiplexer};
use crate::client::socks5::{
    ConnectionServerReplyCode, construct_connection_server_reply, consume_client_hello,
    handle_target_addr_negotiation,
};
use crate::config::{HashedAuthSecret, SecurityConfig};
use crate::data_endpoint::{BidirectionalCopier, H2StreamEndpoint, TcpStreamDataEndpoint};
use crate::net::{
    KcpConnector, StreamAcceptor, StreamConnector, TcpConnector, TcpStreamAcceptor,
    TlsStreamConnector,
};
use crate::tls::build_client_tls_config;
use anyhow::{Context, bail};
use bytes::{BufMut, BytesMut};
use kcp_tokio::KcpConfig;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::{debug, instrument, warn};

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
        let h2_multiplexer =
            H2ToServerMultiplexer::new(tls_connector, self.server_addr, self.auth_secret);
        let multiplexer_handler = h2_multiplexer.run_multiplexer_loop();

        let mut socks5_listener = TcpStreamAcceptor::bind(bind_addr).await?;
        let socks5_client_handler = Socks5Handler {
            h2_multiplexer: multiplexer_handler,
            id: AtomicUsize::new(0),
        };
        let socks5_client_handler = Arc::new(socks5_client_handler);

        loop {
            let (s, addr) = match socks5_listener.accept_stream().await {
                Ok(stream) => stream,
                Err(err) => {
                    warn!("failed to accept new stream: {err}");
                    continue;
                }
            };
            let handler = socks5_client_handler.clone();
            tokio::spawn(async move {
                if let Err(err) = handler.handle_stream(s, addr).await {
                    warn!("failed to handle stream: {err}");
                };
            });
        }
    }
}

struct Socks5Handler {
    h2_multiplexer: H2MultiplexerHandle,
    id: AtomicUsize,
}
impl Socks5Handler {
    async fn handle_stream(&self, stream: TcpStream, addr: SocketAddr) -> anyhow::Result<()> {
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        self.handle_stream_inner(stream, id, addr).await
    }
}

impl Socks5Handler {
    #[instrument(skip(self, stream))]
    async fn handle_stream_inner(
        &self,
        mut stream: TcpStream,
        id: usize,
        addr: SocketAddr,
    ) -> anyhow::Result<()> {
        let _ = addr; // suppress warning

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

        debug!("client wants to connect to {target_host:?}:{target_port}");

        let h2_proxy_stream = match self
            .h2_multiplexer
            .create_new_proxy_stream(target_host.to_str_repr(), target_port)
            .await
        {
            Ok(proxy_stream) => proxy_stream,
            Err(err) => {
                proxy_write_buf.put_slice(&construct_connection_server_reply(
                    ConnectionServerReplyCode::GeneralFailure,
                ));
                stream.write_all_buf(&mut proxy_write_buf).await.ok();
                stream.shutdown().await.ok();
                bail!(err)
            }
        };

        debug!("h2 multiplexer accepted new proxy request");

        let resp_recv_stream = match h2_proxy_stream.resp_fut.await {
            Ok(recv_stream) => recv_stream,
            Err(err) => {
                proxy_write_buf.put_slice(&construct_connection_server_reply(
                    ConnectionServerReplyCode::GeneralFailure,
                ));
                stream.write_all_buf(&mut proxy_write_buf).await.ok();
                stream.shutdown().await.ok();
                bail!(err)
            }
        };
        if resp_recv_stream.status() != 200 {
            proxy_write_buf.put_slice(&construct_connection_server_reply(
                ConnectionServerReplyCode::ConnectionRefused,
            ));
            stream.write_all_buf(&mut proxy_write_buf).await.ok();
            stream.shutdown().await.ok();
            bail!("server actively rejected our request");
        }

        proxy_write_buf.put_slice(&construct_connection_server_reply(
            ConnectionServerReplyCode::Success,
        ));
        stream.write_all_buf(&mut proxy_write_buf).await.ok();

        let h2_to_server_data_endpoint = H2StreamEndpoint {
            send_to_client: h2_proxy_stream.send_stream,
            recv_from_client: resp_recv_stream.into_body(),
        };
        let proxy_client_endpoint = TcpStreamDataEndpoint {
            stream,
            read_buf_len: 8192,
        };
        let copier = BidirectionalCopier {
            endpoint_a: proxy_client_endpoint,
            endpoint_b: h2_to_server_data_endpoint,
        };
        copier.run_copy_task(id);

        debug!("proxy done");
        Ok(())
    }
}
