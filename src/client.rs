use crate::config::SecurityConfig;
use crate::net::{KcpConfig, KcpStream, StreamConnection};
use crate::server::proxy_manager::{DataEndpoint, DataReader, DataWriter, ProxyManager};
use crate::server::server_connection_handler::PROTOCOL_MAGIC;
use crate::socks5::ConnectionServerReplyCode::{GeneralFailure, Success};
use crate::socks5::{
    VariableHostRepr, construct_connection_server_reply, consume_client_hello,
    handle_target_addr_negotiation,
};
use crate::tls::build_client_tls_config;
use anyhow::{Context, bail};
use bytes::{BufMut, Bytes, BytesMut};
use h2::client;
use h2::client::SendRequest;
use h2::{RecvStream, SendStream};
use http::{Method, Request};
use rustls::pki_types::ServerName;
use std::future::poll_fn;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tokio_rustls::TlsConnector;
use tracing::{debug, error, info, instrument, warn};

pub struct ClientConnectionManager {
    security_config: SecurityConfig,
    remote_addr: SocketAddr,
    server_name: ServerName<'static>,
    protocol: crate::cli::Protocol,
    kcp_config: Option<KcpConfig>,
    h2_sender: Arc<Mutex<Option<SendRequest<Bytes>>>>,
}

impl ClientConnectionManager {
    pub fn new(
        security_config: SecurityConfig,
        remote_addr: SocketAddr,
        server_name: ServerName<'static>,
        protocol: crate::cli::Protocol,
        kcp_config: Option<KcpConfig>,
    ) -> Self {
        Self {
            security_config,
            remote_addr,
            server_name,
            protocol,
            kcp_config,
            h2_sender: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn get_h2_stream(&self, target: String) -> anyhow::Result<H2StreamEndpoint> {
        loop {
            let mut sender_opt = self.h2_sender.lock().await;
            if let Some(mut sender) = sender_opt.clone() {
                // Try to use existing sender
                if poll_fn(|cx| sender.poll_ready(cx)).await.is_ok() {
                    let request = Request::builder()
                        .method(Method::GET)
                        .header("target", target.clone())
                        .body(())?;
                    match sender.send_request(request, false) {
                        Ok((response, send_to_server)) => {
                            let response = response.await?;
                            if response.status() != 200 {
                                bail!("server returned error: {}", response.status());
                            }
                            return Ok(H2StreamEndpoint {
                                send_to_server,
                                recv_from_server: response.into_body(),
                            });
                        }
                        Err(e) => {
                            warn!("Failed to send request over H2, connection might be dead: {e}");
                            *sender_opt = None;
                        }
                    }
                } else {
                    debug!("H2 sender not ready, reconnecting");
                    *sender_opt = None;
                }
            }

            // Drop the lock before reconnecting to avoid deadlocks and allow other tasks to see None
            drop(sender_opt);
            self.reconnect().await?;
        }
    }

    async fn reconnect(&self) -> anyhow::Result<()> {
        let mut sender_opt = self.h2_sender.lock().await;
        if sender_opt.is_some() {
            return Ok(()); // Already reconnected by another task
        }

        info!("Connecting to remote proxy server at {}", self.remote_addr);

        let mut backoff = Duration::from_millis(500);
        let max_backoff = Duration::from_secs(30);

        loop {
            match self.connect_and_auth().await {
                Ok(sender) => {
                    *sender_opt = Some(sender);
                    info!("Successfully connected and authenticated with remote proxy");
                    return Ok(());
                }
                Err(e) => {
                    error!("Failed to connect to proxy: {e}. Retrying in {backoff:?}");
                    sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                }
            }
        }
    }

    async fn connect_and_auth(&self) -> anyhow::Result<SendRequest<Bytes>> {
        let tls_config = build_client_tls_config(
            self.security_config.self_cert_bundle.certificate.clone(),
            self.security_config
                .self_cert_bundle
                .certificate_priv_key
                .clone_key(),
            self.security_config.ca_cert.clone(),
        )?;
        let tls_connector = TlsConnector::from(Arc::new(tls_config));

        match self.protocol {
            crate::cli::Protocol::Tcp => {
                let stream = TcpStream::connect(self.remote_addr).await?;
                let tls_stream = tls_connector
                    .connect(self.server_name.clone(), stream)
                    .await?;
                self.auth_and_handshake(tls_stream).await
            }
            crate::cli::Protocol::Kcp => {
                let kcp_config = self
                    .kcp_config
                    .clone()
                    .unwrap_or_else(KcpConfig::file_transfer);
                let stream = KcpStream::connect(self.remote_addr, kcp_config).await?;
                let tls_stream = tls_connector
                    .connect(self.server_name.clone(), stream)
                    .await?;
                self.auth_and_handshake(tls_stream).await
            }
        }
    }

    async fn auth_and_handshake<T: StreamConnection + 'static>(
        &self,
        mut stream: T,
    ) -> anyhow::Result<SendRequest<Bytes>> {
        let mut buf = [0u8; 32];
        buf[..16].copy_from_slice(&PROTOCOL_MAGIC);
        buf[16..32].copy_from_slice(&self.security_config.auth_secret);

        stream.write_all(&buf).await?;
        stream.read_exact(&mut buf[..17]).await?;

        if buf[..16] != PROTOCOL_MAGIC {
            bail!("invalid protocol magic from server");
        }
        if buf[16] != 0x1 {
            bail!("authentication failed");
        }

        let (h2_client, connection) = client::handshake(stream).await?;

        // Spawn a task to manage the H2 connection
        let h2_sender_for_cleanup = self.h2_sender.clone();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("H2 connection error: {e}");
            }
            let mut sender = h2_sender_for_cleanup.lock().await;
            *sender = None;
            debug!("H2 connection closed, cleared sender");
        });

        Ok(h2_client)
    }
}

pub struct H2StreamEndpoint {
    send_to_server: SendStream<Bytes>,
    recv_from_server: RecvStream,
}

impl DataEndpoint for H2StreamEndpoint {
    type ReadHalf = RecvStream;
    type WriteHalf = SendStream<Bytes>;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        (self.recv_from_server, self.send_to_server)
    }
}

pub struct Socks5Processor {
    tcp_listener: TcpListener,
    connection_manager: Arc<ClientConnectionManager>,
    proxy_manager: Arc<ProxyManager>,
}

impl Socks5Processor {
    pub async fn new(
        socks5_addr: String,
        connection_manager: Arc<ClientConnectionManager>,
    ) -> anyhow::Result<Self> {
        let tcp_listener = TcpListener::bind(socks5_addr).await?;
        Ok(Self {
            tcp_listener,
            connection_manager,
            proxy_manager: Arc::new(ProxyManager::default()),
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
            let connection_manager = self.connection_manager.clone();
            let proxy_manager = self.proxy_manager.clone();
            tokio::spawn(async move {
                let result =
                    Self::handle_socks_client(client, connection_manager, proxy_manager, &addr)
                        .await;
                if let Err(err) = result {
                    warn!("handle client {addr} failed: {err}");
                } else {
                    debug!("handle client {addr} done");
                }
            });
        }
    }

    #[instrument(skip(proxy_client_stream, connection_manager, proxy_manager))]
    async fn handle_socks_client(
        mut proxy_client_stream: TcpStream,
        connection_manager: Arc<ClientConnectionManager>,
        proxy_manager: Arc<ProxyManager>,
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

        let target_str = match target_host {
            VariableHostRepr::Ipv4(ip) => {
                std::net::Ipv4Addr::from(ip).to_string() + ":" + &target_port.to_string()
            }
            VariableHostRepr::DomainName(ref domain) => {
                domain.clone() + ":" + &target_port.to_string()
            }
        };

        let h2_stream = match connection_manager.get_h2_stream(target_str).await {
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

        debug!("proxy server to target success");
        proxy_write_buf.put_slice(&construct_connection_server_reply(Success));
        proxy_client_stream
            .write_all_buf(&mut proxy_write_buf)
            .await
            .ok();

        debug!("begin proxy application data");

        let client_endpoint = TcpStreamDataEndpoint {
            stream: proxy_client_stream,
            read_chunk_size: 8192,
        };

        proxy_manager.start_session(client_endpoint, h2_stream);

        Ok(())
    }
}

struct TcpStreamDataEndpoint {
    stream: TcpStream,
    read_chunk_size: usize,
}

impl DataEndpoint for TcpStreamDataEndpoint {
    type ReadHalf = StreamReader<tokio::net::tcp::OwnedReadHalf>;
    type WriteHalf = StreamWriter<tokio::net::tcp::OwnedWriteHalf>;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        let (rh, wh) = self.stream.into_split();
        (
            StreamReader {
                stream: rh,
                buf: BytesMut::with_capacity(self.read_chunk_size),
                buf_size: self.read_chunk_size,
            },
            StreamWriter { stream: wh },
        )
    }
}

struct StreamReader<T> {
    stream: T,
    buf: BytesMut,
    buf_size: usize,
}

impl<T: tokio::io::AsyncRead + Unpin + Send> DataReader for StreamReader<T> {
    async fn read_data(
        &mut self,
    ) -> Result<Option<Bytes>, crate::server::proxy_manager::DataEndpointError> {
        self.buf.reserve(self.buf_size);
        match self.stream.read_buf(&mut self.buf).await {
            Ok(0) => Ok(None),
            Ok(_) => Ok(Some(self.buf.split().freeze())),
            Err(e) => Err(crate::server::proxy_manager::DataEndpointError::IoError(e)),
        }
    }
}

struct StreamWriter<T> {
    stream: T,
}

impl<T: tokio::io::AsyncWrite + Unpin + Send> DataWriter for StreamWriter<T> {
    async fn write_data(
        &mut self,
        data: Bytes,
    ) -> Result<(), crate::server::proxy_manager::DataEndpointError> {
        self.stream.write_all(&data).await?;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), crate::server::proxy_manager::DataEndpointError> {
        self.stream.shutdown().await?;
        Ok(())
    }
}
