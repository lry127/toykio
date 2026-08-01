use enum_dispatch::enum_dispatch;
use kcp_tokio::{KcpConfig, KcpListener, KcpStream, UdpTransport};
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs, lookup_host};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tracing::warn;

pub trait ReadStream: AsyncRead + Send + Unpin {}

impl<T> ReadStream for T where T: AsyncRead + Send + Unpin {}

pub trait WriteStream: AsyncWrite + Send + Unpin {}

impl<T> WriteStream for T where T: AsyncWrite + Send + Unpin {}

pub trait StreamConnection: ReadStream + WriteStream {}

impl<T> StreamConnection for T where T: ReadStream + WriteStream {}

#[allow(async_fn_in_trait)]
pub trait StreamAcceptor {
    type Stream: StreamConnection + 'static + Send;
    fn accept_stream(
        &mut self,
    ) -> impl Future<Output = tokio::io::Result<(Self::Stream, SocketAddr)>> + Send + '_;
    #[cfg(test)]
    fn get_local_addr(&self) -> Option<SocketAddr>;
}

pub struct TcpStreamAcceptor {
    pub tcp_listener: TcpListener,
}

impl StreamAcceptor for TcpStreamAcceptor {
    type Stream = TcpStream;

    async fn accept_stream(&mut self) -> tokio::io::Result<(TcpStream, SocketAddr)> {
        let (stream, addr) = self.tcp_listener.accept().await?;
        Ok((stream, addr))
    }

    #[cfg(test)]
    fn get_local_addr(&self) -> Option<SocketAddr> {
        self.tcp_listener.local_addr().ok()
    }
}

impl TcpStreamAcceptor {
    pub async fn bind<T: ToSocketAddrs>(addr: T) -> tokio::io::Result<Self> {
        let tcp_listener = TcpListener::bind(addr).await?;
        Ok(Self { tcp_listener })
    }
}

pub struct KcpStreamAcceptor {
    pub kcp_listener: KcpListener,
}

impl StreamAcceptor for KcpStreamAcceptor {
    type Stream = KcpStream;
    async fn accept_stream(&mut self) -> tokio::io::Result<(KcpStream, SocketAddr)> {
        self.kcp_listener
            .accept()
            .await
            .map_err(tokio::io::Error::other)
    }

    #[cfg(test)]
    fn get_local_addr(&self) -> Option<SocketAddr> {
        Some(*self.kcp_listener.local_addr())
    }
}

impl KcpStreamAcceptor {
    pub async fn bind(addr: impl ToSocketAddrs, kcp_config: KcpConfig) -> tokio::io::Result<Self> {
        let udp_transport = UdpTransport::bind(addr).await?;
        let kcp_listener = KcpListener::with_transport(Arc::new(udp_transport), kcp_config)
            .await
            .map_err(tokio::io::Error::other)?;
        Ok(Self { kcp_listener })
    }
}

pub trait StreamConnector {
    type StreamType: StreamConnection;
    fn connect_to<T: ToSocketAddrs + Send + 'static>(
        &self,
        remote: T,
    ) -> impl Future<Output = tokio::io::Result<Self::StreamType>> + Send + '_;
}

pub struct TcpConnector;

impl StreamConnector for TcpConnector {
    type StreamType = TcpStream;

    async fn connect_to<T: ToSocketAddrs>(&self, remote: T) -> tokio::io::Result<Self::StreamType> {
        TcpStream::connect(remote).await
    }
}

pub struct KcpConnector {
    pub kcp_config: KcpConfig,
}

impl StreamConnector for KcpConnector {
    type StreamType = KcpStream;

    async fn connect_to<T: ToSocketAddrs>(&self, remote: T) -> std::io::Result<Self::StreamType> {
        let config = self.kcp_config.clone();
        let resolved: Vec<_> = lookup_host(remote).await?.collect();

        let remote_resolved = resolved
            .iter()
            .find(|s| s.is_ipv4())
            .copied()
            .or_else(|| resolved.first().copied())
            .ok_or_else(|| tokio::io::Error::other("can't resolve target host"))?;

        Ok(KcpStream::connect(remote_resolved, config)
            .await
            .map_err(tokio::io::Error::other)?)
    }
}

pub struct TlsStreamConnector<Inner: StreamConnector> {
    tls_connector: TlsConnector,
    server_name: ServerName<'static>,
    inner_connector: Inner,
}

impl<Inner: StreamConnector> TlsStreamConnector<Inner> {
    pub fn new(
        client_config: ClientConfig,
        server_name: ServerName<'static>,
        inner_connector: Inner,
    ) -> Self {
        let tls_connector = TlsConnector::from(Arc::new(client_config));
        Self {
            tls_connector,
            server_name,
            inner_connector,
        }
    }
}

impl<Inner: StreamConnector + Sync> StreamConnector for TlsStreamConnector<Inner> {
    type StreamType = TlsStream<Inner::StreamType>;

    async fn connect_to<T: ToSocketAddrs + Send + 'static>(
        &self,
        remote: T,
    ) -> std::io::Result<Self::StreamType> {
        let raw_connection = self.inner_connector.connect_to(remote).await?;
        let tls_stream = self
            .tls_connector
            .connect(self.server_name.clone(), raw_connection)
            .await?;
        Ok(tls_stream)
    }
}

pub trait StreamHandler {
    fn handle_stream<T: StreamConnection + 'static>(
        &self,
        stream: T,
        addr: SocketAddr,
    ) -> impl Future<Output = anyhow::Result<()>> + Send + '_;
}

pub struct ConnectionManager<A, H>
where
    A: StreamAcceptor + 'static + Send + Sync,
    H: StreamHandler + 'static + Send + Sync,
{
    stream_acceptor: A,
    handler: Arc<H>,
}

impl<A, H> ConnectionManager<A, H>
where
    A: StreamAcceptor + 'static + Send + Sync,
    H: StreamHandler + 'static + Send + Sync,
{
    pub fn new(stream_acceptor: A, handler: H) -> Self {
        Self {
            stream_acceptor,
            handler: Arc::new(handler),
        }
    }

    pub async fn run_accept_loop(mut self) {
        loop {
            let (s, addr) = match self.stream_acceptor.accept_stream().await {
                Ok(stream) => stream,
                Err(err) => {
                    warn!("failed to accept new stream: {err}");
                    continue;
                }
            };
            let handler = self.handler.clone();
            tokio::spawn(async move {
                handler.handle_stream(s, addr).await.ok();
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::net::{ConnectionManager, KcpStreamAcceptor, TcpStreamAcceptor};
    use crate::test_helpers::SimpleEchoHandler;
    use kcp_tokio::{KcpConfig, KcpStream};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn ensure_same_handler_work_for_both_tcp_and_kcp() -> anyhow::Result<()> {
        let addr = "127.0.0.1:0";
        let msg = b"hello async world";
        // tcp
        {
            let tcp_acceptor = TcpStreamAcceptor::bind(addr).await?;
            let local_addr = tcp_acceptor.tcp_listener.local_addr()?;

            let manager = ConnectionManager::new(tcp_acceptor, SimpleEchoHandler);
            tokio::spawn(async move {
                manager.run_accept_loop().await;
            });

            let mut client = TcpStream::connect(local_addr).await?;

            client.write_all(msg).await?;
            let mut buf = vec![0; msg.len()];
            client.read_exact(&mut buf).await?;

            assert_eq!(&buf, msg);
        }

        // kcp
        {
            let kcp_acceptor = KcpStreamAcceptor::bind(addr, KcpConfig::file_transfer()).await?;
            let local_addr = *kcp_acceptor.kcp_listener.local_addr();

            // Pass the original handler to the KCP manager
            let manager = ConnectionManager::new(kcp_acceptor, SimpleEchoHandler);
            tokio::spawn(async move {
                manager.run_accept_loop().await;
            });

            let mut client = KcpStream::connect(local_addr, KcpConfig::file_transfer()).await?;
            client.write_all(msg).await?;

            let mut buf = vec![0; msg.len()];
            client.read_exact(&mut buf).await?;
            assert_eq!(&buf, msg);
        }
        Ok(())
    }
}
