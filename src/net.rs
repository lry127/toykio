use kcp_tokio::{KcpConfig, KcpListener, KcpStream, UdpTransport};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
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

pub trait StreamHandler {
    fn handle_stream<T: StreamConnection + 'static>(
        &self,
        stream: T,
        addr: SocketAddr,
    ) -> impl Future<Output = ()> + Send + '_;
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
                handler.handle_stream(s, addr).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::net::{
        ConnectionManager, KcpStreamAcceptor, StreamConnection, StreamHandler, TcpStreamAcceptor,
    };
    use kcp_tokio::{KcpConfig, KcpStream};
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    struct SimpleEchoHandler;
    impl StreamHandler for SimpleEchoHandler {
        async fn handle_stream<T: StreamConnection + 'static>(&self, stream: T, _addr: SocketAddr) {
            let (mut reader, mut writer) = tokio::io::split(stream);

            if let Err(e) = tokio::io::copy(&mut reader, &mut writer).await {
                eprintln!("Echo failed: {}", e);
            }
        }
    }

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
