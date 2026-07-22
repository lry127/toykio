use kcp_tokio::{KcpListener, KcpStream};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
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
    async fn accept_stream(&mut self) -> tokio::io::Result<(Self::Stream, SocketAddr)>;
}

pub struct TcpStreamAcceptor {
    tcp_listener: TcpListener,
}

impl StreamAcceptor for TcpStreamAcceptor {
    type Stream = TcpStream;

    async fn accept_stream(&mut self) -> tokio::io::Result<(TcpStream, SocketAddr)> {
        let (stream, addr) = self.tcp_listener.accept().await?;
        Ok((stream, addr))
    }
}

pub struct KcpStreamAcceptor {
    kcp_listener: KcpListener,
}

impl StreamAcceptor for KcpStreamAcceptor {
    type Stream = KcpStream;
    async fn accept_stream(&mut self) -> tokio::io::Result<(KcpStream, SocketAddr)> {
        self.kcp_listener
            .accept()
            .await
            .map_err(tokio::io::Error::other)
    }
}

pub trait StreamHandler<T: StreamConnection + 'static> {
    fn handle_stream(&self, stream: T, addr: SocketAddr) -> impl Future<Output = ()> + Send + '_;
}

pub struct ConnectionManager<A, H>
where
    A: StreamAcceptor,
    H: StreamHandler<A::Stream> + 'static + Send + Sync,
{
    stream_acceptor: A,
    handler: Arc<H>,
}

impl<A, H> ConnectionManager<A, H>
where
    A: StreamAcceptor,
    H: StreamHandler<A::Stream> + 'static + Send + Sync,
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
