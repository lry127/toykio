use bytes::{Bytes, BytesMut};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};

#[derive(Error, Debug)]
pub enum DataEndpointError {
    #[error("io failed due to underlying transport error {0}")]
    IoError(#[from] std::io::Error),

    #[error("h2 protocol error {0}")]
    H2Error(#[from] h2::Error),
}

pub trait DataEndpoint {
    type ReadHalf: DataReader + Send + 'static;
    type WriteHalf: DataWriter + Send + 'static;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf);
}

pub trait DataReader {
    fn read_data(
        &mut self,
    ) -> impl Future<Output = Result<Option<Bytes>, DataEndpointError>> + Send + '_;
}

pub trait DataWriter {
    fn write_data(
        &mut self,
        data: Bytes,
    ) -> impl Future<Output = Result<(), DataEndpointError>> + Send + '_;
    fn shutdown(&mut self) -> impl Future<Output = Result<(), DataEndpointError>> + Send + '_;
}

struct StreamReader<T> {
    stream: T,
    buf: BytesMut,
    buf_size: usize,
}

impl<T: AsyncRead + Unpin + Send> DataReader for StreamReader<T> {
    async fn read_data(&mut self) -> Result<Option<Bytes>, DataEndpointError> {
        self.buf.reserve(self.buf_size);
        match self.stream.read_buf(&mut self.buf).await {
            Ok(n) => {
                if n == 0 {
                    Ok(None)
                } else {
                    Ok(Some(self.buf.split().freeze()))
                }
            }
            Err(err) => Err(DataEndpointError::IoError(err)),
        }
    }
}

struct StreamWriter<T> {
    stream: T,
}

impl<T: AsyncWrite + Unpin + Send> DataWriter for StreamWriter<T> {
    async fn write_data(&mut self, data: Bytes) -> Result<(), DataEndpointError> {
        self.stream.write_all(&data).await?;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), DataEndpointError> {
        self.stream.shutdown().await?;
        Ok(())
    }
}

struct TcpStreamDataEndpoint {
    stream: TcpStream,
    read_buf_len: usize,
}
impl DataEndpoint for TcpStreamDataEndpoint {
    type ReadHalf = StreamReader<OwnedReadHalf>;
    type WriteHalf = StreamWriter<OwnedWriteHalf>;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        let (read_half, write_half) = self.stream.into_split();
        let reader = StreamReader {
            stream: read_half,
            buf: BytesMut::with_capacity(self.read_buf_len),
            buf_size: self.read_buf_len,
        };
        let writer = StreamWriter { stream: write_half };
        (reader, writer)
    }
}

struct BidirectionalCopier<A, B>
where
    A: DataEndpoint,
    B: DataEndpoint,
{
    endpoint_a: A,
    endpoint_b: B,
}

impl<A, B> BidirectionalCopier<A, B>
where
    A: DataEndpoint,
    B: DataEndpoint,
{
    fn spawn_copy_tasks(self, proxy_id: ProxyId, cancellation_token: CancellationToken) {
        let (rx_a, tx_a) = self.endpoint_a.split();
        let (rx_b, tx_b) = self.endpoint_b.split();

        // Clone the token for each direction
        let token_a = cancellation_token;
        let token_b = token_a.clone();

        let a_id = ProxyTaskIdentifier {
            proxy_id,
            direction: StreamDirection::A,
        };
        let b_id = ProxyTaskIdentifier {
            proxy_id,
            direction: StreamDirection::B,
        };

        tokio::spawn(async move {
            Self::run_copy(a_id, rx_a, tx_b, token_a).await;
        });

        tokio::spawn(async move {
            Self::run_copy(b_id, rx_b, tx_a, token_b).await;
        });
    }

    #[instrument(skip(read_half, write_half, token))]
    async fn run_copy<T: DataReader, U: DataWriter>(
        proxy_task_identifier: ProxyTaskIdentifier,
        mut read_half: T,
        mut write_half: U,
        token: CancellationToken,
    ) {
        loop {
            let data_res = tokio::select! {
                res = read_half.read_data() => res,
                _ = token.cancelled() => {
                    debug!("task cancelled by sibling");
                    write_half.shutdown().await.ok();
                    return;
                }
            };

            let data = match data_res {
                Ok(Some(data)) => data,
                Ok(None) => {
                    debug!("read half done (clean eof)");
                    write_half.shutdown().await.ok();
                    return;
                }
                Err(err) => {
                    warn!("read half error: {err}");
                    write_half.shutdown().await.ok();
                    token.cancel();
                    return;
                }
            };

            if let Err(err) = write_half.write_data(data).await {
                warn!("can't send to remote {err}");
                write_half.shutdown().await.ok();
                token.cancel();
                return;
            };
        }
    }
}

type ProxyId = usize;
#[derive(Debug)]
enum StreamDirection {
    A,
    B,
}
#[derive(Debug)]
#[allow(unused)] // it's used implicitly in tracing crate
struct ProxyTaskIdentifier {
    proxy_id: ProxyId,
    direction: StreamDirection,
}

/// Manages a group of proxy connections
pub struct ProxyManager {
    root_cancellation_token: CancellationToken,
    read_chunk_size: usize,
    proxy_id_counter: AtomicUsize,
}

impl ProxyManager {
    pub fn new(read_chunk_size: usize) -> Self {
        Self {
            root_cancellation_token: CancellationToken::new(),
            read_chunk_size,
            proxy_id_counter: AtomicUsize::new(0),
        }
    }

    pub async fn tcp_connect_to_target<T: ToSocketAddrs>(
        &self,
        remote: T,
    ) -> tokio::io::Result<impl DataEndpoint> {
        let tcp_stream = TcpStream::connect(remote).await?;
        let stream_endpoint = TcpStreamDataEndpoint {
            stream: tcp_stream,
            read_buf_len: self.read_chunk_size,
        };
        Ok(stream_endpoint)
    }

    pub fn start_session<A: DataEndpoint, B: DataEndpoint>(&self, endpoint_a: A, endpoint_b: B) {
        let proxy_id = self.proxy_id_counter.fetch_add(1, Ordering::Relaxed);

        let token = self.root_cancellation_token.child_token();

        let copier = BidirectionalCopier {
            endpoint_a,
            endpoint_b,
        };

        copier.spawn_copy_tasks(proxy_id, token);
    }

    pub fn shutdown_manager(&self) {
        self.root_cancellation_token.cancel();
    }
}

impl Default for ProxyManager {
    fn default() -> Self {
        Self::new(8192)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{
        AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf, ReadHalf, WriteHalf,
    };
    use tokio_test::io::Builder;

    struct StreamDataEndpoint<T> {
        stream: T,
        read_buf_len: usize,
    }

    impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> DataEndpoint for StreamDataEndpoint<T> {
        type ReadHalf = StreamReader<ReadHalf<T>>;
        type WriteHalf = StreamWriter<WriteHalf<T>>;

        fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
            let (read_half, write_half) = tokio::io::split(self.stream);
            let reader = StreamReader {
                stream: read_half,
                buf: BytesMut::with_capacity(self.read_buf_len),
                buf_size: self.read_buf_len,
            };
            let writer = StreamWriter { stream: write_half };
            (reader, writer)
        }
    }

    /// Helper to quickly wrap any stream into a StreamDataEndpoint
    fn make_endpoint<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
        stream: T,
    ) -> StreamDataEndpoint<T> {
        StreamDataEndpoint {
            stream,
            read_buf_len: 1024,
        }
    }

    /// 1) TEST: Normal EOF cleanly shuts down the sibling stream
    #[tokio::test]
    async fn test_normal_eof() {
        let manager = ProxyManager::new(1024);

        // tokio-test Builder scripts a remote endpoint that yields "ping", then immediately EOFs
        let mock_remote = Builder::new().read(b"ping").build();
        let (mut local_client, proxy_local) = tokio::io::duplex(1024);

        manager.start_session(make_endpoint(proxy_local), make_endpoint(mock_remote));

        let mut buf = vec![0; 10];

        // 1. Client successfully reads the "ping" forwarded by the proxy
        let n = local_client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ping");

        // 2. Mock EOFs -> Proxy reads EOF -> Proxy shuts down local_client -> Client gets EOF
        let n = local_client.read(&mut buf).await.unwrap();
        assert_eq!(n, 0, "Expected clean EOF after data was exhausted");
    }

    /// 2) TEST: Unexpected IO Error on Read
    #[tokio::test]
    async fn test_unexpected_io_error_on_read() {
        let manager = ProxyManager::new(1024);

        // tokio-test Builder scripts a stream that immediately throws a ConnectionReset on read
        let error_remote = Builder::new()
            .read_error(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "mock read error",
            ))
            .build();

        let (mut local_client, proxy_local) = tokio::io::duplex(1024);

        manager.start_session(make_endpoint(proxy_local), make_endpoint(error_remote));

        // Proxy catches the mock read error, logs it, and shuts down the local sibling stream.
        let mut buf = vec![0; 10];
        let n = local_client.read(&mut buf).await.unwrap();
        assert_eq!(n, 0, "Client should receive EOF due to sibling read error");
    }

    // --- Write Error Workaround ---
    // tokio-test enforces a strict, single-threaded order of read and write operations.
    // In a full-duplex proxy, the background read loop will continuously poll `read`.
    // If we script a `Builder::new().write_error(...)`, the read loop will hit the mock first,
    // and tokio-test will panic with "unexpected read".
    // To test write errors safely in concurrent streams, we use a targeted mock that blocks reads forever.
    struct WriteErrorStream;
    impl AsyncRead for WriteErrorStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending // Block reads forever so it doesn't trigger an EOF shutdown
        }
    }
    impl AsyncWrite for WriteErrorStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "mock write error",
            )))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// 3) TEST: Unexpected IO Error on Write
    #[tokio::test]
    async fn test_unexpected_io_error_on_write() {
        let manager = ProxyManager::new(1024);
        let (mut local_client, proxy_local) = tokio::io::duplex(1024);

        manager.start_session(make_endpoint(proxy_local), make_endpoint(WriteErrorStream));

        // Send data to force the proxy to attempt a write to the WriteErrorStream
        local_client
            .write_all(b"trigger write error")
            .await
            .unwrap();

        // The proxy catches the BrokenPipe on write, cancels the token, and shuts down the local stream
        let mut buf = vec![0; 10];
        let n = local_client.read(&mut buf).await.unwrap();
        assert_eq!(n, 0, "Client should receive EOF due to sibling write error");
    }

    /// 4) TEST: Global Shutdown / Cancellation
    #[tokio::test]
    async fn test_global_shutdown() {
        let manager = ProxyManager::new(1024);
        let (mut client_a, proxy_a) = tokio::io::duplex(1024);
        let (mut client_b, proxy_b) = tokio::io::duplex(1024);

        manager.start_session(make_endpoint(proxy_a), make_endpoint(proxy_b));

        // Yield to the runtime to ensure the spawned copy tasks have started
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Trigger the global cancellation token, cascading to all active sessions
        manager.shutdown_manager();

        // Both endpoints should have their write_halves cleanly shut down by the canceled tasks
        let mut buf = vec![0; 10];

        let n_a = client_a.read(&mut buf).await.unwrap();
        assert_eq!(n_a, 0, "Client A should receive EOF on global shutdown");

        let n_b = client_b.read(&mut buf).await.unwrap();
        assert_eq!(n_b, 0, "Client B should receive EOF on global shutdown");
    }
}
