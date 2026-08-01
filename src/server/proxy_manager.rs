use crate::data_endpoint::{BidirectionalCopier, DataEndpoint, TcpStreamDataEndpoint};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::sync::CancellationToken;

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

        copier.run_copy_task_with_cancel_token(proxy_id, token);
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
    use crate::data_endpoint::{DataEndpoint, StreamReader, StreamWriter};
    use bytes::BytesMut;
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

        // Both endpoints should have their write_halves cleanly shut down by the cancelled tasks
        let mut buf = vec![0; 10];

        let n_a = client_a.read(&mut buf).await.unwrap();
        assert_eq!(n_a, 0, "Client A should receive EOF on global shutdown");

        let n_b = client_b.read(&mut buf).await.unwrap();
        assert_eq!(n_b, 0, "Client B should receive EOF on global shutdown");
    }
}
