use bytes::{Bytes, BytesMut};
use std::future::Future;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};

#[derive(Error, Debug)]
pub enum DataEndpointError {
    #[error("io failed due to underlying transport error {0}")]
    IoError(#[from] tokio::io::Error),
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
}

impl<T: AsyncRead + Unpin + Send> DataReader for StreamReader<T> {
    async fn read_data(&mut self) -> Result<Option<Bytes>, DataEndpointError> {
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
    fn spawn_copy_tasks(self, cancellation_token: CancellationToken) {
        let (rx_a, tx_a) = self.endpoint_a.split();
        let (rx_b, tx_b) = self.endpoint_b.split();

        // Clone the token for each direction
        let token_a = cancellation_token;
        let token_b = token_a.clone();

        tokio::spawn(async move {
            Self::run_copy(rx_a, tx_b, token_a).await;
        });

        tokio::spawn(async move {
            Self::run_copy(rx_b, tx_a, token_b).await;
        });
    }

    #[instrument(skip(read_half, write_half, token))]
    async fn run_copy<T: DataReader, U: DataWriter>(
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

/// Manages a group of proxy connections
pub struct ProxyManager {
    root_cancellation_token: CancellationToken,
    read_chunk_size: usize,
}

impl ProxyManager {
    pub fn new(read_chunk_size: usize) -> Self {
        Self {
            root_cancellation_token: CancellationToken::new(),
            read_chunk_size,
        }
    }

    pub fn default() -> Self {
        Self::new(8192)
    }

    pub async fn tcp_connect_to_target<T: ToSocketAddrs>(
        &self,
        remote: T,
    ) -> tokio::io::Result<impl DataEndpoint> {
        let tcp_stream = TcpStream::connect(remote).await?;
        let stream_endpoint = StreamDataEndpoint {
            stream: tcp_stream,
            read_buf_len: self.read_chunk_size,
        };
        Ok(stream_endpoint)
    }

    pub fn start_session<A: DataEndpoint, B: DataEndpoint>(&self, endpoint_a: A, endpoint_b: B) {
        let token = self.root_cancellation_token.child_token();

        let copier = BidirectionalCopier {
            endpoint_a: endpoint_a,
            endpoint_b: endpoint_b,
        };

        copier.spawn_copy_tasks(token);
    }

    pub fn shutdown_manager(&self) {
        self.root_cancellation_token.cancel();
    }
}
