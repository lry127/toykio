use bytes::{Bytes, BytesMut};
use h2::{RecvStream, SendStream};
use std::future::poll_fn;
use std::sync::atomic::AtomicUsize;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
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

pub struct StreamReader<T> {
    pub stream: T,
    pub buf: BytesMut,
    pub buf_size: usize,
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

pub struct StreamWriter<T> {
    pub stream: T,
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

pub struct TcpStreamDataEndpoint {
    pub stream: TcpStream,
    pub read_buf_len: usize,
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

pub struct H2StreamEndpoint {
    pub send_to_client: SendStream<Bytes>,
    pub recv_from_client: RecvStream,
}

impl DataReader for RecvStream {
    async fn read_data(&mut self) -> Result<Option<Bytes>, DataEndpointError> {
        let res = match poll_fn(|cx| self.poll_data(cx)).await {
            None => return Ok(None),
            Some(res) => res,
        };
        match res {
            Ok(data) => {
                let res = self.flow_control().release_capacity(data.len());
                match res {
                    Ok(_) => Ok(Some(data)),
                    Err(err) => Err(DataEndpointError::IoError(std::io::Error::other(err))),
                }
            }
            Err(err) => Err(DataEndpointError::IoError(std::io::Error::other(err))),
        }
    }
}

impl DataWriter for SendStream<Bytes> {
    async fn write_data(&mut self, mut data: Bytes) -> Result<(), DataEndpointError> {
        while !data.is_empty() {
            // 1. Signal intent to send the exact remaining amount of data.
            self.reserve_capacity(data.len());

            // 2. Bridge the poll-based API into the async/await world.
            // poll_fn provides the Context (`cx`) needed by `poll_capacity`.
            let available_capacity = poll_fn(|cx| self.poll_capacity(cx))
                .await
                .ok_or_else(|| {
                    // poll_capacity returns Option::None if the stream is closed
                    // and will never receive capacity again.
                    DataEndpointError::from(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "HTTP/2 stream closed",
                    ))
                })?
                .map_err(DataEndpointError::from)?; // Handle the inner Result error

            // 3. Determine how much data we are allowed to send right now.
            let chunk_size = std::cmp::min(data.len(), available_capacity);

            // 4. Zero-copy split of the payload.
            let chunk = data.split_to(chunk_size);

            // 5. Immediately consume the assigned capacity by sending the chunk.
            self.send_data(chunk, false)
                .map_err(DataEndpointError::from)?;
        }

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), DataEndpointError> {
        self.send_data(Bytes::new(), true)
            .map_err(DataEndpointError::from)?;
        Ok(())
    }
}

impl DataEndpoint for H2StreamEndpoint {
    type ReadHalf = RecvStream;
    type WriteHalf = SendStream<Bytes>;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        (self.recv_from_client, self.send_to_client)
    }
}

pub struct BidirectionalCopier<A, B>
where
    A: DataEndpoint,
    B: DataEndpoint,
{
    pub endpoint_a: A,
    pub endpoint_b: B,
}

impl<A, B> BidirectionalCopier<A, B>
where
    A: DataEndpoint,
    B: DataEndpoint,
{
    pub fn spawn_copy_tasks(self, proxy_id: ProxyId, cancellation_token: CancellationToken) {
        let (rx_a, tx_a) = self.endpoint_a.split();
        let (rx_b, tx_b) = self.endpoint_b.split();

        // Clone the token for each direction
        let token_a = cancellation_token;
        let token_b = token_a.clone();

        let (a_id, b_id) = TaskIdentifier::create_pair(proxy_id);

        tokio::spawn(async move {
            Self::run_copy(a_id, rx_a, tx_b, token_a).await;
        });

        tokio::spawn(async move {
            Self::run_copy(b_id, rx_b, tx_a, token_b).await;
        });
    }

    #[instrument(skip(read_half, write_half, token))]
    async fn run_copy<T: DataReader, U: DataWriter>(
        proxy_task_identifier: TaskIdentifier,
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
#[allow(unused)] // the fields are accessed by tracing crate
struct TaskIdentifier {
    proxy_id: ProxyId,
    direction: StreamDirection,
}

impl TaskIdentifier {
    fn create_pair(proxy_id: ProxyId) -> (TaskIdentifier, TaskIdentifier) {
        let a = TaskIdentifier {
            proxy_id,
            direction: StreamDirection::A,
        };
        let b = TaskIdentifier {
            proxy_id,
            direction: StreamDirection::B,
        };
        (a, b)
    }
}
