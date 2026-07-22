use bytes::Bytes;
use std::future::Future;
use thiserror::Error;
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

struct BidirectionalCopier<A, B>
where
    A: DataEndpoint,
    B: DataEndpoint,
{
    endpoint_alice: A,
    endpoint_bob: B,
}

impl<A, B> BidirectionalCopier<A, B>
where
    A: DataEndpoint,
    B: DataEndpoint,
{
    fn spawn_copy_tasks(self, cancellation_token: CancellationToken) {
        let (rx_a, tx_a) = self.endpoint_alice.split();
        let (rx_b, tx_b) = self.endpoint_bob.split();

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
pub struct ProxyManager {}

impl ProxyManager {
    async fn start_session<A: DataEndpoint, B: DataEndpoint>(endpoint_a: A, endpoint_b: B) {
        let token = CancellationToken::new();

        let copier = BidirectionalCopier {
            endpoint_alice: endpoint_a,
            endpoint_bob: endpoint_b,
        };

        copier.spawn_copy_tasks(token);
    }
}
