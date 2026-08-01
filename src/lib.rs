use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt};

pub mod config;
pub(crate) mod net;

pub(crate) mod tls;

pub(crate) mod protocol;

pub(crate) mod data_endpoint;

pub mod server;

pub mod client;

pub mod cli;

#[cfg(test)]
pub(crate) mod test_helpers;

#[allow(async_fn_in_trait)]
pub trait ReadBufNExt {
    async fn read_buf_n(&mut self, buf: &mut BytesMut, n: usize) -> tokio::io::Result<()>;
}

impl<T: AsyncRead + Unpin> ReadBufNExt for T {
    async fn read_buf_n(&mut self, buf: &mut BytesMut, n: usize) -> tokio::io::Result<()> {
        while buf.len() < n {
            if self.read_buf(buf).await? == 0 {
                return Err(tokio::io::Error::new(
                    tokio::io::ErrorKind::UnexpectedEof,
                    format!("eof while trying to read {n} bytes"),
                ));
            }
        }
        Ok(())
    }
}
