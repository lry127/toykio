use anyhow::bail;
use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt};

pub mod config;
pub mod protocol;

pub(crate) mod tls;

pub mod server;

pub mod client;

#[allow(async_fn_in_trait)]
pub trait ReadBufNExt {
    async fn read_buf_n(&mut self, buf: &mut BytesMut, n: usize) -> anyhow::Result<()>;
}

impl<T: AsyncRead + Unpin> ReadBufNExt for T {
    async fn read_buf_n(&mut self, buf: &mut BytesMut, n: usize) -> anyhow::Result<()> {
        while buf.len() < n {
            if self.read_buf(buf).await? == 0 {
                bail!("eof while trying to read {n} bytes");
            }
        }
        Ok(())
    }
}
