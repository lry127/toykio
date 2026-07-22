use crate::net::{StreamConnection, StreamHandler};
use std::net::SocketAddr;

#[derive(Clone)]
pub struct SimpleEchoHandler;
impl StreamHandler for SimpleEchoHandler {
    async fn handle_stream<T: StreamConnection + 'static>(
        &self,
        stream: T,
        _addr: SocketAddr,
    ) -> anyhow::Result<()> {
        let (mut reader, mut writer) = tokio::io::split(stream);

        if let Err(e) = tokio::io::copy(&mut reader, &mut writer).await {
            eprintln!("Echo failed: {}", e);
        }
        Ok(())
    }
}
