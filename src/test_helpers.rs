use crate::config::SecurityConfig;
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

const AUTH_SECRET: &str = "hello_world+123###@@@QwQ";

pub fn get_server_config() -> anyhow::Result<SecurityConfig> {
    SecurityConfig::new(
        "certs/server/server.crt",
        "certs/server/server.key",
        "certs/ca/ca.crt",
        AUTH_SECRET,
    )
}

pub fn get_client_config() -> anyhow::Result<SecurityConfig> {
    SecurityConfig::new(
        "certs/client/client.crt",
        "certs/client/client.key",
        "certs/ca/ca.crt",
        AUTH_SECRET,
    )
}
