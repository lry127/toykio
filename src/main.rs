use toykio::server::ProxyServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = toykio::config::preconfigured_secrets::get_preconfigured_security_config();
    let mut proxy_server = ProxyServer::bind("127.0.0.1:1234", config).await?;
    let _ = proxy_server.server_loop().await;
    Ok(())
}
