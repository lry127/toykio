use toykio::server::ProxyServer;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
        )
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    tracing::debug!("started");
    let config = toykio::config::preconfigured_secrets::get_preconfigured_security_config();
    let mut proxy_server = ProxyServer::bind("127.0.0.1:1234", config).await?;
    let _ = proxy_server.server_loop().await;
    Ok(())
}
