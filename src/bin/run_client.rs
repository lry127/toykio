use toykio::client::Socks5Processor;
use toykio::config::preconfigured_secrets::get_client_config;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let client = Socks5Processor::bind("127.0.0.1:1080", get_client_config()?).await?;
    client.run_socks5_loop().await;
    Ok(())
}
