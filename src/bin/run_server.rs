use anyhow::bail;
use clap::Parser;
use std::net::SocketAddr;
use std::str::FromStr;
use toykio::cli::{SecurityConfigArgs, get_security_config_from_cli};
use toykio::server::ProxyServer;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Parser)]
struct ServerCli {
    #[command(flatten)]
    security_config_args: SecurityConfigArgs,
    #[arg(long)]
    listen_addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = ServerCli::parse();
    let server_security_config = match get_security_config_from_cli(&cli.security_config_args) {
        Ok(res) => res,
        Err(err) => {
            bail!("failed to read security config: {err}")
        }
    };

    if let Err(err) = SocketAddr::from_str(&cli.listen_addr) {
        bail!("listening addr {} is invalid: {}", cli.listen_addr, err);
    }

    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
        )
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    tracing::debug!("started");

    let mut proxy_server = ProxyServer::bind(&cli.listen_addr, server_security_config).await?;
    let _ = proxy_server.server_loop().await;
    Ok(())
}
