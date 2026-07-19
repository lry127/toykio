use anyhow::{Context, bail};
use clap::Parser;
use std::net::SocketAddr;
use std::str::FromStr;
use toykio::cli::{SecurityConfigArgs, get_security_config_from_cli};
use toykio::client::Socks5Processor;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Parser)]
struct ClientCli {
    #[command(flatten)]
    security_config_args: SecurityConfigArgs,
    #[arg(long)]
    socks5_addr: String,
    #[arg(long)]
    remote_addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = ClientCli::parse();
    let client_security_config = match get_security_config_from_cli(&cli.security_config_args) {
        Ok(res) => res,
        Err(err) => {
            bail!("failed to read security config: {err}")
        }
    };

    if let Err(err) = SocketAddr::from_str(&cli.socks5_addr) {
        bail!("socks5 addr {} is invalid: {}", cli.socks5_addr, err);
    }

    if let Err(err) = SocketAddr::from_str(&cli.remote_addr) {
        bail!("remote proxy addr {} is invalid: {}", cli.remote_addr, err);
    }

    let (server_hostname, server_port) = &cli
        .remote_addr
        .split_once(':')
        .context("unable to split server addr")?;
    let server_port = server_port.parse().context("invalid server port")?;

    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    let client = Socks5Processor::new(
        cli.socks5_addr,
        client_security_config,
        server_hostname,
        server_port,
    )
    .await?;
    client.run_socks5_loop().await;
    println!("hello");
    Ok(())
}
