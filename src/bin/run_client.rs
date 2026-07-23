use anyhow::{Context, bail};
use clap::Parser;
use rustls::pki_types::ServerName;
use std::sync::Arc;
use toykio::cli::{ClientConfigArgs, Protocol, get_security_config_from_cli};
use toykio::client::{ClientConnectionManager, Socks5Processor};
use toykio::net::KcpConfig;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = ClientConfigArgs::parse();
    let client_security_config = match get_security_config_from_cli(&cli.security_config_args) {
        Ok(res) => res,
        Err(err) => {
            bail!("failed to read security config: {err}")
        }
    };

    let (server_hostname, _server_port) = &cli
        .remote_addr
        .split_once(':')
        .context("unable to split server addr")?;

    let remote_addr = tokio::net::lookup_host(&cli.remote_addr)
        .await?
        .next()
        .context("can't resolve remote proxy addr")?;

    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let server_name = ServerName::try_from(*server_hostname)?.to_owned();

    let kcp_config = if matches!(cli.protocol, Protocol::Kcp) {
        let mut config = KcpConfig::file_transfer();
        if let Some(mtu) = cli.kcp_mtu {
            config.mtu = mtu;
        }
        Some(config)
    } else {
        None
    };

    let connection_manager = Arc::new(ClientConnectionManager::new(
        client_security_config,
        remote_addr,
        server_name,
        cli.protocol,
        kcp_config,
    ));

    let client = Socks5Processor::new(cli.socks5_addr, connection_manager).await?;

    client.run_socks5_loop().await;
    Ok(())
}
