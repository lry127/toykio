use anyhow::bail;
use clap::Parser;
use std::net::SocketAddr;
use std::str::FromStr;
use toykio::cli::{Protocol, ServerConfigArgs, get_security_config_from_cli};
use toykio::net::KcpConfig;
use toykio::server::ProxyServer;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = ServerConfigArgs::parse();
    let server_security_config = match get_security_config_from_cli(&cli.security_config_args) {
        Ok(res) => res,
        Err(err) => {
            bail!("failed to read security config: {err}")
        }
    };

    let listen_addr = cli.listen_addr.as_deref().unwrap_or("0.0.0.0:5928");
    if let Err(err) = SocketAddr::from_str(listen_addr) {
        bail!("listening addr {} is invalid: {}", listen_addr, err);
    }

    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
        )
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    tracing::debug!("started");

    match cli.protocol {
        Protocol::Tcp => {
            ProxyServer::run_tcp(listen_addr, server_security_config).await?;
        }
        Protocol::Kcp => {
            let mut kcp_config = KcpConfig::file_transfer();
            if let Some(mtu) = cli.kcp_mtu {
                kcp_config.mtu = mtu;
            }
            ProxyServer::run_kcp(listen_addr, server_security_config, kcp_config).await?;
        }
    }
    Ok(())
}
