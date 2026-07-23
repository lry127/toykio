use std::path::PathBuf;

use crate::config::SecurityConfig;
use clap::{Args, Parser, ValueEnum};

#[derive(Args, Debug, Clone)]
pub struct SecurityConfigArgs {
    #[arg(long)]
    cert_path: PathBuf,

    #[arg(long)]
    cert_key: PathBuf,

    #[arg(long)]
    ca_cert: PathBuf,

    #[arg(long)]
    shared_secret: String,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum Protocol {
    Tcp,
    Kcp,
}

#[derive(Parser, Debug, Clone)]
pub struct ServerConfigArgs {
    #[command(flatten)]
    pub security_config_args: SecurityConfigArgs,

    #[arg(long, default_value = "tcp")]
    pub protocol: Protocol,

    #[arg(long)]
    pub listen_addr: Option<String>,

    #[arg(long)]
    pub kcp_mtu: Option<u32>,
}

#[derive(Parser, Debug, Clone)]
pub struct ClientConfigArgs {
    #[command(flatten)]
    pub security_config_args: SecurityConfigArgs,

    #[arg(long, default_value = "tcp")]
    pub protocol: Protocol,

    #[arg(long)]
    pub socks5_addr: String,

    #[arg(long)]
    pub remote_addr: String,

    #[arg(long)]
    pub kcp_mtu: Option<u32>,
}

pub fn get_security_config_from_cli(args: &SecurityConfigArgs) -> anyhow::Result<SecurityConfig> {
    SecurityConfig::new(
        args.cert_path.as_path(),
        args.cert_key.as_path(),
        args.ca_cert.as_path(),
        &args.shared_secret,
    )
}
