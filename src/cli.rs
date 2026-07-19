use std::path::PathBuf;

use crate::config::SecurityConfig;
use clap::Args;

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

pub fn get_security_config_from_cli(args: &SecurityConfigArgs) -> anyhow::Result<SecurityConfig> {
    SecurityConfig::new(
        args.cert_path.as_path(),
        args.cert_key.as_path(),
        args.ca_cert.as_path(),
        &args.shared_secret,
    )
}
