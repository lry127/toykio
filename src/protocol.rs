use anyhow::{Context, bail};
use http::{HeaderValue, Request};

const HEADER_TARGET_KEY: &str = "target";
pub(crate) fn build_proxy_steam_establish_req(
    host: &str,
    port: u16,
) -> anyhow::Result<Request<()>> {
    Request::builder()
        .method("get")
        .uri("/")
        .header(HEADER_TARGET_KEY, format!("{host}:{port}"))
        .body(())
        .context("can't build proxy steam req")
}

pub(crate) fn parse_target_from_req<T>(req: &Request<T>) -> anyhow::Result<String> {
    match req.headers().get(HEADER_TARGET_KEY) {
        None => {
            bail!("no target found")
        }
        Some(target) => {
            let target = target.to_str()?.to_string();
            let (_, port) = target.split_once(":").context("invalid target")?;
            port.parse::<u16>().context("invalid port")?;
            Ok(target)
        }
    }
}
