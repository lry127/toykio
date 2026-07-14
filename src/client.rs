use crate::client::socks5::{consume_client_hello, handle_target_addr_negotiation};
use crate::config::SecurityConfig;
use anyhow::bail;
use bytes::{Buf, BytesMut};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tracing::{debug, instrument, warn};

mod socks5;

pub struct Socks5Processor {
    tcp_listener: TcpListener,
    security_config: Arc<SecurityConfig>,
}

impl Socks5Processor {
    pub async fn bind<T: ToSocketAddrs>(
        addr: T,
        security_config: SecurityConfig,
    ) -> anyhow::Result<Self> {
        let tcp_listener = TcpListener::bind(addr).await?;
        Ok(Self {
            tcp_listener,
            security_config: Arc::new(security_config),
        })
    }

    pub async fn run_socks5_loop(self) {
        loop {
            let (client, addr) = match self.tcp_listener.accept().await {
                Ok((client, addr)) => (client, addr),
                Err(err) => {
                    warn!("failed accept: {err}");
                    continue;
                }
            };
            let security_config = self.security_config.clone();

            tokio::spawn(async move {
                let result = Self::handle_socks_client(client, &security_config, &addr).await;
                if let Err(err) = result {
                    warn!("handle client {addr} failed: {err}");
                } else {
                    debug!("handle client {addr} done");
                }
            });
        }
    }

    #[instrument(skip(tcp_stream, security_config))]
    async fn handle_socks_client(
        mut tcp_stream: TcpStream,
        security_config: &SecurityConfig,
        _socks_req_addr: &SocketAddr,
    ) -> anyhow::Result<()> {
        debug!("client connected");
        let mut read_buf = BytesMut::with_capacity(512);
        let mut write_buf = BytesMut::with_capacity(512);

        if let Err(err) = consume_client_hello(&mut tcp_stream, &mut read_buf, &mut write_buf).await
        {
            let _ = tcp_stream.shutdown().await;
            bail!(err.context("socks5 client hello failed"));
        }

        debug!("client hello successful");

        let (target_ip, target_port) =
            match handle_target_addr_negotiation(&mut tcp_stream, &mut read_buf, &mut write_buf)
                .await
            {
                Ok(res) => res,
                Err(err) => {
                    let _ = tcp_stream.shutdown().await;
                    bail!(err.context("socks5 connection establishment request failed"));
                }
            };

        // todo: impl protocol
        let _ = tcp_stream.read_buf(&mut read_buf).await;
        debug!("req: {:?}", read_buf.chunk());
        let _ = tcp_stream.read_to_end(&mut Vec::new()).await;

        Ok(())
    }
}
