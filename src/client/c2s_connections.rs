use crate::config::HashedAuthSecret;
use crate::net::StreamConnector;
use crate::protocol::{authenticate_to_server, build_proxy_steam_establish_req};
use anyhow::{Context, bail};
use bytes::Bytes;
use h2::SendStream;
use h2::client::{ResponseFuture, SendRequest};
use log::warn;
use std::collections::VecDeque;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::error;

pub struct H2ToServerMultiplexer<T: StreamConnector> {
    connector: T,
    target: SocketAddr,
    auth_secret: HashedAuthSecret,
    connections: VecDeque<(JoinHandle<()>, SendRequest<Bytes>)>,
}

impl<T: StreamConnector + Send + 'static> H2ToServerMultiplexer<T> {
    pub fn new(connector: T, target: SocketAddr, auth_secret: HashedAuthSecret) -> Self {
        Self {
            connector,
            target,
            auth_secret,
            connections: VecDeque::new(),
        }
    }

    pub fn run_multiplexer_loop(self) -> H2MultiplexerHandle {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        tokio::spawn(async move {
            let res = self.run_multiplexer_inner(rx).await;
            error!("multiplexer unexpected returned: {res:?}");
        });

        H2MultiplexerHandle { sender: tx }
    }

    async fn run_multiplexer_inner(
        mut self,
        mut receiver: Receiver<CreateNewProxySteamMsg>,
    ) -> anyhow::Result<()> {
        loop {
            match receiver.recv().await {
                None => {
                    bail!("receiver has no more msg to process");
                }
                Some(msg) => {
                    if let Err(err) = self.process_msg(msg).await {
                        warn!("can't handle msg: {err}");
                    }
                }
            }
        }
    }

    async fn process_msg(&mut self, msg: CreateNewProxySteamMsg) -> anyhow::Result<()> {
        let try_establish_result = async {
            let conn = self.pick_connection().await?;
            let req = build_proxy_steam_establish_req(&msg.target_addr, msg.target_port)?;
            let (resp_fut, send_stream) = conn.send_request(req, false)?;
            let client_stream = ClientH2ProxyStream {
                resp_fut,
                send_stream,
            };
            anyhow::Ok(client_stream)
        }
        .await;

        match try_establish_result {
            Ok(stream) => {
                if msg.sender.send(Ok(stream)).is_err() {
                    bail!("failed to send resp");
                } else {
                    Ok(())
                }
            }
            Err(err) => {
                let err_msg = err.to_string();
                let _ = msg.sender.send(Err(err));
                bail!(err_msg);
            }
        }
    }

    async fn pick_connection(&mut self) -> anyhow::Result<&mut SendRequest<Bytes>> {
        // currently, there's only one active connection only (idx == 0)
        // we might support multiple concurrent connection later (impl a conn pool)
        self.connections.retain(|(c, _)| !c.is_finished());

        if self.connections.is_empty() {
            self.establish_new_connection().await?;
        }

        self.connections.rotate_left(1); // rr algo, currently no op for only one element

        let (_, send_req) = self
            .connections
            .back_mut()
            .context("can't find active connection")?;
        Ok(send_req)
    }

    async fn establish_new_connection(&mut self) -> anyhow::Result<()> {
        let mut raw_conn = self.connector.connect_to(self.target).await?;
        authenticate_to_server(&self.auth_secret, &mut raw_conn).await?;
        let (send_req, h2_conn) = h2::client::handshake(raw_conn).await?;
        let job_handle = tokio::spawn(async move {
            let res = h2_conn.await;
            warn!("h2 connection poll ended: {res:?}");
        });
        self.connections.push_back((job_handle, send_req));
        Ok(())
    }
}

struct CreateNewProxySteamMsg {
    target_addr: String,
    target_port: u16,
    sender: oneshot::Sender<anyhow::Result<ClientH2ProxyStream>>,
}

impl CreateNewProxySteamMsg {
    pub fn new_request(
        target_addr: String,
        target_port: u16,
    ) -> (Self, oneshot::Receiver<anyhow::Result<ClientH2ProxyStream>>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                target_addr,
                target_port,
                sender: tx,
            },
            rx,
        )
    }
}

pub struct H2MultiplexerHandle {
    sender: Sender<CreateNewProxySteamMsg>,
}

impl H2MultiplexerHandle {
    pub async fn create_new_proxy_stream(
        &self,
        host: String,
        port: u16,
    ) -> anyhow::Result<ClientH2ProxyStream> {
        let (msg, receiver) = CreateNewProxySteamMsg::new_request(host, port);
        if self.sender.send(msg).await.is_err() {
            bail!("failed to send msg to multiplexer");
        }
        match receiver.await {
            Ok(msg) => msg,
            Err(err) => {
                bail!("failed to recv msg from multiplexer {err}")
            }
        }
    }
}

pub struct ClientH2ProxyStream {
    pub resp_fut: ResponseFuture,
    pub send_stream: SendStream<Bytes>,
}
