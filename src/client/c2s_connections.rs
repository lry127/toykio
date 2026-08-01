use crate::net::{StreamConnection, StreamConnector};
use anyhow::{Context, bail};
use bytes::Bytes;
use h2::client::{Connection, SendRequest};
use http::Request;
use log::warn;
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::net::ToSocketAddrs;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::error;

pub struct H2ToServerMultiplexer<T: StreamConnector, U> {
    connector: T,
    target: U,
    connections: VecDeque<(JoinHandle<()>, SendRequest<Bytes>)>,
}

impl<T: StreamConnector + Send + 'static, U: ToSocketAddrs + Send + 'static + Sync + Clone>
    H2ToServerMultiplexer<T, U>
{
    pub async fn new(connector: T, target: U) -> Self {
        Self {
            connector,
            target,
            connections: VecDeque::new(),
        }
    }

    pub async fn run_multiplexer_loop(self) -> ToServerConnectionHandle {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        tokio::spawn(async move {
            let res = self.run_multiplexer_inner(rx).await;
            error!("multiplexer unexpected returned: {res:?}");
        });

        ToServerConnectionHandle { sender: tx }
    }

    async fn run_multiplexer_inner(
        mut self,
        mut receiver: Receiver<ToConnectionMsg>,
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

    async fn process_msg(&mut self, msg: ToConnectionMsg) -> anyhow::Result<()> {
        let conn = self.pick_connection().await?;
        let req = Request::builder().method("get").uri("/hello").body(())?;

        let (resp_fut, mut send_stream) = conn.send_request(req, false)?;
        send_stream.send_data(Bytes::new(), true)?;
        Ok(())
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
        let raw_conn = self.connector.connect_to(self.target.clone()).await?;
        let (send_req, h2_conn) = h2::client::handshake(raw_conn).await?;
        let job_handle = tokio::spawn(async move {
            let res = h2_conn.await;
            warn!("h2 connection poll ended: {res:?}");
        });
        self.connections.push_back((job_handle, send_req));
        Ok(())
    }
}

struct H2ToServerConnection<S: StreamConnection> {
    conn: Connection<S>,
}

impl<S: StreamConnection + 'static> H2ToServerConnection<S> {
    async fn handshake(raw_conn: S) -> anyhow::Result<(Self, SendRequest<Bytes>)> {
        let (send_req, h2_conn) = h2::client::handshake(raw_conn).await?;
        let wrapped_conn = Self { conn: h2_conn };
        Ok((wrapped_conn, send_req))
    }
    fn poll_h2_connection(self) {}
}

struct ToConnectionMsg {
    target_addr: String,
    target_port: u16,
}

pub struct ToServerConnectionHandle {
    sender: Sender<ToConnectionMsg>,
}
