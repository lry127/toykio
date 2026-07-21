use kcp_tokio::{KcpListener, KcpStream};
use std::collections::HashMap;
use std::io::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{StreamExt, StreamMap};
use tracing::{error, warn};

pub type ConnectionId = u64;

pub trait ReadStream: AsyncRead + Send + Unpin {}

impl<T> ReadStream for T where T: AsyncRead + Send + Unpin {}

pub trait WriteStream: AsyncWrite + Send + Unpin {}

impl<T> WriteStream for T where T: AsyncWrite + Send + Unpin {}

pub trait StreamConnection: ReadStream + WriteStream {}

impl<T> StreamConnection for T where T: ReadStream + WriteStream {}

#[allow(async_fn_in_trait)]
pub trait StreamAcceptor {
    type Stream: StreamConnection + Send + 'static;
    async fn accept_stream(&mut self) -> tokio::io::Result<(Self::Stream, SocketAddr)>;
}

pub struct TcpStreamAcceptor {
    tcp_listener: TcpListener,
}

impl StreamAcceptor for TcpStreamAcceptor {
    type Stream = TcpStream;

    async fn accept_stream(&mut self) -> tokio::io::Result<(Self::Stream, SocketAddr)> {
        let (stream, addr) = self.tcp_listener.accept().await?;
        Ok((stream, addr))
    }
}

pub struct KcpStreamAcceptor {
    kcp_listener: KcpListener,
}

impl StreamAcceptor for KcpStreamAcceptor {
    type Stream = KcpStream;
    async fn accept_stream(&mut self) -> tokio::io::Result<(Self::Stream, SocketAddr)> {
        self.kcp_listener
            .accept()
            .await
            .map_err(tokio::io::Error::other)
    }
}

pub trait StreamHandler {
    fn handle_stream<C2S, S2C>(
        &self,
        stream: impl StreamConnection + Send + 'static,
        to_server_sender: Sender<C2S>,
        from_server_receiver: Receiver<S2C>,
    ) -> impl Future<Output = ()> + Send + '_;
}

pub struct FairFlowController<T> {
    active_channels: StreamMap<ConnectionId, ReceiverStream<T>>,
}

impl<T> FairFlowController<T> {
    fn new() -> Self {
        FairFlowController {
            active_channels: StreamMap::new(),
        }
    }
}

impl<T> FairFlowController<T> {
    fn add_connection(&mut self, id: ConnectionId, receiver: Receiver<T>) {
        let stream = ReceiverStream::new(receiver);
        self.active_channels.insert(id, stream);
    }

    fn remove_connection(&mut self, id: ConnectionId) {
        self.active_channels.remove(&id);
    }

    async fn get_next_msg(&mut self) -> Option<(ConnectionId, T)> {
        self.active_channels.next().await
    }

    fn is_msg_possible(&self) -> bool {
        !self.active_channels.is_empty()
    }
}

struct ManagedStreamRecord<M> {
    id: ConnectionId,
    remote_addr: SocketAddr,
    to_connection_sender: Sender<M>, // keep a copy in manager, this might be needed by manager user
}

enum C2SMessage<T> {
    NewConnectionEstablished(ConnectionId, SocketAddr),
    ReadError(ConnectionId, Error),
    WriteError(ConnectionId, Error),
    OnApplicationMessage(ConnectionId, T),
}

enum S2CMessage<T> {
    CreateNewConnection(SocketAddr),
    SendApplicationMessage(ConnectionId, T),
    AbortConnection(ConnectionId),
}

pub struct ConnectionManager<A, H, C2S, S2C: Send + 'static + Sync>
where
    A: StreamAcceptor,
    H: StreamHandler + Send + Sync + 'static,
{
    acceptor: Option<A>,
    handler: Arc<H>,
    conn_id_counter: ConnectionId,
    conn_flow_controller: FairFlowController<C2SMessage<C2S>>,
    from_server_receiver: Receiver<S2CMessage<S2C>>,
    to_server_sender: Sender<C2SMessage<C2S>>,
    streams: HashMap<ConnectionId, ManagedStreamRecord<S2CMessage<S2C>>>,
}

#[derive(Debug, Error)]
pub enum EventError {
    #[error("failed to send c2s msg, broken channel")]
    SendC2SMsgFailed,
    #[error("failed to recv s2c msg, broken channel")]
    RecvS2CMsgFailed,
    #[error("failed to accept new socket")]
    AcceptNewSocketErr(#[from] tokio::io::Error),
    #[error("failed to process c2s msg")]
    C2SMsgProcessError,
    #[error("failed to process s2c msg")]
    S2CMsgProcessError,
}

impl<A, H, C2S: Send + 'static + Sync, S2C: Send + 'static + Sync> ConnectionManager<A, H, C2S, S2C>
where
    A: StreamAcceptor,
    H: StreamHandler + Send + Sync + 'static,
{
    // todo: wrap sender to make it easier to send to this manger from user struct
    fn new(
        acceptor: Option<A>,
        handler: H,
    ) -> (Self, Sender<S2CMessage<S2C>>, Receiver<C2SMessage<C2S>>) {
        let (sender_s2c, receiver_s2c) = channel::<S2CMessage<S2C>>(128);
        let (sender_c2s, receiver_c2s) = channel::<C2SMessage<C2S>>(128);

        let manager = Self {
            acceptor,
            handler: Arc::new(handler),
            conn_id_counter: 0,
            conn_flow_controller: FairFlowController::new(),
            streams: HashMap::new(),
            to_server_sender: sender_c2s,
            from_server_receiver: receiver_s2c,
        };
        (manager, sender_s2c, receiver_c2s)
    }

    pub async fn run_event_loop(&mut self) {
        loop {
            let err = match self.process_event_once().await {
                Ok(_) => continue,
                Err(err) => err,
            };

            match err {
                EventError::SendC2SMsgFailed => {
                    error!("fatal error: server dead");
                    self.clean().await;
                    break;
                }
                EventError::AcceptNewSocketErr(err) => {
                    warn!("failed to accept new socket: {err}");
                }
                other => {
                    warn!("other event error: {other}");
                }
            }
        }
    }

    async fn clean(&mut self) {}

    async fn process_event_once(&mut self) -> Result<(), EventError> {
        let accept_fut = async {
            match &mut self.acceptor {
                Some(acceptor) => Some(acceptor.accept_stream().await),
                None => std::future::pending().await,
            }
        };

        select! {
            msg = self.conn_flow_controller.get_next_msg(), if self.conn_flow_controller.is_msg_possible() => {
               if let Some((id, msg)) = msg {
                    self.process_c2s_from_connections(id, msg).await?;
               }
            },

            accept_res = accept_fut => {
                if let Some(accept_res) = accept_res {
                    match accept_res {
                        Ok((s, addr)) => {
                            self.handle_new_connection(s, addr).await;
                        },
                        Err(err) => {
                            return Err(EventError::AcceptNewSocketErr(err))
                        }
                    }
                }
            },

            s2c_msg = self.from_server_receiver.recv() => {
                let s2c_msg = match s2c_msg {
                    None => return Err(EventError::RecvS2CMsgFailed),
                    Some(msg) => msg
                };
                self.process_s2c_msg(s2c_msg).await?;
            }
        }
        Ok(())
    }

    async fn process_c2s_from_connections(
        &mut self,
        id: ConnectionId,
        msg: C2SMessage<C2S>,
    ) -> Result<(), EventError> {
        self.to_server_sender
            .send(msg)
            .await
            .map_err(|_| EventError::SendC2SMsgFailed)
    }

    async fn process_s2c_msg(&mut self, msg: S2CMessage<S2C>) -> Result<(), EventError> {
        Ok(())
    }

    async fn handle_new_connection(&mut self, s: A::Stream, addr: SocketAddr) {
        let (sender_c2s, receiver_c2s, receiver_s2c, record) = self.create_new_stream_record(addr);
        self.conn_flow_controller
            .add_connection(record.id, receiver_c2s);
        self.streams.insert(record.id, record);
        let clone = self.handler.clone();
        tokio::spawn(async move {
            let _ = clone.handle_stream(s, sender_c2s, receiver_s2c).await;
        });
    }

    fn create_new_stream_record(
        &mut self,
        remote_addr: SocketAddr,
    ) -> (
        Sender<C2SMessage<C2S>>,
        Receiver<C2SMessage<C2S>>,
        Receiver<S2CMessage<S2C>>,
        ManagedStreamRecord<S2CMessage<S2C>>,
    ) {
        let (sender_c2s, receiver_c2s) = channel::<C2SMessage<C2S>>(32);
        let (sender_s2c, receiver_s2c) = channel::<S2CMessage<S2C>>(32);

        let record = ManagedStreamRecord {
            id: self.conn_id_counter,
            remote_addr,
            to_connection_sender: sender_s2c,
        };
        self.conn_id_counter += 1;

        (sender_c2s, receiver_c2s, receiver_s2c, record)
    }
}
