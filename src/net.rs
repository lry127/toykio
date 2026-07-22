use crate::net::ToManagerMessage::GetRemoteAddr;
use kcp_tokio::{KcpListener, KcpStream};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::oneshot;
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

/// handle io for one connection stream
pub trait IoHandler {
    type IoToLogicMsgType;
    type LogicToIoMsgType;
    fn handle_stream(
        &self,
        stream: impl StreamConnection + Send + 'static,
        io_context: ClientConnectionContext<Self::IoToLogicMsgType, Self::LogicToIoMsgType>,
    ) -> impl Future<Output = ()> + Send + '_;
}

/// handle logic for many connections
pub trait LogicHandler {
    type IoToLogicMsgType;
    type LogicToIoMsgType;
    fn handle_logic(
        self,
        io_context: ServerConnectionContext<Self::IoToLogicMsgType, Self::LogicToIoMsgType>,
    ) -> impl Future<Output = ()> + Send;
}

struct StreamRecord {
    id: ConnectionId,
    remote_addr: SocketAddr,
}

type OneshotSender<T> = oneshot::Sender<T>;
type OneshotReceiver<T> = oneshot::Receiver<T>;

#[derive(Debug, Error)]
pub enum ManagerError {
    #[error("fatal application bug: communication channel dropped")]
    FatalChannelError,
    #[error("Can't find channel with id: {0}")]
    ChannelIdNotFound(ConnectionId),
    #[error("IO Error {0}")]
    IoError(#[from] tokio::io::Error),
}

type ManagerResult<T> = Result<T, ManagerError>;
enum ToManagerMessage {
    CloseConnection(ConnectionId, OneshotSender<ManagerResult<()>>),
    GetRemoteAddr(ConnectionId, OneshotSender<ManagerResult<SocketAddr>>),
    AcceptNewConnection(OneshotSender<ManagerResult<StreamRecord>>),
}

pub struct ClientConnectionContext<C2S, S2C> {
    to_server_sender: Sender<C2S>,
    from_server_receiver: Receiver<S2C>,
    connection_id: ConnectionId,
    to_manager_sender: Sender<ToManagerMessage>,
}

struct ServerManagedConnection<C2S, S2C> {
    connection_id: ConnectionId,
    to_client_sender: Sender<S2C>,
    from_client_receiver: Receiver<C2S>,
}
pub struct ServerConnectionContext<C2S, S2C> {
    all_connections: HashMap<ConnectionId, ServerManagedConnection<C2S, S2C>>,
    to_manager_sender: Sender<ToManagerMessage>,
}

async fn send_to_manager_and_wait_for_reply<T>(
    msg: ToManagerMessage,
    sender: &Sender<ToManagerMessage>,
    rx: oneshot::Receiver<ManagerResult<T>>,
) -> ManagerResult<T> {
    if sender.send(msg).await.is_err() {
        return Err(ManagerError::FatalChannelError);
    };
    rx.await.map_err(|_| ManagerError::FatalChannelError)?
}

// common methods for both contexts
async fn get_remote_addr(
    connection_id: ConnectionId,
    sender: &Sender<ToManagerMessage>,
) -> ManagerResult<SocketAddr> {
    let (tx, rx) = oneshot::channel();
    let get_remote_addr_req = GetRemoteAddr(connection_id, tx);
    send_to_manager_and_wait_for_reply(get_remote_addr_req, sender, rx).await
}

impl<C2S, S2C> ClientConnectionContext<C2S, S2C> {
    pub async fn get_remote_addr(&mut self) -> ManagerResult<SocketAddr> {
        get_remote_addr(self.connection_id, &self.to_manager_sender).await
    }
}

pub struct ConnectionManager<I, A>
where
    I: IoHandler + Send + Sync + 'static,
    A: StreamAcceptor,
{
    handler: Arc<I>,
    conn_id_counter: ConnectionId,
    streams: HashMap<ConnectionId, StreamRecord>,
    sender: Sender<ToManagerMessage>,
    receiver: Receiver<ToManagerMessage>,
    _stream_acceptor: PhantomData<A>,
}

impl<I, A> ConnectionManager<I, A>
where
    I: IoHandler + Send + Sync + 'static,
    A: StreamAcceptor,
{
    fn new<C2S, S2C>(io_handler: I) -> (Self, ServerConnectionContext<C2S, S2C>) {
        let (to_manager_tx, to_manager_rx) = channel::<ToManagerMessage>(128);
        let server_context = ServerConnectionContext {
            all_connections: HashMap::new(),
            to_manager_sender: to_manager_tx.clone(),
        };

        let manager = Self {
            handler: Arc::new(io_handler),
            conn_id_counter: 0,
            streams: HashMap::new(),
            sender: to_manager_tx,
            receiver: to_manager_rx,
            _stream_acceptor: PhantomData,
        };
        (manager, server_context)
    }

    pub async fn add_connections_from_acceptor(mut acceptor: A) {
        loop {
            let (s, addr) = match acceptor.accept_stream().await {
                Ok((s, addr)) => (s, addr),
                Err(err) => {
                    warn!("failed to accept connection");
                    continue;
                }
            };
        }
    }

    pub async fn run_event_loop(&mut self) {
        loop {
            match self.receiver.recv().await {
                None => {
                    self.clean().await;
                    break;
                }
                Some(msg) => {
                    self.process_manager_msg(&msg).await;
                }
            }
        }
    }

    async fn clean(&mut self) {}

    async fn process_manager_msg(&mut self, msg: &ToManagerMessage) {
        panic!("todo");
    }

    async fn handle_new_connection(&mut self, s: A::Stream, addr: SocketAddr) {
        panic!("todo")
    }

    fn create_new_stream_record(&mut self, remote_addr: SocketAddr) -> () {
        panic!("todo")
    }
}
