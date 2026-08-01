use crate::config::HashedAuthSecret;
use crate::data_endpoint::H2StreamEndpoint;
use crate::net::{StreamConnection, StreamHandler};
use crate::protocol::{parse_target_from_req, server_authenticate_client};
use crate::server::proxy_manager::ProxyManager;
use anyhow::{Context, bail};
use bytes::Bytes;
use h2::server::{Connection, SendResponse};
use h2::{Reason, RecvStream};
use http::{Method, Request, Response};
use log::warn;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, instrument};

struct H2ProxyConnectionsMultiplexer<T> {
    h2_connection: Connection<T, Bytes>,
    proxy_manager: Arc<ProxyManager>,
}

impl<T: StreamConnection + 'static> H2ProxyConnectionsMultiplexer<T> {
    async fn wrap_server_stream(stream: T) -> anyhow::Result<Self> {
        Ok(Self {
            h2_connection: h2::server::handshake(stream).await?,
            proxy_manager: Arc::new(ProxyManager::default()),
        })
    }

    async fn accept_connections(mut self) -> anyhow::Result<()> {
        loop {
            // a h2 conn is not designed to be shutdown unless the client no longer uses it
            // if there's an IO error or client closes the connection, abort everything and clean up
            let accept_res = match self.h2_connection.accept().await {
                None => {
                    return Ok(());
                }
                Some(accept_res) => accept_res,
            };
            let (req, send_response) = match accept_res {
                Ok(res) => res,
                Err(err) => {
                    self.clean_up();
                    return Err(err.into());
                }
            };

            let proxy_manager = self.proxy_manager.clone();
            tokio::spawn(async move {
                Self::handle_new_stream(proxy_manager, req, send_response)
                    .await
                    .ok();
            });
        }
    }

    fn clean_up(&mut self) {
        self.h2_connection.abrupt_shutdown(Reason::CANCEL);
        self.proxy_manager.shutdown_manager();
    }

    #[instrument(skip(resp, proxy_manager))]
    async fn handle_new_stream(
        proxy_manager: Arc<ProxyManager>,
        req: Request<RecvStream>,
        resp: SendResponse<Bytes>,
    ) -> anyhow::Result<()> {
        let stream_handler = H2StreamHandler { req, resp };
        match stream_handler.run_proxy(proxy_manager).await {
            Ok(_) => {
                debug!("stream_handler exited OK");
            }
            Err(err) => {
                warn!("proxy stream handler error: {err}");
                bail!(err);
            }
        };
        Ok(())
    }
}

struct H2StreamHandler {
    req: Request<RecvStream>,
    resp: SendResponse<Bytes>,
}

impl H2StreamHandler {
    async fn run_proxy(mut self, proxy_manager: Arc<ProxyManager>) -> anyhow::Result<()> {
        if self.req.method() != Method::GET {
            self.send_error_resp(405).await.ok();
            bail!("invalid req method");
        }

        let target = match parse_target_from_req(&self.req) {
            Ok(target) => target,
            Err(err) => {
                self.send_error_resp(400).await.ok();
                bail!(err);
            }
        };

        let target_endpoint = match proxy_manager.tcp_connect_to_target(target).await {
            Ok(ep) => ep,
            Err(err) => {
                self.send_error_resp(460).await.ok();
                bail!("failed to connect to remote: {err}");
            }
        };

        let send_to_client = self
            .resp
            .send_response(Response::builder().status(200).body(())?, false)
            .context("can't send response")?;

        let client_data_endpoint = H2StreamEndpoint {
            send_to_client,
            recv_from_client: self.req.into_body(),
        };
        proxy_manager.start_session(client_data_endpoint, target_endpoint);
        Ok(())
    }

    async fn send_error_resp(&mut self, status: u16) -> anyhow::Result<()> {
        self.resp
            .send_response(Response::builder().status(status).body(())?, true)?;
        Ok(())
    }
}

pub(super) struct ServerConnectionHandler {
    pub(super) auth_secret: Arc<HashedAuthSecret>,
}

impl ServerConnectionHandler {
    pub fn new(auth_secret: HashedAuthSecret) -> Self {
        Self {
            auth_secret: Arc::new(auth_secret),
        }
    }
}

impl StreamHandler for ServerConnectionHandler {
    #[instrument(skip(self, stream))]
    async fn handle_stream<T: StreamConnection + 'static>(
        &self,
        mut stream: T,
        _addr: SocketAddr,
    ) -> anyhow::Result<()> {
        debug!("client connected");
        // verify protocol preface & authenticate client
        timeout(
            Duration::from_secs(10),
            server_authenticate_client(&self.auth_secret, &mut stream),
        )
        .await
        .context("auth timeout")?
        .context("auth failed")?;

        let h2_multiplexer = H2ProxyConnectionsMultiplexer::wrap_server_stream(stream).await?;
        h2_multiplexer.accept_connections().await?;
        Ok(())
    }
}

#[cfg(test)]
mod h2_proxy_tests {
    use super::*;
    use crate::data_endpoint::DataReader;
    use bytes::Bytes;
    use h2::client;
    use http::{Method, Request};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
    use tokio::net::TcpListener;

    /// Helper function to establish an in-memory HTTP/2 connection
    async fn setup_h2_duplex() -> (
        client::SendRequest<Bytes>,
        h2::server::Connection<tokio::io::DuplexStream, Bytes>,
    ) {
        // Create an in-memory duplex stream (Client IO <-> Server IO)
        let (client_io, server_io) = duplex(8192);

        // Initialize the client side
        let (client, client_conn) = client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            client_conn.await.unwrap();
        });

        // Initialize the server side
        let server_conn = h2::server::handshake(server_io).await.unwrap();

        (client, server_conn)
    }

    #[tokio::test]
    async fn test_h2_proxy_bidirectional_data() {
        // 1. Setup mock TCP target
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = listener.local_addr().unwrap();

        let target_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 12];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hello target");
            stream.write_all(b"hello client").await.unwrap();
        });

        // 2. Setup H2 duplex
        let (mut client, mut server_conn) = setup_h2_duplex().await;

        // 3. Client sends request
        let req = Request::builder()
            .method(Method::GET)
            .uri("https://example.com/")
            .header("target", target_addr.to_string())
            .body(())
            .unwrap();

        let (response_future, mut client_send_stream) = client.send_request(req, false).unwrap();

        // 4. Server accepts stream and runs proxy
        let (server_req, server_resp) = server_conn.accept().await.unwrap().unwrap();
        tokio::spawn(async move { while let Some(Ok(_)) = server_conn.accept().await {} });

        let proxy_manager = Arc::new(ProxyManager::default());
        let handler = H2StreamHandler {
            req: server_req,
            resp: server_resp,
        };

        tokio::spawn(async move {
            handler.run_proxy(proxy_manager).await.unwrap();
        });

        // 5. Verify response and data transmission
        let response = response_future.await.unwrap();
        assert_eq!(response.status(), 200);
        let mut client_recv_stream = response.into_body();

        // Send data from client to target
        client_send_stream
            .send_data(Bytes::from("hello target"), true)
            .unwrap();

        // Read data from target to client
        let data = client_recv_stream.read_data().await.unwrap().unwrap();
        assert_eq!(data, Bytes::from("hello client"));

        target_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_h2_proxy_concurrent_requests() {
        // 1. Setup mock TCP target that handles multiple connections
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 5];
                    if stream.read_exact(&mut buf).await.is_ok() {
                        let msg = format!("echo: {}", String::from_utf8_lossy(&buf));
                        stream.write_all(msg.as_bytes()).await.unwrap();
                    }
                });
            }
        });

        // 2. Setup H2 duplex and Multiplexer
        let (client_io, server_io) = duplex(8192);
        let (client, client_conn) = client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            client_conn.await.unwrap();
        });

        let multiplexer = H2ProxyConnectionsMultiplexer {
            h2_connection: h2::server::handshake(server_io).await.unwrap(),
            proxy_manager: Arc::new(ProxyManager::default()),
        };
        tokio::spawn(async move { multiplexer.accept_connections().await });

        // 3. Send multiple concurrent requests
        let mut futures = Vec::new();
        for i in 0..5 {
            let mut client = client.clone();
            let target_addr_str = target_addr.to_string();
            futures.push(tokio::spawn(async move {
                let req = Request::builder()
                    .method(Method::GET)
                    .uri("https://example.com/")
                    .header("target", target_addr_str)
                    .body(())
                    .unwrap();

                let (response_future, mut send_stream) = client.send_request(req, false).unwrap();
                let payload = format!("req{:02}", i);
                send_stream
                    .send_data(Bytes::from(payload.clone()), true)
                    .unwrap();

                let response = response_future.await.unwrap();
                assert_eq!(response.status(), 200);
                let mut recv_stream = response.into_body();
                let data = recv_stream.read_data().await.unwrap().unwrap();
                assert_eq!(data, Bytes::from(format!("echo: {}", payload)));
            }));
        }

        for f in futures {
            f.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_h2_stream_handler_invalid_method() {
        let (mut client, mut server_conn) = setup_h2_duplex().await;

        // Send a POST request (Proxy expects GET)
        let req = Request::builder()
            .method(Method::POST)
            .uri("https://example.com/")
            .body(())
            .unwrap();

        let (response_future, _send_stream) = client.send_request(req, true).unwrap();

        // Accept the stream on the server side
        let (server_req, server_resp) = server_conn.accept().await.unwrap().unwrap();
        tokio::spawn(async move { while let Some(Ok(_)) = server_conn.accept().await {} });

        let proxy_manager = Arc::new(ProxyManager::default());
        let handler = H2StreamHandler {
            req: server_req,
            resp: server_resp,
        };

        // Run the handler
        let res = handler.run_proxy(proxy_manager).await;

        // Verify the proxy rejects the request internally
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "invalid req method");

        // Verify the client received the correct HTTP status code (405 Method Not Allowed)
        let response = response_future.await.unwrap();
        assert_eq!(response.status(), 405);
    }

    #[tokio::test]
    async fn test_h2_stream_handler_missing_target() {
        let (mut client, mut server_conn) = setup_h2_duplex().await;

        // Send a GET request but omit the "target" header
        let req = Request::builder()
            .method(Method::GET)
            .uri("https://example.com/")
            .body(())
            .unwrap();

        let (response_future, _send_stream) = client.send_request(req, true).unwrap();

        let (server_req, server_resp) = server_conn.accept().await.unwrap().unwrap();
        tokio::spawn(async move { while let Some(Ok(_)) = server_conn.accept().await {} });

        let proxy_manager = Arc::new(ProxyManager::default());
        let handler = H2StreamHandler {
            req: server_req,
            resp: server_resp,
        };

        let res = handler.run_proxy(proxy_manager).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "no target found");

        // Verify client gets 400 Bad Request
        let response = response_future.await.unwrap();
        assert_eq!(response.status(), 400);
    }

    #[tokio::test]
    async fn test_h2_stream_handler_invalid_target_format() {
        let (mut client, mut server_conn) = setup_h2_duplex().await;

        // Send a GET request with an incorrectly formatted target (missing colon)
        let req = Request::builder()
            .method(Method::GET)
            .uri("https://example.com/")
            .header("target", "invalid_target_without_port")
            .body(())
            .unwrap();

        let (response_future, _send_stream) = client.send_request(req, true).unwrap();

        let (server_req, server_resp) = server_conn.accept().await.unwrap().unwrap();
        tokio::spawn(async move { while let Some(Ok(_)) = server_conn.accept().await {} });

        let proxy_manager = Arc::new(ProxyManager::default());
        let handler = H2StreamHandler {
            req: server_req,
            resp: server_resp,
        };

        let res = handler.run_proxy(proxy_manager).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "invalid target");

        let response = response_future.await.unwrap();
        assert_eq!(response.status(), 400);
    }
}
