use crate::config::HashedAuthSecret;
use crate::net::{StreamConnection, StreamHandler};
use crate::server::proxy_manager::{
    DataEndpoint, DataEndpointError, DataReader, DataWriter, ProxyManager,
};
use anyhow::{Context, bail};
use bytes::Bytes;
use h2::server::{Connection, SendResponse};
use h2::{Reason, RecvStream, SendStream};
use http::{Method, Request, Response};
use log::warn;
use std::future::poll_fn;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
                    if err.is_io()
                        && err
                            .get_io()
                            .map_or(false, |io| io.kind() == std::io::ErrorKind::BrokenPipe)
                    {
                        return Ok(());
                    }
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

struct H2StreamEndpoint {
    send_to_client: SendStream<Bytes>,
    recv_from_client: RecvStream,
}

struct H2StreamReader {
    send_stream: SendStream<Bytes>,
}

impl DataReader for RecvStream {
    async fn read_data(&mut self) -> Result<Option<Bytes>, DataEndpointError> {
        let res = match poll_fn(|cx| self.poll_data(cx)).await {
            None => return Ok(None),
            Some(res) => res,
        };
        match res {
            Ok(data) => {
                let res = self.flow_control().release_capacity(data.len());
                match res {
                    Ok(_) => Ok(Some(data)),
                    Err(err) => Err(DataEndpointError::IoError(std::io::Error::other(err))),
                }
            }
            Err(err) => Err(DataEndpointError::IoError(std::io::Error::other(err))),
        }
    }
}

impl DataWriter for SendStream<Bytes> {
    async fn write_data(&mut self, mut data: Bytes) -> Result<(), DataEndpointError> {
        while !data.is_empty() {
            // 1. Signal intent to send the exact remaining amount of data.
            self.reserve_capacity(data.len());

            // 2. Bridge the poll-based API into the async/await world.
            // poll_fn provides the Context (`cx`) needed by `poll_capacity`.
            let available_capacity = poll_fn(|cx| self.poll_capacity(cx))
                .await
                .ok_or_else(|| {
                    // poll_capacity returns Option::None if the stream is closed
                    // and will never receive capacity again.
                    DataEndpointError::from(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "HTTP/2 stream closed",
                    ))
                })?
                .map_err(|e| DataEndpointError::from(e))?; // Handle the inner Result error

            // 3. Determine how much data we are allowed to send right now.
            let chunk_size = std::cmp::min(data.len(), available_capacity);

            // 4. Zero-copy split of the payload.
            let chunk = data.split_to(chunk_size);

            // 5. Immediately consume the assigned capacity by sending the chunk.
            self.send_data(chunk, false)
                .map_err(|e| DataEndpointError::from(e))?;
        }

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), DataEndpointError> {
        self.send_reset(Reason::CANCEL);
        Ok(())
    }
}

impl DataEndpoint for H2StreamEndpoint {
    type ReadHalf = RecvStream;
    type WriteHalf = SendStream<Bytes>;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        (self.recv_from_client, self.send_to_client)
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

        let target = match self.req.headers().get("target") {
            None => {
                self.send_error_resp(400).await.ok();
                bail!("no target found");
            }
            Some(target) => match target.to_str() {
                Ok(s) if s.contains(':') => s.to_owned(),
                _ => {
                    self.send_error_resp(400).await.ok();
                    bail!("invalid target, ':' separated target hostname expected");
                }
            },
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

struct ServerConnectionHandler {
    auth_secret: HashedAuthSecret,
}

const PROTOCOL_MAGIC: [u8; 16] = [
    0x9e, 0x25, 0xc3, 0x73, 0xe6, 0x70, 0x57, 0x8c, 0x66, 0x5e, 0x62, 0x63, 0xd1, 0xcb, 0x54, 0x16,
];

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
            self.authenticate_client(&mut stream),
        )
        .await
        .context("auth timeout")?
        .context("auth failed")?;

        let h2_multiplexer = H2ProxyConnectionsMultiplexer::wrap_server_stream(stream).await?;
        h2_multiplexer.accept_connections().await?;
        Ok(())
    }
}

impl ServerConnectionHandler {
    async fn authenticate_client<T: StreamConnection + 'static>(
        &self,
        stream: &mut T,
    ) -> anyhow::Result<()> {
        let mut buf = [0u8; 32];
        stream.read_exact(&mut buf).await?;
        if buf[..16] != PROTOCOL_MAGIC {
            bail!("invalid protocol magic");
        }
        let is_correct_auth_token = self.auth_secret.ct_eq(&buf[16..32]).unwrap_u8() == 1;
        buf[16] = if is_correct_auth_token { 0x1 } else { 0x0 };
        stream.write_all(&buf[..17]).await?;

        if !is_correct_auth_token {
            bail!("invalid auth token");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::server::server_connection_handler::{PROTOCOL_MAGIC, ServerConnectionHandler};
    use rand::{Rng, rng};
    use tokio_test::io::Builder;

    #[tokio::test]
    async fn test_invalid_magic() {
        let handler = ServerConnectionHandler {
            auth_secret: [0; 16],
        };

        let mut client_payload = Vec::with_capacity(32);
        client_payload.extend_from_slice(&PROTOCOL_MAGIC);
        client_payload[0] = !client_payload[0];
        client_payload.extend_from_slice(&[0; 16]);

        let mut mock_stream = Builder::new().read(&client_payload).build();

        let result = handler.authenticate_client(&mut mock_stream).await;
        assert!(
            result.is_err(),
            "Authentication should have failed due to invalid magic, but it succeeded"
        );
    }

    #[tokio::test]
    async fn test_correct_magic_with_correct_auth_token() {
        let mut rng_token = [0u8; 16];
        rng().fill_bytes(&mut rng_token);

        let handler = ServerConnectionHandler {
            auth_secret: rng_token,
        };

        let mut client_payload = Vec::with_capacity(32);
        client_payload.extend_from_slice(&PROTOCOL_MAGIC);
        client_payload.extend_from_slice(&rng_token);

        let mut expected_response = Vec::with_capacity(17);
        expected_response.extend_from_slice(&PROTOCOL_MAGIC);
        expected_response.push(0x1);

        let mut mock_stream = Builder::new()
            .read(&client_payload)
            .write(&expected_response)
            .build();

        let result = handler.authenticate_client(&mut mock_stream).await;
        assert!(
            result.is_ok(),
            "Authentication failed unexpectedly: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_correct_magic_with_incorrect_auth_token() {
        let mut rng_token = [0u8; 16];
        rng().fill_bytes(&mut rng_token);

        let handler = ServerConnectionHandler {
            auth_secret: rng_token,
        };

        let mut client_payload = Vec::with_capacity(32);
        client_payload.extend_from_slice(&PROTOCOL_MAGIC);
        client_payload.extend_from_slice(&rng_token);
        client_payload[31] = !client_payload[31];

        let mut expected_response = Vec::with_capacity(17);
        expected_response.extend_from_slice(&PROTOCOL_MAGIC);
        expected_response.push(0x0);

        let mut mock_stream = Builder::new()
            .read(&client_payload)
            .write(&expected_response)
            .build();

        let result = handler.authenticate_client(&mut mock_stream).await;
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod h2_proxy_tests {
    use super::*;
    use bytes::Bytes;
    use h2::client;
    use http::{Method, Request};
    use std::sync::Arc;
    use tokio::io::duplex;

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
        assert_eq!(
            res.unwrap_err().to_string(),
            "invalid target, ':' separated target hostname expected"
        );

        let response = response_future.await.unwrap();
        assert_eq!(response.status(), 400);
    }

    #[tokio::test]
    async fn test_h2_multiplexer_clean_shutdown() {
        let (client_io, server_io) = duplex(8192);

        // Only start the client side connection; don't make any requests
        let (client, client_conn) = client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            client_conn.await.ok();
        });

        // Initialize the Multiplexer
        let multiplexer = H2ProxyConnectionsMultiplexer {
            h2_connection: h2::server::handshake(server_io).await.unwrap(),
            proxy_manager: Arc::new(ProxyManager::default()),
        };

        // Run multiplexer on a separate task
        let multiplexer_task = tokio::spawn(async move { multiplexer.accept_connections().await });

        // Simulate the client disconnecting by dropping the client handle
        drop(client);

        // Wait for the multiplexer task to resolve with a timeout to avoid hanging
        let result = tokio::time::timeout(Duration::from_secs(1), multiplexer_task).await;

        match result {
            Ok(inner) => {
                let res = inner.unwrap();
                assert!(res.is_ok(), "Expected Ok(()), got {:?}", res);
            }
            Err(_) => panic!("Multiplexer did not shut down in time"),
        }
    }
}
