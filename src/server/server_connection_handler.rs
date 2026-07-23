use crate::config::HashedAuthSecret;
use crate::net::{StreamConnection, StreamHandler};
use anyhow::{Context, bail};
use bytes::Bytes;
use h2::server::Connection;
use std::net::SocketAddr;
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use tracing::{debug, instrument};

struct H2ProxyConnectionsMultiplexer<T> {
    h2_connection: Connection<T, Bytes>,
}

impl<T: StreamConnection> H2ProxyConnectionsMultiplexer<T> {
    async fn h2_handshake(stream: T) -> anyhow::Result<Self> {
        Ok(Self {
            h2_connection: h2::server::handshake(stream).await?,
        })
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
