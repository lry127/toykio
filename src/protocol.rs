use crate::config::HashedAuthSecret;
use crate::net::StreamConnection;
use anyhow::{Context, bail};
use http::Request;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const HEADER_TARGET_KEY: &str = "target";
pub(crate) fn build_proxy_steam_establish_req(
    host: &str,
    port: u16,
) -> anyhow::Result<Request<()>> {
    Request::builder()
        .method("GET")
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

pub const PROTOCOL_MAGIC: [u8; 16] = [
    0x9e, 0x25, 0xc3, 0x73, 0xe6, 0x70, 0x57, 0x8c, 0x66, 0x5e, 0x62, 0x63, 0xd1, 0xcb, 0x54, 0x16,
];

pub(crate) async fn authenticate_to_server<T: StreamConnection>(
    auth_secret: &HashedAuthSecret,
    stream: &mut T,
) -> anyhow::Result<()> {
    stream.write_all(&PROTOCOL_MAGIC).await?;
    stream.write_all(auth_secret).await?;
    let mut read_buf = [0u8; 17];
    stream.read_exact(&mut read_buf).await?;
    if read_buf[..16] != PROTOCOL_MAGIC {
        bail!("invalid protocol magic");
    }
    if read_buf[16] == 0x0 {
        bail!("invalid auth secret");
    }
    Ok(())
}

pub(crate) async fn server_authenticate_client<T: StreamConnection>(
    auth_secret: &HashedAuthSecret,
    stream: &mut T,
) -> anyhow::Result<()> {
    let mut buf = [0u8; 32];
    stream.read_exact(&mut buf).await?;
    if buf[..16] != PROTOCOL_MAGIC {
        bail!("invalid protocol magic");
    }
    let is_correct_auth_token = auth_secret.ct_eq(&buf[16..32]).unwrap_u8() == 1;
    buf[16] = if is_correct_auth_token { 0x1 } else { 0x0 };
    stream.write_all(&buf[..17]).await?;

    if !is_correct_auth_token {
        bail!("invalid auth token");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*; // Assuming the functions are in the parent module
    use crate::protocol::PROTOCOL_MAGIC;
    use rand::{Rng, rng};
    use tokio_test::io::Builder;

    // -------------------------------------------------------------------------
    // Server-Side Authentication Tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_server_invalid_magic() {
        let auth_secret = [0; 16]; // Adjust if HashedAuthSecret needs specific instantiation

        let mut client_payload = Vec::with_capacity(32);
        client_payload.extend_from_slice(&PROTOCOL_MAGIC);
        // Corrupt the magic
        client_payload[0] = !client_payload[0];
        client_payload.extend_from_slice(&[0; 16]);

        let mut mock_stream = Builder::new().read(&client_payload).build();

        let result = server_authenticate_client(&auth_secret, &mut mock_stream).await;
        assert!(
            result.is_err(),
            "Authentication should have failed due to invalid magic"
        );
    }

    #[tokio::test]
    async fn test_server_correct_magic_with_correct_auth_token() {
        let mut rng_token = [0u8; 16];
        rng().fill_bytes(&mut rng_token);

        let mut client_payload = Vec::with_capacity(32);
        client_payload.extend_from_slice(&PROTOCOL_MAGIC);
        client_payload.extend_from_slice(&rng_token);

        let mut expected_response = Vec::with_capacity(17);
        expected_response.extend_from_slice(&PROTOCOL_MAGIC);
        expected_response.push(0x1); // Success flag

        let mut mock_stream = Builder::new()
            .read(&client_payload)
            .write(&expected_response)
            .build();

        let result = server_authenticate_client(&rng_token, &mut mock_stream).await;
        assert!(
            result.is_ok(),
            "Server authentication failed unexpectedly: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_server_correct_magic_with_incorrect_auth_token() {
        let mut rng_token = [0u8; 16];
        rng().fill_bytes(&mut rng_token);

        let mut client_payload = Vec::with_capacity(32);
        client_payload.extend_from_slice(&PROTOCOL_MAGIC);
        client_payload.extend_from_slice(&rng_token);
        // Corrupt the auth token in the payload
        client_payload[31] = !client_payload[31];

        let mut expected_response = Vec::with_capacity(17);
        expected_response.extend_from_slice(&PROTOCOL_MAGIC);
        expected_response.push(0x0); // Failure flag

        let mut mock_stream = Builder::new()
            .read(&client_payload)
            .write(&expected_response)
            .build();

        let result = server_authenticate_client(&rng_token, &mut mock_stream).await;
        assert!(
            result.is_err(),
            "Server authentication should have failed due to invalid token"
        );
    }

    // -------------------------------------------------------------------------
    // Client-Side Authentication Tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_client_successful_authentication() {
        let mut rng_token = [0u8; 16];
        rng().fill_bytes(&mut rng_token);

        let mut expected_write = Vec::with_capacity(32);
        expected_write.extend_from_slice(&PROTOCOL_MAGIC);
        expected_write.extend_from_slice(&rng_token);

        let mut server_response = Vec::with_capacity(17);
        server_response.extend_from_slice(&PROTOCOL_MAGIC);
        server_response.push(0x1); // Success flag

        let mut mock_stream = Builder::new()
            .write(&expected_write)
            .read(&server_response)
            .build();

        let result = authenticate_to_server(&rng_token, &mut mock_stream).await;
        assert!(
            result.is_ok(),
            "Client failed to authenticate with server: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_client_receives_invalid_magic() {
        let mut rng_token = [0u8; 16];
        rng().fill_bytes(&mut rng_token);

        let mut expected_write = Vec::with_capacity(32);
        expected_write.extend_from_slice(&PROTOCOL_MAGIC);
        expected_write.extend_from_slice(&rng_token);

        let mut server_response = Vec::with_capacity(17);
        server_response.extend_from_slice(&PROTOCOL_MAGIC);
        server_response[0] = !server_response[0]; // Corrupt the magic
        server_response.push(0x1);

        let mut mock_stream = Builder::new()
            .write(&expected_write)
            .read(&server_response)
            .build();

        let result = authenticate_to_server(&rng_token, &mut mock_stream).await;
        assert!(
            result.is_err(),
            "Client should have rejected server's invalid magic"
        );
    }

    #[tokio::test]
    async fn test_client_receives_auth_failure() {
        let mut rng_token = [0u8; 16];
        rng().fill_bytes(&mut rng_token);

        let mut expected_write = Vec::with_capacity(32);
        expected_write.extend_from_slice(&PROTOCOL_MAGIC);
        expected_write.extend_from_slice(&rng_token);

        let mut server_response = Vec::with_capacity(17);
        server_response.extend_from_slice(&PROTOCOL_MAGIC);
        server_response.push(0x0); // Server indicates auth failure

        let mut mock_stream = Builder::new()
            .write(&expected_write)
            .read(&server_response)
            .build();

        let result = authenticate_to_server(&rng_token, &mut mock_stream).await;
        assert!(
            result.is_err(),
            "Client should have returned an error based on server's 0x0 response flag"
        );
    }
}
