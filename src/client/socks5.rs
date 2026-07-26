use crate::ReadBufNExt;
use anyhow::{Context, bail};
use bytes::{Buf, BufMut, BytesMut};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio::io;

use crate::client::socks5::VariableHostRepr::{DomainName, Ipv4};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::lookup_host;
use tracing::debug;

#[derive(Debug)]
pub enum VariableHostRepr {
    Ipv4(u32),
    DomainName(String),
}

impl VariableHostRepr {
    const TYPE_IPV4: u8 = 0x1;
    const TYPE_DOMAIN: u8 = 0x3;
    pub fn new_ip(ip: u32) -> Self {
        Ipv4(ip)
    }

    pub fn new_domain(domain_name: String) -> Self {
        DomainName(domain_name)
    }

    fn get_type_repr(&self) -> u8 {
        match self {
            Ipv4(_) => 0x1,
            DomainName(_) => 0x3,
        }
    }

    pub fn serialize_to_buf(&self, buf: &mut BytesMut) {
        buf.put_u8(self.get_type_repr());
        match self {
            Ipv4(ip) => {
                buf.put_u32(*ip);
            }
            DomainName(addr) => {
                buf.put_u8(addr.len() as u8);
                buf.put(addr.as_bytes());
            }
        }
    }

    pub async fn read_from_stream<T: AsyncRead + Unpin>(
        s: &mut T,
        read_buf: &mut BytesMut,
    ) -> tokio::io::Result<Self> {
        s.read_buf_n(read_buf, 1).await?;
        match read_buf.get_u8() {
            Self::TYPE_IPV4 => {
                s.read_buf_n(read_buf, 4).await?;
                Ok(Ipv4(read_buf.get_u32()))
            }
            Self::TYPE_DOMAIN => {
                s.read_buf_n(read_buf, 1).await?;
                let domain_len = read_buf.get_u8() as usize;
                s.read_buf_n(read_buf, domain_len).await?;
                let domain_bytes = read_buf.split_to(domain_len);

                let domain_name = String::from_utf8(domain_bytes.to_vec())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(DomainName(domain_name))
            }
            unknown => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown type: {unknown}"),
            )),
        }
    }

    pub async fn resolve(&self, port: u16) -> anyhow::Result<SocketAddr> {
        match self {
            Ipv4(ip) => {
                let socket_addr = SocketAddrV4::new(Ipv4Addr::from(*ip), port);
                Ok(SocketAddr::V4(socket_addr))
            }
            DomainName(domain) => lookup_host(format!("{domain}:{port}"))
                .await?
                .next()
                .context("can't resolve"),
        }
    }
}

const SOCKS5_PROTOCOL_VERSION: u8 = 0x5;
const RESERVED: u8 = 0x0;

pub async fn consume_client_hello<T: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut T,
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
) -> anyhow::Result<()> {
    stream.read_buf_n(read_buf, 2).await?;

    if read_buf.get_u8() != SOCKS5_PROTOCOL_VERSION {
        bail!("protocol error: client hello: 0x5 expected");
    }
    let nums_of_methods = read_buf.get_u8() as usize;
    stream.read_buf_n(read_buf, nums_of_methods).await?;
    let methods = &read_buf[..nums_of_methods];
    debug!("supported auth methods: {:?}", methods);
    let method_accepted = methods.contains(&0u8);
    read_buf.advance(nums_of_methods);

    let selected_method = if method_accepted { 0u8 } else { 0xffu8 };
    write_buf.put_slice(&[0x5u8, selected_method]);
    stream.write_all_buf(write_buf).await?;
    if !method_accepted {
        bail!("no auth method selected");
    }

    Ok(())
}

pub async fn handle_target_addr_negotiation<T: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut T,
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
) -> anyhow::Result<(VariableHostRepr, u16)> {
    stream.read_buf_n(read_buf, 3).await?;
    if read_buf.get_u8() != SOCKS5_PROTOCOL_VERSION {
        bail!("protocol error: client request: 0x5 expected");
    }
    let cmd = read_buf.get_u8();

    const CMD_CONNECT: u8 = 0x1;
    if cmd != CMD_CONNECT {
        write_buf.put_slice(&construct_connection_server_reply(
            ConnectionServerReplyCode::CmdNotSupported,
        ));
        let _ = stream.write_all_buf(write_buf).await;
        bail!("protocol error: client request: only tcp connect is supported");
    }
    let _rfc_reserved = read_buf.get_u8();

    debug!("begin read target_hostname");
    let target_hostname = match VariableHostRepr::read_from_stream(stream, read_buf).await {
        Ok(t) => t,
        Err(err) => {
            write_buf.put_slice(&construct_connection_server_reply(
                ConnectionServerReplyCode::AddrTypeNotSupported,
            ));
            let _ = stream.write_all_buf(write_buf).await;
            bail!(err);
        }
    };

    debug!("target host: (raw): {target_hostname:?}");
    let target_port = stream.read_u16().await?;
    debug!("target port: {target_port}");

    // if we accept the connection, wait for remote (actual) proxy server to establish tcp connection
    // before sending response to socks5 client at our side

    Ok((target_hostname, target_port))
}

#[repr(u8)]
pub enum ConnectionServerReplyCode {
    Success = 0x0,
    GeneralFailure = 0x1,
    ConnectionRefused = 0x5,
    CmdNotSupported = 0x7,
    AddrTypeNotSupported = 0x8,
}

pub fn construct_connection_server_reply(reply_code: ConnectionServerReplyCode) -> [u8; 10] {
    [
        SOCKS5_PROTOCOL_VERSION,
        reply_code as u8,
        RESERVED,
        /* BIND_ADDR is intentionally set to all 0's */
        /* atyp (1 byte) */
        0x1,
        /* ip (4 bytes) */
        0x0,
        0x0,
        0x0,
        0x0,
        /* port (2 bytes) */
        0x0,
        0x0,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use tokio_test::io::Builder;

    #[tokio::test]
    async fn test_variable_host_repr_ipv4() {
        // [ATYP: IPv4], [IP: 127.0.0.1]
        let mut mock = Builder::new()
            .read(&[VariableHostRepr::TYPE_IPV4, 127, 0, 0, 1])
            .build();

        let mut read_buf = BytesMut::new();
        let repr = VariableHostRepr::read_from_stream(&mut mock, &mut read_buf)
            .await
            .unwrap();

        match repr {
            Ipv4(ip) => {
                // 127.0.0.1 in hex is 0x7F000001
                assert_eq!(ip, 0x7F000001);
            }
            _ => panic!("Expected Ipv4"),
        }
    }

    #[tokio::test]
    async fn test_variable_host_repr_domain() {
        let domain = b"example.com";
        let mut input = vec![VariableHostRepr::TYPE_DOMAIN, domain.len() as u8];
        input.extend_from_slice(domain);

        let mut mock = Builder::new().read(&input).build();
        let mut read_buf = BytesMut::new();
        let repr = VariableHostRepr::read_from_stream(&mut mock, &mut read_buf)
            .await
            .unwrap();

        match repr {
            DomainName(d) => assert_eq!(d, "example.com"),
            _ => panic!("Expected DomainName"),
        }
    }

    #[tokio::test]
    async fn test_consume_client_hello_success() {
        // Client sends: [Ver: 5, NMETHODS: 2, METHOD: NO_AUTH(0), METHOD: USER_PASS(2)]
        // Server replies: [Ver: 5, METHOD: NO_AUTH(0)]
        let mut mock = Builder::new()
            .read(&[0x05, 0x02, 0x00, 0x02])
            .write(&[0x05, 0x00])
            .build();

        let mut read_buf = BytesMut::new();
        let mut write_buf = BytesMut::new();

        consume_client_hello(&mut mock, &mut read_buf, &mut write_buf)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_consume_client_hello_unsupported_auth() {
        // Client sends: [Ver: 5, NMETHODS: 1, METHOD: USER_PASS(2)]
        // Our proxy only supports NO_AUTH(0) right now.
        // Server replies: [Ver: 5, METHOD: NO_ACCEPTABLE(255)]
        let mut mock = Builder::new()
            .read(&[0x05, 0x01, 0x02])
            .write(&[0x05, 0xff])
            .build();

        let mut read_buf = BytesMut::new();
        let mut write_buf = BytesMut::new();

        let result = consume_client_hello(&mut mock, &mut read_buf, &mut write_buf).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "no auth method selected");
    }

    #[tokio::test]
    async fn test_handle_target_addr_negotiation_success() {
        // Client request:
        // [Ver: 5, Cmd: CONNECT(1), Rsv: 0] + [ATYP: IPv4(1), IP: 192.168.1.1] + [Port: 80(0x00, 0x50)]
        let mut mock = Builder::new()
            .read(&[0x05, 0x01, 0x00])
            .read(&[0x01, 192, 168, 1, 1])
            .read(&[0x00, 0x50])
            .build();

        let mut read_buf = BytesMut::new();
        let mut write_buf = BytesMut::new();

        let (target, port) =
            handle_target_addr_negotiation(&mut mock, &mut read_buf, &mut write_buf)
                .await
                .unwrap();

        assert_eq!(port, 80);
        match target {
            // 192.168.1.1 in hex is 0xC0A80101
            VariableHostRepr::Ipv4(ip) => assert_eq!(ip, 0xC0A80101),
            _ => panic!("Expected Ipv4"),
        }
    }

    #[tokio::test]
    async fn test_handle_target_addr_negotiation_unsupported_cmd() {
        // Client request:
        // [Ver: 5, Cmd: BIND(2), Rsv: 0] -> We only support CONNECT(1)
        let expected_reply =
            construct_connection_server_reply(ConnectionServerReplyCode::CmdNotSupported);

        let mut mock = Builder::new()
            .read(&[0x05, 0x02, 0x00])
            .write(&expected_reply)
            .build();

        let mut read_buf = BytesMut::new();
        let mut write_buf = BytesMut::new();

        let result = handle_target_addr_negotiation(&mut mock, &mut read_buf, &mut write_buf).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("only tcp connect is supported")
        );
    }
}
