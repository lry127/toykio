use crate::ReadBufNExt;
use anyhow::{Context, bail};
use bytes::{Buf, BufMut, BytesMut};
use std::io::{Cursor, Error, ErrorKind};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use crate::socks5::VariableHostRepr::{DomainName, Ipv4};
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

    pub async fn read_from_stream<T: AsyncRead + Unpin>(s: &mut T) -> tokio::io::Result<Self> {
        match s.read_u8().await? {
            Self::TYPE_IPV4 => Ok(Ipv4(s.read_u32().await?)),
            Self::TYPE_DOMAIN => {
                let domain_len = s.read_u8().await? as usize;
                let mut buf = vec![0; domain_len];
                s.read_exact(&mut buf).await?;

                let domain_name =
                    String::from_utf8(buf).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
                Ok(DomainName(domain_name))
            }
            unknown => Err(Error::new(
                ErrorKind::InvalidData,
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
    write_buf: &mut BytesMut,
) -> anyhow::Result<(VariableHostRepr, u16)> {
    let mut read_buf = [0; 3];
    stream.take(3).read_exact(&mut read_buf).await?;
    let mut cursor = Cursor::new(read_buf);
    if cursor.get_u8() != SOCKS5_PROTOCOL_VERSION {
        bail!("protocol error: client request: 0x5 expected");
    }
    let cmd = cursor.get_u8();

    const CMD_CONNECT: u8 = 0x1;
    if cmd != CMD_CONNECT {
        write_buf.put_slice(&construct_connection_server_reply(
            ConnectionServerReplyCode::CmdNotSupported,
        ));
        let _ = stream.write_all_buf(write_buf).await;
        bail!("protocol error: client request: only tcp connect is supported");
    }
    let _rfc_reserved = cursor.get_u8();

    debug!("begin read target_hostname");
    let target_hostname = match VariableHostRepr::read_from_stream(stream).await {
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
