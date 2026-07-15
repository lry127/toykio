use crate::ReadBufNExt;
use anyhow::bail;
use bytes::{Buf, BufMut, BytesMut};
use std::net::Ipv4Addr;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::debug;

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
) -> anyhow::Result<(u32, u16)> {
    stream.read_buf_n(read_buf, 4).await?;
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
    let addr_type = read_buf.get_u8();

    const IPV4_ADDR_TYPE: u8 = 0x1;
    if addr_type != IPV4_ADDR_TYPE {
        // todo: support domain type (0x3) (name resolution happens here or at remote outbound server)
        write_buf.put_slice(&construct_connection_server_reply(
            ConnectionServerReplyCode::AddrTypeNotSupported,
        ));
        let _ = stream.write_all_buf(write_buf).await;
        bail!("protocol error: client request: only ipv4 addr type is supported");
    }

    stream.read_buf_n(read_buf, 6).await?;
    let target_ip = read_buf.get_u32();
    debug!(
        "target ip: (raw): {target_ip}, formatted: {:?}",
        Ipv4Addr::from(target_ip)
    );
    let target_port = read_buf.get_u16();
    debug!("target port: {target_port}");

    // if we accept the connection, wait for remote (actual) proxy server to establish tcp connection
    // before sending response to socks5 client at our side

    Ok((target_ip, target_port))
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
