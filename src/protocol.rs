use crate::config::HashedAuthSecret;
use bytes::{Buf, BufMut, BytesMut};
use num_enum::TryFromPrimitive;
use std::io::{Error, ErrorKind};
use tokio::io::{AsyncRead, AsyncReadExt};

#[allow(async_fn_in_trait)]
pub trait WireMessage {
    fn serialize_to_bytes(&self, buf: &mut BytesMut);
    async fn read_from_stream<T: AsyncRead + Unpin>(s: &mut T) -> tokio::io::Result<Self>
    where
        Self: Sized;
}

pub struct ConnectionEstablishMessageC2S {
    pub hashed_auth_secret: HashedAuthSecret,
    pub ip: u32,
    pub port: u16,
}

impl WireMessage for ConnectionEstablishMessageC2S {
    fn serialize_to_bytes(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.hashed_auth_secret);
        buf.put_u32(self.ip);
        buf.put_u16(self.port);
    }

    async fn read_from_stream<T: AsyncRead + Unpin>(s: &mut T) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        const MESSAGE_SIZE: u32 = 16 + u32::BITS / 8 + u16::BITS / 8;
        let mut buf = [0u8; MESSAGE_SIZE as usize];

        s.read_exact(&mut buf).await?;
        let mut cursor = std::io::Cursor::new(buf);

        let mut hashed_auth_secret = [0u8; 16];
        cursor.copy_to_slice(&mut hashed_auth_secret);
        let ip = cursor.get_u32();
        let port = cursor.get_u16();

        Ok(Self {
            hashed_auth_secret,
            ip,
            port,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive)]
#[repr(u16)]
pub enum ConnectionEstablishErrorType {
    Success = 0,
    AuthError = 1,
    TargetError = 2,
}

pub struct ConnectionEstablishResponseS2C {
    pub error_type: ConnectionEstablishErrorType,
}

impl WireMessage for ConnectionEstablishResponseS2C {
    fn serialize_to_bytes(&self, buf: &mut BytesMut) {
        buf.put_u16(self.error_type as u16);
    }

    async fn read_from_stream<T: AsyncRead + Unpin>(s: &mut T) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let raw_error = s.read_u16().await?;
        let error_type = ConnectionEstablishErrorType::try_from(raw_error)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Unknown error type"))?;
        Ok(Self { error_type })
    }
}
