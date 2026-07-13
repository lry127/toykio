use bytes::{Buf, BufMut, BytesMut};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};

pub type HashedAuthSecret = [u8; 16];
pub fn compute_auth_hash_from_raw(raw: &str) -> HashedAuthSecret {
    let hash = Sha256::digest(raw.as_bytes());
    let mut res = [0u8; 16];
    res.copy_from_slice(&hash.0[..16]);
    res
}

pub trait WireMessage {
    fn serialize_to_bytes(&self, buf: &mut BytesMut);
}

pub struct ConnectionEstablishMessageC2S {
    pub hashed_auth_secret: HashedAuthSecret,
    pub ip: u32,
    pub port: u16,
}

pub struct ConnectionEstablishResponseS2C {
    pub error_type: u16,
}

impl WireMessage for ConnectionEstablishResponseS2C {
    fn serialize_to_bytes(&self, buf: &mut BytesMut) {
        buf.put_u16(self.error_type);
    }
}

impl ConnectionEstablishMessageC2S {
    const MESSAGE_SIZE: u32 = 16 + u32::BITS / 8 + u16::BITS / 8;
    pub async fn read_from_stream<T: AsyncRead + Unpin>(
        remote: &mut T,
    ) -> tokio::io::Result<ConnectionEstablishMessageC2S> {
        let mut buf = [0u8; Self::MESSAGE_SIZE as usize];
        remote.read_exact(&mut buf).await?;
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
