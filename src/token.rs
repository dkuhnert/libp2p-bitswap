use std::{
    convert::TryInto,
    io::{self, ErrorKind},
};

/// Bitswap authentication.
/// See: https://github.com/ipfs/specs/pull/270
/// Format:
/// ```ebnf
/// Token = MultiCodec TokenLength TokenValue
/// MultiCodec = unsigned_varint_u64
/// TokenLength = unsigned_varint_u64
/// TokenValue = *OCTET
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Token(pub u64, pub Vec<u8>);
impl Token {
    /// Write token as bytes.
    pub fn write_to<W: io::Write>(&self, w: &mut W) -> io::Result<()> {
        let mut buf = unsigned_varint::encode::u64_buffer();

        // MultiCodec
        let multicodec = unsigned_varint::encode::u64(self.0, &mut buf);
        w.write_all(multicodec)?;

        // TokenLength
        let token_length = unsigned_varint::encode::u64(
            self.1
                .len()
                .try_into()
                .map_err(|_| ErrorKind::InvalidInput)?,
            &mut buf,
        );
        w.write_all(token_length)?;

        // TokenValue
        w.write_all(&self.1)?;

        Ok(())
    }

    /// Read token from bytes.
    pub fn read_bytes<R: io::Read>(mut r: R) -> io::Result<Self> {
        let multicodec = unsigned_varint::io::read_u64(&mut r).map_err(Into::<io::Error>::into)?;
        let token_length =
            unsigned_varint::io::read_u64(&mut r).map_err(Into::<io::Error>::into)?;
        let mut token = vec![
            0;
            token_length
                .try_into()
                .map_err(|_| ErrorKind::InvalidInput)?
        ];
        r.read_exact(&mut token)?;
        Ok(Self(multicodec, token))
    }

    /// Token as bytes vector.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut result = vec![0; 8 + 8 + self.1.len()];
        self.write_to(&mut result).unwrap();
        result
    }

    /// Create token from bytes.
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        Self::read_bytes(bytes)
    }
}
