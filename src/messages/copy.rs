use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{codec, DecodeContext, Message};
use crate::error::PgWireResult;

pub const MESSAGE_TYPE_BYTE_COPY_DATA: u8 = b'd';

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct CopyData {
    pub data: Bytes,
}

impl Message for CopyData {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_COPY_DATA)
    }

    #[inline]
    fn max_message_length() -> usize {
        super::LARGE_PACKET_SIZE_LIMIT
    }

    fn message_length(&self) -> usize {
        4 + self.data.len()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put(self.data.as_ref());
        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, len: usize, _ctx: &DecodeContext) -> PgWireResult<Self> {
        let data = buf.split_to(len - 4).freeze();
        Ok(Self::new(data))
    }
}

pub const MESSAGE_TYPE_BYTE_COPY_DONE: u8 = b'c';

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct CopyDone;

impl Message for CopyDone {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_COPY_DONE)
    }

    fn message_length(&self) -> usize {
        4
    }

    fn encode_body(&self, _buf: &mut BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    fn decode_body(_buf: &mut BytesMut, _len: usize, _ctx: &DecodeContext) -> PgWireResult<Self> {
        Ok(Self::new())
    }
}

pub const MESSAGE_TYPE_BYTE_COPY_FAIL: u8 = b'f';

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct CopyFail {
    pub message: String,
}

impl Message for CopyFail {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_COPY_DONE)
    }

    fn message_length(&self) -> usize {
        4 + self.message.len() + 1
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        codec::put_cstring(buf, &self.message);
        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _len: usize, _ctx: &DecodeContext) -> PgWireResult<Self> {
        let msg = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());
        Ok(Self::new(msg))
    }
}

pub const MESSAGE_TYPE_BYTE_COPY_IN_RESPONSE: u8 = b'G';

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct CopyInResponse {
    pub format: i8,
    pub columns: i16,
    pub column_formats: Vec<i16>,
}

impl Message for CopyInResponse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_COPY_IN_RESPONSE)
    }

    #[inline]
    fn max_message_length() -> usize {
        super::SMALL_BACKEND_PACKET_SIZE_LIMIT
    }

    fn message_length(&self) -> usize {
        4 + 1 + 2 + self.column_formats.len() * 2
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_i8(self.format);
        buf.put_i16(self.columns);
        for cf in &self.column_formats {
            buf.put_i16(*cf);
        }
        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _len: usize, _ctx: &DecodeContext) -> PgWireResult<Self> {
        let format = buf.get_i8();
        let columns = buf.get_i16();
        let mut column_formats = Vec::with_capacity(columns as usize);
        for _ in 0..columns {
            column_formats.push(buf.get_i16());
        }

        Ok(Self::new(format, columns, column_formats))
    }
}

pub const MESSAGE_TYPE_BYTE_COPY_OUT_RESPONSE: u8 = b'H';

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct CopyOutResponse {
    pub format: i8,
    pub columns: i16,
    pub column_formats: Vec<i16>,
}

impl Message for CopyOutResponse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_COPY_OUT_RESPONSE)
    }

    #[inline]
    fn max_message_length() -> usize {
        super::SMALL_BACKEND_PACKET_SIZE_LIMIT
    }

    fn message_length(&self) -> usize {
        4 + 1 + 2 + self.column_formats.len() * 2
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_i8(self.format);
        buf.put_i16(self.columns);
        for cf in &self.column_formats {
            buf.put_i16(*cf);
        }
        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _len: usize, _ctx: &DecodeContext) -> PgWireResult<Self> {
        let format = buf.get_i8();
        let columns = buf.get_i16();
        let mut column_formats = Vec::with_capacity(columns as usize);
        for _ in 0..columns {
            column_formats.push(buf.get_i16());
        }

        Ok(Self::new(format, columns, column_formats))
    }
}

pub const MESSAGE_TYPE_BYTE_COPY_BOTH_RESPONSE: u8 = b'W';

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct CopyBothResponse {
    pub format: i8,
    pub columns: i16,
    pub column_formats: Vec<i16>,
}

impl Message for CopyBothResponse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_COPY_BOTH_RESPONSE)
    }

    #[inline]
    fn max_message_length() -> usize {
        super::SMALL_BACKEND_PACKET_SIZE_LIMIT
    }

    fn message_length(&self) -> usize {
        4 + 1 + 2 + self.column_formats.len() * 2
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_i8(self.format);
        buf.put_i16(self.columns);
        for cf in &self.column_formats {
            buf.put_i16(*cf);
        }
        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _len: usize, _ctx: &DecodeContext) -> PgWireResult<Self> {
        let format = buf.get_i8();
        let columns = buf.get_i16();
        let mut column_formats = Vec::with_capacity(columns as usize);
        for _ in 0..columns {
            column_formats.push(buf.get_i16());
        }

        Ok(Self::new(format, columns, column_formats))
    }
}
