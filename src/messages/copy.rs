use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::codec;
use super::Message;
use crate::error::PgWireResult;

pub const MESSAGE_TYPE_BYTE_COPY_DATA: u8 = b'd';

#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct CopyData {
    pub data: Bytes,
}

impl Message for CopyData {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_COPY_DATA)
    }

    fn message_length(&self) -> usize {
        4 + self.data.len()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put(self.data.as_ref());
        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, len: usize) -> PgWireResult<Self> {
        let data = buf.split_to(len - 4).freeze();
        Ok(Self::new(data))
    }
}

pub const MESSAGE_TYPE_BYTE_COPY_DONE: u8 = b'c';

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

    fn decode_body(_buf: &mut BytesMut, _len: usize) -> PgWireResult<Self> {
        Ok(Self::new())
    }
}

pub const MESSAGE_TYPE_BYTE_COPY_FAIL: u8 = b'f';

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
        4 + self.message.as_bytes().len() + 1
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        codec::put_cstring(buf, &self.message);
        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _len: usize) -> PgWireResult<Self> {
        let msg = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());
        Ok(Self::new(msg))
    }
}

pub const MESSAGE_TYPE_BYTE_COPY_IN_RESPONSE: u8 = b'G';

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

    fn decode_body(buf: &mut BytesMut, _len: usize) -> PgWireResult<Self> {
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

#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct CopyOutResponse {
    format: i8,
    columns: i16,
    column_formats: Vec<i16>,
}

impl Message for CopyOutResponse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_COPY_OUT_RESPONSE)
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

    fn decode_body(buf: &mut BytesMut, _len: usize) -> PgWireResult<Self> {
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

#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct CopyBothResponse {
    format: i8,
    columns: i16,
    column_formats: Vec<i16>,
}

impl Message for CopyBothResponse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_COPY_BOTH_RESPONSE)
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

    fn decode_body(buf: &mut BytesMut, _len: usize) -> PgWireResult<Self> {
        let format = buf.get_i8();
        let columns = buf.get_i16();
        let mut column_formats = Vec::with_capacity(columns as usize);
        for _ in 0..columns {
            column_formats.push(buf.get_i16());
        }

        Ok(Self::new(format, columns, column_formats))
    }
}
