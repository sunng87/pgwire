use bytes::{Buf, BufMut, BytesMut};

use super::codec;
use super::Message;
use crate::error::PgWireResult;

#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug, new)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct CommandComplete {
    tag: String,
}

pub const MESSAGE_TYPE_BYTE_COMMAND_COMPLETE: u8 = b'C';

impl Message for CommandComplete {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_COMMAND_COMPLETE)
    }

    fn message_length(&self) -> usize {
        5 + self.tag.as_bytes().len()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        codec::put_cstring(buf, &self.tag);

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize) -> PgWireResult<Self> {
        let tag = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());

        Ok(CommandComplete::new(tag))
    }
}

#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug, new)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct ReadyForQuery {
    status: u8,
}

pub const READY_STATUS_IDLE: u8 = b'I';
pub const READY_STATUS_TRANSACTION_BLOCK: u8 = b'T';
pub const READY_STATUS_FAILED_TRANSACTION_BLOCK: u8 = b'E';

pub const MESSAGE_TYPE_BYTE_READY_FOR_QUERY: u8 = b'Z';

impl Message for ReadyForQuery {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_READY_FOR_QUERY)
    }

    #[inline]
    fn message_length(&self) -> usize {
        5
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(self.status);

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize) -> PgWireResult<Self> {
        let status = buf.get_u8();
        Ok(ReadyForQuery::new(status))
    }
}

/// postgres error response, sent from backend to frontend
#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug, Default, new)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct ErrorResponse {
    fields: Vec<(u8, String)>,
}

pub const MESSAGE_TYPE_BYTE_ERROR_RESPONSE: u8 = b'E';

impl Message for ErrorResponse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_ERROR_RESPONSE)
    }

    fn message_length(&self) -> usize {
        4 + self
            .fields
            .iter()
            .map(|f| 1 + f.1.as_bytes().len() + 1)
            .sum::<usize>()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        for (code, value) in &self.fields {
            buf.put_u8(*code);
            codec::put_cstring(buf, value);
        }

        buf.put_u8(b'\0');

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize) -> PgWireResult<Self> {
        let mut fields = Vec::new();
        loop {
            let code = buf.get_u8();

            if code == b'\0' {
                return Ok(ErrorResponse { fields });
            } else {
                let value = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());
                fields.push((code, value));
            }
        }
    }
}
