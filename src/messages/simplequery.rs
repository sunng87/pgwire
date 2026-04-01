use bytes::BytesMut;

use super::{DecodeContext, Message, codec};
use crate::error::PgWireResult;

/// A sql query sent from frontend to backend.
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct Query {
    pub query: String,
}

pub const MESSAGE_TYPE_BYTE_QUERY: u8 = b'Q';

impl Message for Query {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_QUERY)
    }

    fn message_length(&self) -> usize {
        5 + self.query.len()
    }

    #[inline]
    fn max_message_length() -> usize {
        super::LARGE_PACKET_SIZE_LIMIT
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        codec::put_cstring(buf, &self.query);

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize, _ctx: &DecodeContext) -> PgWireResult<Self> {
        let query = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());

        Ok(Query::new(query))
    }
}
