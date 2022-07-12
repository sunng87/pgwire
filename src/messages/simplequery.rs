use bytes::BytesMut;

use super::codec;
use super::Message;
use crate::error::PgWireResult;

/// A sql query sent from frontend to backend.
#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug, new)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct Query {
    query: String,
}

pub const MESSAGE_TYPE_BYTE_QUERY: u8 = b'Q';

impl Message for Query {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_QUERY)
    }

    fn message_length(&self) -> usize {
        5 + self.query.as_bytes().len()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        codec::put_cstring(buf, &self.query);

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut) -> PgWireResult<Self> {
        let query = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());

        Ok(Query::new(query))
    }
}
