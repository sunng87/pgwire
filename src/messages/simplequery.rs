use bytes::BytesMut;

use super::codec;
use super::Message;

#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct Query {
    query: String,
}

impl Query {
    pub fn new(query: String) -> Query {
        Query { query }
    }
}

impl Message for Query {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(b'Q')
    }

    fn message_length(&self) -> i32 {
        (5 + self.query.as_bytes().len()) as i32
    }

    fn encode_body(&self, buf: &mut BytesMut) -> std::io::Result<()> {
        codec::put_cstring(buf, &self.query);

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut) -> std::io::Result<Self> {
        let query = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());

        Ok(Query::new(query))
    }
}
