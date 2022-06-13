use bytes::{BufMut, BytesMut};

use super::codec;
use super::{Codec, MessageLength, MessageType};

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

impl MessageType for Query {
    fn message_type(&self) -> Option<u8> {
        Some(b'Q')
    }
}

impl MessageLength for Query {
    fn message_length(&self) -> i32 {
        (5 + self.query.as_bytes().len()) as i32
    }
}

impl Codec for Query {
    fn encode(&self, buf: &mut BytesMut) -> std::io::Result<()> {
        buf.put_u8(self.message_type().unwrap());
        buf.put_i32(self.message_length());
        codec::put_cstring(buf, &self.query);

        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> std::io::Result<Option<Self>> {
        codec::get_and_ensure_message_type(buf, b'Q')?;

        codec::decode_packet(buf, |buf, _| {
            let query = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());

            Ok(Query::new(query))
        })
    }
}
