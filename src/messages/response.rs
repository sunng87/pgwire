use bytes::{Buf, BufMut, BytesMut};

use super::codec;
use super::Message;

#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct CommandComplete {
    tag: String,
}

impl CommandComplete {
    pub fn new(tag: String) -> CommandComplete {
        CommandComplete { tag }
    }
}

impl Message for CommandComplete {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(b'C')
    }

    fn message_length(&self) -> i32 {
        (5 + self.tag.as_bytes().len()) as i32
    }

    fn encode_body(&self, buf: &mut BytesMut) -> std::io::Result<()> {
        codec::put_cstring(buf, &self.tag);

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut) -> std::io::Result<Self> {
        let tag = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());

        Ok(CommandComplete::new(tag))
    }
}

#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct ReadyForQuery {
    status: u8,
}

impl ReadyForQuery {
    pub fn new(status: u8) -> ReadyForQuery {
        ReadyForQuery { status }
    }
}

impl Message for ReadyForQuery {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(b'Z')
    }

    #[inline]
    fn message_length(&self) -> i32 {
        5
    }

    fn encode_body(&self, buf: &mut BytesMut) -> std::io::Result<()> {
        buf.put_u8(self.status);

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut) -> std::io::Result<Self> {
        let status = buf.get_u8();
        Ok(ReadyForQuery::new(status))
    }
}
