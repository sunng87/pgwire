use bytes::{Buf, BufMut, BytesMut};

use super::codec;
use super::{Codec, MessageLength, MessageType};

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

impl MessageType for CommandComplete {
    fn message_type(&self) -> Option<u8> {
        Some(b'C')
    }
}

impl MessageLength for CommandComplete {
    fn message_length(&self) -> i32 {
        (5 + self.tag.as_bytes().len()) as i32
    }
}

impl Codec for CommandComplete {
    fn encode(&self, buf: &mut BytesMut) -> std::io::Result<()> {
        buf.put_u8(self.message_type().unwrap());
        buf.put_i32(self.message_length());
        codec::put_cstring(buf, &self.tag);

        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> std::io::Result<Option<Self>> {
        codec::get_and_ensure_message_type(buf, b'C')?;

        codec::decode_packet(buf, |buf, _| {
            let tag = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());

            Ok(CommandComplete::new(tag))
        })
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

impl MessageType for ReadyForQuery {
    fn message_type(&self) -> Option<u8> {
        Some(b'Z')
    }
}

impl MessageLength for ReadyForQuery {
    #[inline]
    fn message_length(&self) -> i32 {
        5
    }
}

impl Codec for ReadyForQuery {
    fn encode(&self, buf: &mut BytesMut) -> std::io::Result<()> {
        buf.put_u8(self.message_type().unwrap());
        buf.put_i32(self.message_length());
        buf.put_u8(self.status);

        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> std::io::Result<Option<Self>> {
        codec::get_and_ensure_message_type(buf, b'Z')?;

        codec::decode_packet(buf, |buf, _| {
            let status = buf.get_u8();
            Ok(ReadyForQuery::new(status))
        })
    }
}
