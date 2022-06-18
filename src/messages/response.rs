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

#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug, Default)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct ErrorResponse {
    fields: Vec<(u8, String)>,
}

impl Message for ErrorResponse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(b'E')
    }

    fn message_length(&self) -> i32 {
        4 + self
            .fields
            .iter()
            .map(|f| 1 + f.1.as_bytes().len() + 1)
            .sum::<usize>() as i32
    }

    fn encode_body(&self, buf: &mut BytesMut) -> std::io::Result<()> {
        for (code, value) in &self.fields {
            buf.put_u8(*code);
            codec::put_cstring(buf, value);
        }

        buf.put_u8(b'\0');

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut) -> std::io::Result<Self> {
        let mut fields = Vec::new();
        loop {
            let code = buf.get_u8();

            if code == b'\0' {
                return Ok(ErrorResponse { fields });
            } else {
                if let Some(value) = codec::get_cstring(buf) {
                    fields.push((code, value));
                } else {
                    // error
                }
            }
        }
    }
}
