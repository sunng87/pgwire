use std::io;
use std::str;

use bytes::{Buf, BytesMut};

pub(crate) trait MessageType {
    #[inline]
    fn message_type(&self) -> Option<u8> {
        None
    }
}

pub(crate) trait MessageLength {
    fn message_length(&self) -> i32;
}

pub(crate) trait Codec: Sized + MessageType + MessageLength {
    fn encode(&self, buf: &mut BytesMut) -> io::Result<()>;
    fn decode(buf: &mut BytesMut) -> io::Result<Option<Self>>;
}

pub(crate) fn get_cstring(buf: &mut BytesMut) -> Option<String> {
    let mut i = 0;

    // with bound check to prevent invalid format
    while i < buf.remaining() && buf[i] != b'\0' {
        i += 1;
    }

    // i+1: include the '\0'
    // move cursor to the end of cstring
    let string_buf = buf.split_off(i + 1);

    if i == 0 {
        None
    } else {
        // TODO: unwrap
        Some(str::from_utf8(&string_buf[..i]).unwrap().to_owned())
    }
}

mod startup;

pub enum Message {
    Startup(startup::Startup),
    Authentication(startup::Authentication),
    Password(startup::Password),
}
