use std::str;

use bytes::{Buf, BufMut, BytesMut};

pub(crate) fn get_cstring(buf: &mut BytesMut) -> Option<String> {
    let mut i = 0;

    // with bound check to prevent invalid format
    while i < buf.remaining() && buf[i] != b'\0' {
        i += 1;
    }

    // i+1: include the '\0'
    // move cursor to the end of cstring
    let string_buf = buf.split_to(i + 1);

    if i == 0 {
        None
    } else {
        // TODO: unwrap
        Some(str::from_utf8(&string_buf[..i]).unwrap().to_owned())
    }
}

pub(crate) fn put_cstring(buf: &mut BytesMut, s: &str) {
    buf.put_slice(s.as_bytes());
    buf.put_u8(b'\0');
}
