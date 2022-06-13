use std::str;

use bytes::{Buf, BufMut, BytesMut};

/// Get null-terminated string, returns None when empty cstring read.
///
/// Note that this implementation will also advance cursor by 1 after reading
/// empty cstring. This behaviour works for how postgres wire protocol handling
/// key-value pairs, which is ended by a single `\0`
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

/// Put null-termianted string
///
/// You can put empty string by giving `""` as input.
pub(crate) fn put_cstring(buf: &mut BytesMut, input: &str) {
    buf.put_slice(input.as_bytes());
    buf.put_u8(b'\0');
}

/// Try read message length from buf, without actally move the cursor
pub(crate) fn get_length(buf: &BytesMut) -> Option<usize> {
    if buf.remaining() >= 4 {
        Some((&buf[..4]).get_i32() as usize)
    } else {
        None
    }
}

/// Check if message_length matches and move the cursor to right position then
/// call the `decode_fn` for the body
pub(crate) fn decode_packet<T, F>(buf: &mut BytesMut, decode_fn: F) -> std::io::Result<Option<T>>
where
    F: Fn(&mut BytesMut, usize) -> std::io::Result<T>,
{
    if let Some(msg_len) = get_length(buf) {
        if buf.remaining() >= msg_len {
            // skip msg_len
            buf.advance(4);

            return decode_fn(buf, msg_len).map(|r| Some(r));
        }
    }

    Ok(None)
}

pub(crate) fn get_and_ensure_message_type(buf: &mut BytesMut, t: u8) -> std::io::Result<()> {
    let msg_type = buf.get_u8();
    // ensure the type is corrent
    if msg_type != t {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid message type",
        ));
    }

    Ok(())
}
