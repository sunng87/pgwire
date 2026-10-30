use bytes::{BufMut, BytesMut};
use pgwire::error::PgWireError;
use pgwire::messages::{
    DecodeContext, Message, PgWireBackendMessage, PgWireFrontendMessage, copy::CopyData,
    startup::Startup,
};

#[test]
fn copy_data_rejects_lengths_shorter_than_header() {
    let ctx = DecodeContext::default();
    for length in [0, 1, 2, 3] {
        let mut packet = BytesMut::new();
        packet.put_u8(b'd');
        packet.put_i32(length);
        assert!(matches!(
            CopyData::decode(&mut packet, &ctx),
            Err(PgWireError::InvalidMessageLength(actual)) if actual == length as usize
        ));
        // A failed decode must not consume the malformed header.
        assert_eq!(packet.len(), 5);
    }
}

#[test]
fn startup_rejects_lengths_shorter_than_header() {
    let ctx = DecodeContext::default();
    for length in [0, 1, 2, 3] {
        let mut packet = BytesMut::new();
        packet.put_i32(length);
        assert!(matches!(
            Startup::decode(&mut packet, &ctx),
            Err(PgWireError::InvalidMessageLength(actual)) if actual == length as usize
        ));
        assert_eq!(packet.len(), 4);
    }
}

#[test]
fn other_frontend_and_backend_messages_reject_short_lengths() {
    let mut ctx = DecodeContext::default();
    ctx.awaiting_frontend_ssl = false;
    ctx.awaiting_frontend_startup = false;

    for length in [0, 1, 2, 3] {
        for message_type in [b'B', b'P', b'Q', b'd'] {
            let mut packet = BytesMut::new();
            packet.put_u8(message_type);
            packet.put_i32(length);
            assert!(matches!(
                PgWireFrontendMessage::decode(&mut packet, &ctx),
                Err(PgWireError::InvalidMessageLength(actual)) if actual == length as usize
            ));
        }
        for message_type in [b'D', b'G', b'R'] {
            let mut packet = BytesMut::new();
            packet.put_u8(message_type);
            packet.put_i32(length);
            assert!(matches!(
                PgWireBackendMessage::decode(&mut packet, &ctx),
                Err(PgWireError::InvalidMessageLength(actual)) if actual == length as usize
            ));
        }
    }
}

#[test]
fn incomplete_header_waits_and_valid_empty_copy_data_decodes() {
    let ctx = DecodeContext::default();
    let mut packet = BytesMut::from(&b"d\0\0"[..]);
    assert!(CopyData::decode(&mut packet, &ctx).unwrap().is_none());
    packet.put_slice(&[0, 4]);
    let message = CopyData::decode(&mut packet, &ctx).unwrap().unwrap();
    assert!(message.data.is_empty());
    assert!(packet.is_empty());
}
