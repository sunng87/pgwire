use bytes::{BufMut, Bytes, BytesMut};
use pgwire::error::PgWireError;
use pgwire::messages::{
    DecodeContext, PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion,
    copy::{CopyBothResponse, CopyInResponse, CopyOutResponse},
    data::{DataRow, FieldDescription, ParameterDescription, RowDescription},
    extendedquery::{Bind, Close, Describe, Execute, Parse},
    response::{
        ErrorResponse, NoticeResponse, NotificationResponse, ReadyForQuery, TransactionStatus,
    },
    startup::{
        Authentication, BackendKeyData, NegotiateProtocolVersion, ParameterStatus, SecretKey,
    },
};

/// Context for decoding tagged frontend messages after startup/SSL
/// negotiation has completed.
fn frontend_ctx() -> DecodeContext {
    let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
    ctx.awaiting_frontend_ssl = false;
    ctx.awaiting_frontend_startup = false;
    ctx
}

fn backend_ctx() -> DecodeContext {
    DecodeContext::new(ProtocolVersion::PROTOCOL3_0)
}

/// The seven truncated extended-protocol packets reported in #481 must be
/// rejected with an error instead of panicking inside `bytes` bounds checks.
#[test]
fn frontend_extended_query_truncated_bodies_return_errors() {
    let ctx = frontend_ctx();

    let cases: &[(&str, &[u8])] = &[
        ("Parse, empty body", b"P\0\0\0\x04"),
        ("Parse, name+query only", b"P\0\0\0\x06\0\0"),
        ("Bind, empty body", b"B\0\0\0\x04"),
        ("Bind, portal+stmt only", b"B\0\0\0\x06\0\0"),
        ("Describe, empty body", b"D\0\0\0\x04"),
        ("Execute, empty body", b"E\0\0\0\x04"),
        ("Close, empty body", b"C\0\0\0\x04"),
        // extra truncations of the same shape, not in the original report
        // Bind declaring one 5-byte parameter without providing any data
        (
            "Bind, parameter length overruns body",
            b"B\0\0\0\x0e\0\0\0\0\x00\x01\x00\x00\x00\x05",
        ),
        // Parse declaring one type oid without providing it
        (
            "Parse, type oid count overruns body",
            b"P\0\0\0\x08\0\0\x00\x01",
        ),
    ];

    for (name, packet) in cases {
        let mut buf = BytesMut::from(&packet[..]);
        let result = PgWireFrontendMessage::decode(&mut buf, &ctx);
        assert!(
            matches!(
                result,
                Err(PgWireError::TruncatedMessage(..) | PgWireError::InvalidElementCount(..))
            ),
            "{name}: expected an error, got {result:?}"
        );
    }
}

/// Backend messages with truncated bodies must return errors too, so a
/// hostile or buggy server cannot panic a pgwire-based client.
#[test]
fn backend_truncated_bodies_return_errors() {
    let ctx = backend_ctx();

    let cases: &[(&str, &[u8])] = &[
        ("Authentication, empty body", b"R\0\0\0\x04"),
        // Authentication SASLContinue whose length cannot cover code + data
        (
            "Authentication, sasl continue underflow",
            b"R\0\0\0\x06\0\0\0\x0b",
        ),
        // Authentication MD5 salt missing
        ("Authentication, md5 salt missing", b"R\0\0\0\x08\0\0\0\x05"),
        ("BackendKeyData, empty body", b"K\0\0\0\x04"),
        ("RowDescription, empty body", b"T\0\0\0\x04"),
        (
            "RowDescription, count without fields",
            b"T\0\0\0\x06\x00\x01",
        ),
        ("ParameterDescription, empty body", b"t\0\0\0\x04"),
        (
            "ParameterDescription, count without oids",
            b"t\0\0\0\x06\x00\x01",
        ),
        ("DataRow, empty body", b"D\0\0\0\x04"),
        ("CopyInResponse, empty body", b"G\0\0\0\x04"),
        ("CopyOutResponse, empty body", b"H\0\0\0\x04"),
        ("CopyBothResponse, empty body", b"W\0\0\0\x04"),
        ("ReadyForQuery, empty body", b"Z\0\0\0\x04"),
        ("ErrorResponse, empty body", b"E\0\0\0\x04"),
        ("NoticeResponse, empty body", b"N\0\0\0\x04"),
        ("NotificationResponse, empty body", b"A\0\0\0\x04"),
        // NegotiateProtocolVersion without version/option count
        ("NegotiateProtocolVersion, empty body", b"v\0\0\0\x04"),
    ];

    for (name, packet) in cases {
        let mut buf = BytesMut::from(&packet[..]);
        let result = PgWireBackendMessage::decode(&mut buf, &ctx);
        assert!(
            result.is_err(),
            "{name}: expected an error, got {:?}",
            result.map(|o| o.is_some())
        );
    }
}

/// Every prefix of a valid message body, framed with a matching length
/// field, must decode without panicking: a complete packet that declares a
/// body shorter than the message type requires yields an error, never a
/// panic.
#[test]
fn all_body_prefixes_decode_without_panicking() {
    let fctx = frontend_ctx();
    let bctx = backend_ctx();

    let frontend_messages: Vec<(&str, PgWireFrontendMessage)> = vec![
        (
            "Parse",
            PgWireFrontendMessage::Parse(Parse::new(None, "SELECT 1".to_owned(), vec![25])),
        ),
        (
            "Bind",
            PgWireFrontendMessage::Bind(Bind::new(
                None,
                None,
                vec![0],
                vec![Some(Bytes::from_static(b"abc"))],
                vec![],
            )),
        ),
        (
            "Close",
            PgWireFrontendMessage::Close(Close::new(b'P', None)),
        ),
        (
            "Describe",
            PgWireFrontendMessage::Describe(Describe::new(b'S', None)),
        ),
        (
            "Execute",
            PgWireFrontendMessage::Execute(Execute::new(None, 0)),
        ),
    ];

    let backend_messages: Vec<(&str, PgWireBackendMessage)> =
        vec![
            (
                "Authentication::MD5Password",
                PgWireBackendMessage::Authentication(Authentication::MD5Password(vec![1, 2, 3, 4])),
            ),
            (
                "Authentication::SASLContinue",
                PgWireBackendMessage::Authentication(Authentication::SASLContinue(
                    Bytes::from_static(b"data"),
                )),
            ),
            (
                "Authentication::SASLFinal",
                PgWireBackendMessage::Authentication(Authentication::SASLFinal(
                    Bytes::from_static(b"data"),
                )),
            ),
            (
                "BackendKeyData",
                PgWireBackendMessage::BackendKeyData(BackendKeyData::new(1, SecretKey::I32(2))),
            ),
            (
                "NegotiateProtocolVersion",
                PgWireBackendMessage::NegotiateProtocolVersion(NegotiateProtocolVersion::new(
                    196610,
                    vec!["_something".to_owned()],
                )),
            ),
            (
                "ParameterStatus",
                PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                    "TimeZone".to_owned(),
                    "UTC".to_owned(),
                )),
            ),
            (
                "RowDescription",
                PgWireBackendMessage::RowDescription(RowDescription::new(vec![
                    FieldDescription::new("id".to_owned(), 0, 0, 25, 4, -1, 0),
                ])),
            ),
            (
                "ParameterDescription",
                PgWireBackendMessage::ParameterDescription(ParameterDescription::new(vec![25])),
            ),
            (
                "DataRow",
                PgWireBackendMessage::DataRow(DataRow::new(BytesMut::from(&b"\x00\x01ab"[..]), 1)),
            ),
            (
                "CopyInResponse",
                PgWireBackendMessage::CopyInResponse(CopyInResponse::new(0, 1, vec![0])),
            ),
            (
                "CopyOutResponse",
                PgWireBackendMessage::CopyOutResponse(CopyOutResponse::new(0, 1, vec![0])),
            ),
            (
                "CopyBothResponse",
                PgWireBackendMessage::CopyBothResponse(CopyBothResponse::new(0, 1, vec![0])),
            ),
            (
                "ReadyForQuery",
                PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle)),
            ),
            (
                "ErrorResponse",
                PgWireBackendMessage::ErrorResponse(ErrorResponse::new(vec![
                    (b'S', "FATAL".to_owned()),
                    (b'C', "08P01".to_owned()),
                    (b'M', "truncated".to_owned()),
                ])),
            ),
            (
                "NoticeResponse",
                PgWireBackendMessage::NoticeResponse(NoticeResponse::new(vec![(
                    b'S',
                    "NOTICE".to_owned(),
                )])),
            ),
            (
                "NotificationResponse",
                PgWireBackendMessage::NotificationResponse(NotificationResponse::new(
                    1,
                    "channel".to_owned(),
                    "payload".to_owned(),
                )),
            ),
        ];

    for (_name, message) in frontend_messages {
        let mut encoded = BytesMut::new();
        message.encode(&mut encoded).unwrap();
        for packet in truncated_variants(&encoded) {
            let mut buf = BytesMut::from(&packet[..]);
            // A panic here fails the test; errors and successful decodes are
            // both acceptable outcomes for an undersized body.
            let _ = PgWireFrontendMessage::decode(&mut buf, &fctx);
        }
    }

    for (_name, message) in backend_messages {
        let mut encoded = BytesMut::new();
        message.encode(&mut encoded).unwrap();
        for packet in truncated_variants(&encoded) {
            let mut buf = BytesMut::from(&packet[..]);
            let _ = PgWireBackendMessage::decode(&mut buf, &bctx);
        }
    }
}

/// From a complete encoded packet, build one re-framed variant per body
/// prefix: `tag + (4 + k) + body[..k]` for every `k < body.len()`. Each
/// variant is a complete packet whose length field matches the bytes
/// present, but whose body is too short for the message type.
fn truncated_variants(encoded: &[u8]) -> Vec<Vec<u8>> {
    let (&tag, rest) = encoded.split_first().expect("encoded packet has a tag");
    let body = &rest[4..];

    (0..body.len())
        .map(|k| {
            let mut packet = Vec::with_capacity(5 + k);
            packet.push(tag);
            packet.put_i32((4 + k) as i32);
            packet.extend_from_slice(&body[..k]);
            packet
        })
        .collect()
}

/// A `p` (password message family) packet with a body too short for a
/// SASLInitialResponse must fail coercion with an error instead of
/// panicking when the auth driver coerces the raw body.
#[test]
fn password_message_family_coercion_rejects_short_bodies() {
    let ctx = frontend_ctx();

    // body: empty auth method + 4-byte length header, but no data payload
    let mut buf = BytesMut::from(&b"p\0\0\0\x09\0\0\0\0\x05"[..]);
    let message = PgWireFrontendMessage::decode(&mut buf, &ctx)
        .expect("decode itself must not fail")
        .expect("packet is complete");
    let PgWireFrontendMessage::PasswordMessageFamily(raw) = message else {
        panic!("expected a PasswordMessageFamily message");
    };
    assert!(matches!(
        raw.into_sasl_initial_response(),
        Err(PgWireError::TruncatedMessage(..))
    ));

    // body: only the auth method cstring, missing the length header entirely
    let mut buf = BytesMut::from(&b"p\0\0\0\x07SCRAM\0"[..]);
    let message = PgWireFrontendMessage::decode(&mut buf, &ctx)
        .expect("decode itself must not fail")
        .expect("packet is complete");
    let PgWireFrontendMessage::PasswordMessageFamily(raw) = message else {
        panic!("expected a PasswordMessageFamily message");
    };
    assert!(matches!(
        raw.into_sasl_initial_response(),
        Err(PgWireError::TruncatedMessage(..))
    ));
}

/// The full round-trip must still succeed: valid messages encode and decode
/// to equal values after the added bounds checks.
#[test]
fn valid_extended_query_messages_still_round_trip() {
    let ctx = frontend_ctx();

    let messages = vec![
        PgWireFrontendMessage::Parse(Parse::new(
            Some("stmt".to_owned()),
            "SELECT $1".to_owned(),
            vec![25, 23],
        )),
        PgWireFrontendMessage::Bind(Bind::new(
            Some("portal".to_owned()),
            Some("stmt".to_owned()),
            vec![1],
            vec![None, Some(Bytes::from_static(b"xy"))],
            vec![0, 1],
        )),
        PgWireFrontendMessage::Close(Close::new(b'P', Some("portal".to_owned()))),
        PgWireFrontendMessage::Describe(Describe::new(b'S', Some("stmt".to_owned()))),
        PgWireFrontendMessage::Execute(Execute::new(Some("portal".to_owned()), 100)),
    ];

    for message in messages {
        let mut encoded = BytesMut::new();
        message.encode(&mut encoded).unwrap();
        assert!(!encoded.is_empty());

        let mut buf = encoded;
        let decoded = PgWireFrontendMessage::decode(&mut buf, &ctx)
            .expect("valid message must decode")
            .expect("complete message must not be deferred");
        assert!(buf.is_empty());

        // `PgWireFrontendMessage` does not implement `PartialEq`, so verify
        // the round trip at the wire level instead
        let mut reencoded = BytesMut::new();
        decoded.encode(&mut reencoded).unwrap();
        let mut expected = BytesMut::new();
        message.encode(&mut expected).unwrap();
        assert_eq!(reencoded, expected);
    }
}
