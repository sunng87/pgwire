use bytes::{BufMut, BytesMut};
use pgwire::messages::{
    DecodeContext, Message, PgWireBackendMessage, PgWireFrontendMessage,
    extendedquery::{Bind, Execute, Parse},
    response::{ReadyForQuery, TransactionStatus},
    simplequery::Query,
    startup::Startup,
};

fn frontend_ctx() -> DecodeContext {
    let mut ctx = DecodeContext::default();
    ctx.awaiting_frontend_ssl = false;
    ctx.awaiting_frontend_startup = false;
    ctx
}

fn frame(tag: u8, body: &[u8]) -> BytesMut {
    let mut packet = BytesMut::new();
    packet.put_u8(tag);
    packet.put_i32((4 + body.len()) as i32);
    packet.extend_from_slice(body);
    packet
}

#[test]
fn truncated_frontend_bodies_cannot_read_the_next_frame() {
    let ctx = frontend_ctx();
    let next = frame(b'Q', b"SELECT 1\0");
    let cases: &[(u8, &[u8])] = &[
        (b'P', b""),
        (b'P', b"\0\0"),
        // One OID declared, but not supplied.
        (b'P', b"\0\0\0\x01"),
        (b'B', b""),
        (b'B', b"\0\0"),
        // One parameter declaring five bytes, but no payload.
        (b'B', b"\0\0\0\0\0\x01\0\0\0\x05"),
        // One result format declared, but not supplied.
        (b'B', b"\0\0\0\0\0\0\0\x01"),
        (b'D', b""),
        (b'E', b""),
        (b'C', b""),
    ];

    for &(tag, body) in cases {
        let mut buf = frame(tag, body);
        buf.extend_from_slice(&next);
        assert!(
            PgWireFrontendMessage::decode(&mut buf, &ctx).is_err(),
            "tag {tag}, body {body:?} borrowed bytes from the next frame"
        );
        assert_eq!(buf, next, "tag {tag}, body {body:?}");
        assert!(matches!(
            PgWireFrontendMessage::decode(&mut buf, &ctx).unwrap(),
            Some(PgWireFrontendMessage::Query(_))
        ));
        assert!(buf.is_empty());
    }
}

#[test]
fn truncated_backend_bodies_cannot_read_the_next_frame() {
    let ctx = DecodeContext::default();
    let next = frame(b'Z', b"I");
    let cases: &[(u8, &[u8])] = &[
        (b'R', b""),
        (b'R', b"\0\0\0\x05"), // MD5 authentication without salt
        (b'K', b""),
        (b'T', b""),
        (b'T', b"\0\x01"), // one row field without its description
        (b't', b"\0\x01"), // one parameter without its OID
        (b'D', b""),
        (b'G', b""),
        (b'H', b""),
        (b'W', b""),
        (b'Z', b""),
        (b'E', b""),
        (b'N', b""),
        (b'A', b""),
        (b'v', b""),
    ];

    for &(tag, body) in cases {
        let mut buf = frame(tag, body);
        buf.extend_from_slice(&next);
        assert!(
            PgWireBackendMessage::decode(&mut buf, &ctx).is_err(),
            "tag {tag}, body {body:?} borrowed bytes from the next frame"
        );
        assert_eq!(buf, next, "tag {tag}, body {body:?}");
        assert!(matches!(
            PgWireBackendMessage::decode(&mut buf, &ctx).unwrap(),
            Some(PgWireBackendMessage::ReadyForQuery(_))
        ));
        assert!(buf.is_empty());
    }
}

#[test]
fn cstring_reads_cannot_cross_frame_boundaries() {
    let ctx = frontend_ctx();
    let next = frame(b'Q', b"SELECT 2\0");
    // Query currently accepts an unterminated string as an empty query. Frame
    // isolation must not let a terminator in the next header change that result.
    let packet = frame(b'Q', b"SELECT 1");
    let mut standalone = packet.clone();
    let expected = PgWireFrontendMessage::decode(&mut standalone, &ctx).unwrap();
    let mut pipelined = packet;
    pipelined.extend_from_slice(&next);
    let actual = PgWireFrontendMessage::decode(&mut pipelined, &ctx).unwrap();
    let Some(PgWireFrontendMessage::Query(expected)) = expected else {
        panic!("expected a Query");
    };
    let Some(PgWireFrontendMessage::Query(actual)) = actual else {
        panic!("expected a Query");
    };
    assert_eq!(actual, expected);
    assert_eq!(pipelined, next);
    assert!(standalone.is_empty());
}

#[test]
fn valid_frontend_pipeline_decodes_one_frame_at_a_time() {
    let ctx = frontend_ctx();
    let messages = [
        PgWireFrontendMessage::Parse(Parse::new(None, "SELECT $1".to_owned(), vec![25])),
        PgWireFrontendMessage::Bind(Bind::new(None, None, vec![], vec![None], vec![])),
        PgWireFrontendMessage::Execute(Execute::new(None, 0)),
        PgWireFrontendMessage::Query(Query::new("SELECT 2".to_owned())),
    ];
    let mut pipeline = BytesMut::new();
    let mut frames = Vec::new();
    for message in &messages {
        let mut encoded = BytesMut::new();
        message.encode(&mut encoded).unwrap();
        pipeline.extend_from_slice(&encoded);
        frames.push(encoded);
    }
    for expected in frames {
        let decoded = PgWireFrontendMessage::decode(&mut pipeline, &ctx)
            .unwrap()
            .unwrap();
        let mut encoded = BytesMut::new();
        decoded.encode(&mut encoded).unwrap();
        assert_eq!(encoded, expected);
    }
    assert!(pipeline.is_empty());
}

#[test]
fn valid_backend_pipeline_decodes_one_frame_at_a_time() {
    let ctx = DecodeContext::default();
    let frame = frame(b'Z', b"I");
    let mut pipeline = frame.clone();
    pipeline.extend_from_slice(&frame);
    for _ in 0..2 {
        let decoded = ReadyForQuery::decode(&mut pipeline, &ctx).unwrap().unwrap();
        assert_eq!(decoded.status, TransactionStatus::Idle);
    }
    assert!(pipeline.is_empty());
}

#[test]
fn incomplete_frames_do_not_consume_any_bytes() {
    let ctx = frontend_ctx();
    let complete = frame(b'Q', b"SELECT 1\0");
    for len in 0..complete.len() {
        let mut buf = BytesMut::from(&complete[..len]);
        let original = buf.clone();
        assert!(
            PgWireFrontendMessage::decode(&mut buf, &ctx)
                .unwrap()
                .is_none()
        );
        assert_eq!(buf, original);
        buf.extend_from_slice(&complete[len..]);
        assert!(
            PgWireFrontendMessage::decode(&mut buf, &ctx)
                .unwrap()
                .is_some()
        );
        assert!(buf.is_empty());
    }
}

#[test]
fn startup_is_isolated_from_the_next_frame() {
    let ctx = DecodeContext::default();
    // Startup without the final empty parameter key. A decoder using the whole
    // receive buffer would interpret the next frame's tag as another key.
    let body = b"\0\x03\0\0user\0postgres\0";
    let mut buf = BytesMut::new();
    buf.put_i32((4 + body.len()) as i32);
    buf.extend_from_slice(body);
    let next = frame(b'Q', b"SELECT 1\0");
    buf.extend_from_slice(&next);
    let startup = Startup::decode(&mut buf, &ctx).unwrap().unwrap();
    assert_eq!(startup.parameters.len(), 1);
    assert_eq!(startup.parameters.get("user").unwrap(), "postgres");
    assert_eq!(buf, next);
}
