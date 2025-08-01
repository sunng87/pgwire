//! `messages` module contains postgresql wire protocol message definitions and
//! codecs.
//!
//! `PgWireFrontendMessage` and `PgWireBackendMessage` are enums that define all
//! types of supported messages. `Message` trait allows you to encode/decode
//! them on a `BytesMut` buffer.

use bytes::{Buf, BufMut, BytesMut};

use crate::error::{PgWireError, PgWireResult};

#[derive(Default, Debug, PartialEq, Eq, Clone, Copy)]
pub enum ProtocolVersion {
    PROTOCOL3_0,
    #[default]
    PROTOCOL3_2,
}

impl ProtocolVersion {
    pub fn version_number(&self) -> (u16, u16) {
        match &self {
            Self::PROTOCOL3_0 => (3, 0),
            Self::PROTOCOL3_2 => (3, 2),
        }
    }

    /// Get ProtocolVersion from (major, minor) version tuple
    ///
    /// Return none if protocol is not supported.
    pub fn from_version_number(major: u16, minor: u16) -> Option<Self> {
        match (major, minor) {
            (3, 0) => Some(Self::PROTOCOL3_0),
            (3, 2) => Some(Self::PROTOCOL3_2),
            _ => None,
        }
    }
}

#[non_exhaustive]
#[derive(Default, Debug, PartialEq, Eq, new)]
pub struct DecodeContext {
    pub protocol_version: ProtocolVersion,
    #[new(value = "true")]
    pub awaiting_ssl: bool,
    #[new(value = "true")]
    pub awaiting_startup: bool,
}

/// Define how message encode and decoded.
pub trait Message: Sized {
    /// Return the type code of the message. In order to maintain backward
    /// compatibility, `Startup` has no message type.
    #[inline]
    fn message_type() -> Option<u8> {
        None
    }

    /// Return the length of the message, including the length integer itself.
    fn message_length(&self) -> usize;

    /// Encode body part of the message.
    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()>;

    /// Decode body part of the message.
    fn decode_body(buf: &mut BytesMut, full_len: usize, _ctx: &DecodeContext)
        -> PgWireResult<Self>;

    /// Default implementation for encoding message.
    ///
    /// Message type and length are encoded in this implementation and it calls
    /// `encode_body` for remaining parts.
    fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        if let Some(mt) = Self::message_type() {
            buf.put_u8(mt);
        }

        buf.put_i32(self.message_length() as i32);
        self.encode_body(buf)
    }

    /// Default implementation for decoding message.
    ///
    /// Message type and length are decoded in this implementation and it calls
    /// `decode_body` for remaining parts. Return `None` if the packet is not
    /// complete for parsing.
    fn decode(buf: &mut BytesMut, ctx: &DecodeContext) -> PgWireResult<Option<Self>> {
        let offset = Self::message_type().is_some().into();

        codec::decode_packet(buf, offset, |buf, full_len| {
            Self::decode_body(buf, full_len, ctx)
        })
    }
}

/// Cancel message
pub mod cancel;
mod codec;
/// Copy messages
pub mod copy;
/// Data related messages
pub mod data;
/// Extended query messages, including request/response for parse, bind and etc.
pub mod extendedquery;
/// General response messages
pub mod response;
/// Simple query messages, including descriptions
pub mod simplequery;
/// Startup messages
pub mod startup;
/// Termination messages
pub mod terminate;

/// Messages sent from Frontend
#[derive(Debug)]
pub enum PgWireFrontendMessage {
    Startup(startup::Startup),
    CancelRequest(cancel::CancelRequest),
    SslRequest(startup::SslRequest),
    PasswordMessageFamily(startup::PasswordMessageFamily),

    Query(simplequery::Query),

    Parse(extendedquery::Parse),
    Close(extendedquery::Close),
    Bind(extendedquery::Bind),
    Describe(extendedquery::Describe),
    Execute(extendedquery::Execute),
    Flush(extendedquery::Flush),
    Sync(extendedquery::Sync),

    Terminate(terminate::Terminate),

    CopyData(copy::CopyData),
    CopyFail(copy::CopyFail),
    CopyDone(copy::CopyDone),
}

impl PgWireFrontendMessage {
    pub fn is_extended_query(&self) -> bool {
        matches!(
            self,
            Self::Parse(_)
                | Self::Bind(_)
                | Self::Close(_)
                | Self::Describe(_)
                | Self::Execute(_)
                | Self::Flush(_)
                | Self::Sync(_)
        )
    }

    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        match self {
            Self::Startup(msg) => msg.encode(buf),
            Self::CancelRequest(msg) => msg.encode(buf),
            Self::SslRequest(msg) => msg.encode(buf),

            Self::PasswordMessageFamily(msg) => msg.encode(buf),

            Self::Query(msg) => msg.encode(buf),

            Self::Parse(msg) => msg.encode(buf),
            Self::Bind(msg) => msg.encode(buf),
            Self::Close(msg) => msg.encode(buf),
            Self::Describe(msg) => msg.encode(buf),
            Self::Execute(msg) => msg.encode(buf),
            Self::Flush(msg) => msg.encode(buf),
            Self::Sync(msg) => msg.encode(buf),

            Self::Terminate(msg) => msg.encode(buf),

            Self::CopyData(msg) => msg.encode(buf),
            Self::CopyFail(msg) => msg.encode(buf),
            Self::CopyDone(msg) => msg.encode(buf),
        }
    }

    pub fn decode(buf: &mut BytesMut, ctx: &DecodeContext) -> PgWireResult<Option<Self>> {
        if ctx.awaiting_ssl {
            // Connection just estabilished, the incoming message can be:
            // - SSLRequest
            // - Startup
            // - CancelRequest
            // Try to read the magic number to tell whether it SSLRequest or
            // Startup, all these messages should have at least 8 bytes
            if buf.remaining() >= 8 {
                if cancel::CancelRequest::is_cancel_request_packet(buf) {
                    cancel::CancelRequest::decode(buf, ctx)
                        .map(|opt| opt.map(PgWireFrontendMessage::CancelRequest))
                } else if startup::SslRequest::is_ssl_request_packet(buf) {
                    startup::SslRequest::decode(buf, ctx)
                        .map(|opt| opt.map(PgWireFrontendMessage::SslRequest))
                } else {
                    // startup
                    startup::Startup::decode(buf, ctx).map(|v| v.map(Self::Startup))
                }
            } else {
                Ok(None)
            }
        } else if ctx.awaiting_startup {
            // we will check for cancel request again in case it's sent in ssl connection
            if buf.remaining() >= 8 {
                if cancel::CancelRequest::is_cancel_request_packet(buf) {
                    cancel::CancelRequest::decode(buf, ctx)
                        .map(|opt| opt.map(PgWireFrontendMessage::CancelRequest))
                } else {
                    startup::Startup::decode(buf, ctx).map(|v| v.map(Self::Startup))
                }
            } else {
                Ok(None)
            }
        } else if buf.remaining() > 1 {
            let first_byte = buf[0];

            match first_byte {
                // Password, SASLInitialResponse, SASLResponse can only be
                // decoded under certain context
                startup::MESSAGE_TYPE_BYTE_PASWORD_MESSAGE_FAMILY => {
                    startup::PasswordMessageFamily::decode(buf, ctx)
                        .map(|v| v.map(Self::PasswordMessageFamily))
                }

                simplequery::MESSAGE_TYPE_BYTE_QUERY => {
                    simplequery::Query::decode(buf, ctx).map(|v| v.map(Self::Query))
                }

                extendedquery::MESSAGE_TYPE_BYTE_PARSE => {
                    extendedquery::Parse::decode(buf, ctx).map(|v| v.map(Self::Parse))
                }
                extendedquery::MESSAGE_TYPE_BYTE_BIND => {
                    extendedquery::Bind::decode(buf, ctx).map(|v| v.map(Self::Bind))
                }
                extendedquery::MESSAGE_TYPE_BYTE_CLOSE => {
                    extendedquery::Close::decode(buf, ctx).map(|v| v.map(Self::Close))
                }
                extendedquery::MESSAGE_TYPE_BYTE_DESCRIBE => {
                    extendedquery::Describe::decode(buf, ctx).map(|v| v.map(Self::Describe))
                }
                extendedquery::MESSAGE_TYPE_BYTE_EXECUTE => {
                    extendedquery::Execute::decode(buf, ctx).map(|v| v.map(Self::Execute))
                }
                extendedquery::MESSAGE_TYPE_BYTE_FLUSH => {
                    extendedquery::Flush::decode(buf, ctx).map(|v| v.map(Self::Flush))
                }
                extendedquery::MESSAGE_TYPE_BYTE_SYNC => {
                    extendedquery::Sync::decode(buf, ctx).map(|v| v.map(Self::Sync))
                }

                terminate::MESSAGE_TYPE_BYTE_TERMINATE => {
                    terminate::Terminate::decode(buf, ctx).map(|v| v.map(Self::Terminate))
                }

                copy::MESSAGE_TYPE_BYTE_COPY_DATA => {
                    copy::CopyData::decode(buf, ctx).map(|v| v.map(Self::CopyData))
                }
                copy::MESSAGE_TYPE_BYTE_COPY_FAIL => {
                    copy::CopyFail::decode(buf, ctx).map(|v| v.map(Self::CopyFail))
                }
                copy::MESSAGE_TYPE_BYTE_COPY_DONE => {
                    copy::CopyDone::decode(buf, ctx).map(|v| v.map(Self::CopyDone))
                }
                _ => Err(PgWireError::InvalidMessageType(first_byte)),
            }
        } else {
            Ok(None)
        }
    }
}

/// Messages sent from Backend
#[derive(Debug)]
pub enum PgWireBackendMessage {
    // startup
    Authentication(startup::Authentication),
    ParameterStatus(startup::ParameterStatus),
    BackendKeyData(startup::BackendKeyData),
    NegotiateProtocolVersion(startup::NegotiateProtocolVersion),

    // extended query
    ParseComplete(extendedquery::ParseComplete),
    CloseComplete(extendedquery::CloseComplete),
    BindComplete(extendedquery::BindComplete),
    PortalSuspended(extendedquery::PortalSuspended),

    // command response
    CommandComplete(response::CommandComplete),
    EmptyQueryResponse(response::EmptyQueryResponse),
    ReadyForQuery(response::ReadyForQuery),
    ErrorResponse(response::ErrorResponse),
    NoticeResponse(response::NoticeResponse),
    SslResponse(response::SslResponse),
    NotificationResponse(response::NotificationResponse),

    // data
    ParameterDescription(data::ParameterDescription),
    RowDescription(data::RowDescription),
    DataRow(data::DataRow),
    NoData(data::NoData),

    // copy
    CopyData(copy::CopyData),
    CopyFail(copy::CopyFail),
    CopyDone(copy::CopyDone),
    CopyInResponse(copy::CopyInResponse),
    CopyOutResponse(copy::CopyOutResponse),
    CopyBothResponse(copy::CopyBothResponse),
}

impl PgWireBackendMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        match self {
            Self::Authentication(msg) => msg.encode(buf),
            Self::ParameterStatus(msg) => msg.encode(buf),
            Self::BackendKeyData(msg) => msg.encode(buf),
            Self::NegotiateProtocolVersion(msg) => msg.encode(buf),

            Self::ParseComplete(msg) => msg.encode(buf),
            Self::BindComplete(msg) => msg.encode(buf),
            Self::CloseComplete(msg) => msg.encode(buf),
            Self::PortalSuspended(msg) => msg.encode(buf),

            Self::CommandComplete(msg) => msg.encode(buf),
            Self::EmptyQueryResponse(msg) => msg.encode(buf),
            Self::ReadyForQuery(msg) => msg.encode(buf),
            Self::ErrorResponse(msg) => msg.encode(buf),
            Self::NoticeResponse(msg) => msg.encode(buf),
            Self::SslResponse(msg) => msg.encode(buf),
            Self::NotificationResponse(msg) => msg.encode(buf),

            Self::ParameterDescription(msg) => msg.encode(buf),
            Self::RowDescription(msg) => msg.encode(buf),
            Self::DataRow(msg) => msg.encode(buf),
            Self::NoData(msg) => msg.encode(buf),

            Self::CopyData(msg) => msg.encode(buf),
            Self::CopyFail(msg) => msg.encode(buf),
            Self::CopyDone(msg) => msg.encode(buf),
            Self::CopyInResponse(msg) => msg.encode(buf),
            Self::CopyOutResponse(msg) => msg.encode(buf),
            Self::CopyBothResponse(msg) => msg.encode(buf),
        }
    }

    pub fn decode(buf: &mut BytesMut, ctx: &DecodeContext) -> PgWireResult<Option<Self>> {
        if buf.remaining() > 1 {
            let first_byte = buf[0];
            match first_byte {
                startup::MESSAGE_TYPE_BYTE_AUTHENTICATION => {
                    startup::Authentication::decode(buf, ctx).map(|v| v.map(Self::Authentication))
                }
                startup::MESSAGE_TYPE_BYTE_PARAMETER_STATUS => {
                    startup::ParameterStatus::decode(buf, ctx).map(|v| v.map(Self::ParameterStatus))
                }
                startup::MESSAGE_TYPE_BYTE_BACKEND_KEY_DATA => {
                    startup::BackendKeyData::decode(buf, ctx).map(|v| v.map(Self::BackendKeyData))
                }
                startup::MESSAGE_TYPE_BYTE_NEGOTIATE_PROTOCOL_VERSION => {
                    startup::NegotiateProtocolVersion::decode(buf, ctx)
                        .map(|v| v.map(Self::NegotiateProtocolVersion))
                }

                extendedquery::MESSAGE_TYPE_BYTE_PARSE_COMPLETE => {
                    extendedquery::ParseComplete::decode(buf, ctx)
                        .map(|v| v.map(Self::ParseComplete))
                }
                extendedquery::MESSAGE_TYPE_BYTE_BIND_COMPLETE => {
                    extendedquery::BindComplete::decode(buf, ctx).map(|v| v.map(Self::BindComplete))
                }
                extendedquery::MESSAGE_TYPE_BYTE_CLOSE_COMPLETE => {
                    extendedquery::CloseComplete::decode(buf, ctx)
                        .map(|v| v.map(Self::CloseComplete))
                }
                extendedquery::MESSAGE_TYPE_BYTE_PORTAL_SUSPENDED => {
                    extendedquery::PortalSuspended::decode(buf, ctx)
                        .map(|v| v.map(PgWireBackendMessage::PortalSuspended))
                }

                response::MESSAGE_TYPE_BYTE_COMMAND_COMPLETE => {
                    response::CommandComplete::decode(buf, ctx)
                        .map(|v| v.map(Self::CommandComplete))
                }
                response::MESSAGE_TYPE_BYTE_EMPTY_QUERY_RESPONSE => {
                    response::EmptyQueryResponse::decode(buf, ctx)
                        .map(|v| v.map(Self::EmptyQueryResponse))
                }
                response::MESSAGE_TYPE_BYTE_READY_FOR_QUERY => {
                    response::ReadyForQuery::decode(buf, ctx).map(|v| v.map(Self::ReadyForQuery))
                }
                response::MESSAGE_TYPE_BYTE_ERROR_RESPONSE => {
                    response::ErrorResponse::decode(buf, ctx).map(|v| v.map(Self::ErrorResponse))
                }
                response::MESSAGE_TYPE_BYTE_NOTICE_RESPONSE => {
                    response::NoticeResponse::decode(buf, ctx).map(|v| v.map(Self::NoticeResponse))
                }
                response::MESSAGE_TYPE_BYTE_NOTIFICATION_RESPONSE => {
                    response::NotificationResponse::decode(buf, ctx)
                        .map(|v| v.map(Self::NotificationResponse))
                }

                data::MESSAGE_TYPE_BYTE_PARAMETER_DESCRITION => {
                    data::ParameterDescription::decode(buf, ctx)
                        .map(|v| v.map(Self::ParameterDescription))
                }
                data::MESSAGE_TYPE_BYTE_ROW_DESCRITION => {
                    data::RowDescription::decode(buf, ctx).map(|v| v.map(Self::RowDescription))
                }
                data::MESSAGE_TYPE_BYTE_DATA_ROW => {
                    data::DataRow::decode(buf, ctx).map(|v| v.map(Self::DataRow))
                }
                data::MESSAGE_TYPE_BYTE_NO_DATA => {
                    data::NoData::decode(buf, ctx).map(|v| v.map(Self::NoData))
                }

                copy::MESSAGE_TYPE_BYTE_COPY_DATA => {
                    copy::CopyData::decode(buf, ctx).map(|v| v.map(Self::CopyData))
                }
                copy::MESSAGE_TYPE_BYTE_COPY_FAIL => {
                    copy::CopyFail::decode(buf, ctx).map(|v| v.map(Self::CopyFail))
                }
                copy::MESSAGE_TYPE_BYTE_COPY_DONE => {
                    copy::CopyDone::decode(buf, ctx).map(|v| v.map(Self::CopyDone))
                }
                copy::MESSAGE_TYPE_BYTE_COPY_IN_RESPONSE => {
                    copy::CopyInResponse::decode(buf, ctx).map(|v| v.map(Self::CopyInResponse))
                }
                copy::MESSAGE_TYPE_BYTE_COPY_OUT_RESPONSE => {
                    copy::CopyOutResponse::decode(buf, ctx).map(|v| v.map(Self::CopyOutResponse))
                }
                copy::MESSAGE_TYPE_BYTE_COPY_BOTH_RESPONSE => {
                    copy::CopyBothResponse::decode(buf, ctx).map(|v| v.map(Self::CopyBothResponse))
                }
                _ => Err(PgWireError::InvalidMessageType(first_byte)),
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::messages::DecodeContext;

    use super::cancel::CancelRequest;
    use super::copy::*;
    use super::data::*;
    use super::extendedquery::*;
    use super::response::*;
    use super::simplequery::*;
    use super::startup::*;
    use super::terminate::*;
    use super::{Message, ProtocolVersion};
    use bytes::{Buf, BufMut, Bytes, BytesMut};

    macro_rules! roundtrip {
        ($ins:ident, $st:ty, $ctx:expr) => {
            let mut buffer = BytesMut::new();
            $ins.encode(&mut buffer).expect("encode packet");

            assert!(buffer.remaining() > 0);

            let item2 = <$st>::decode(&mut buffer, $ctx)
                .expect("decode packet")
                .expect("packet is none");

            assert_eq!(buffer.remaining(), 0);
            assert_eq!($ins, item2);
        };
    }

    #[test]
    fn test_startup() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);

        let mut s = Startup::default();
        s.parameters.insert("user".to_owned(), "tomcat".to_owned());
        roundtrip!(s, Startup, &ctx);

        ctx.awaiting_ssl = false;
        roundtrip!(s, Startup, &ctx);
    }

    #[test]
    fn test_cancel_request() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_2);

        let s = CancelRequest::new(100, SecretKey::Bytes(Bytes::from("server2008")));
        roundtrip!(s, CancelRequest, &ctx);

        ctx.protocol_version = ProtocolVersion::PROTOCOL3_0;
        let s = CancelRequest::new(100, SecretKey::I32(1900));
        roundtrip!(s, CancelRequest, &ctx);
    }

    #[test]
    fn test_authentication() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let ss = vec![
            Authentication::Ok,
            Authentication::CleartextPassword,
            Authentication::KerberosV5,
            Authentication::SASLContinue(Bytes::from("hello")),
            Authentication::SASLFinal(Bytes::from("world")),
        ];
        for s in ss {
            roundtrip!(s, Authentication, &ctx);
        }

        let md5pass = Authentication::MD5Password(vec![b'p', b's', b't', b'g']);
        roundtrip!(md5pass, Authentication, &ctx);
    }

    #[test]
    fn test_password() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let s = Password::new("pgwire".to_owned());
        roundtrip!(s, Password, &ctx);
    }

    #[test]
    fn test_parameter_status() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let pps = ParameterStatus::new("cli".to_owned(), "psql".to_owned());
        roundtrip!(pps, ParameterStatus, &ctx);
    }

    #[test]
    fn test_query() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let query = Query::new("SELECT 1".to_owned());
        roundtrip!(query, Query, &ctx);
    }

    #[test]
    fn test_command_complete() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let cc = CommandComplete::new("DELETE 5".to_owned());
        roundtrip!(cc, CommandComplete, &ctx);
    }

    #[test]
    fn test_ready_for_query() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let r4q = ReadyForQuery::new(TransactionStatus::Idle);
        roundtrip!(r4q, ReadyForQuery, &ctx);
        let r4q = ReadyForQuery::new(TransactionStatus::Transaction);
        roundtrip!(r4q, ReadyForQuery, &ctx);
        let r4q = ReadyForQuery::new(TransactionStatus::Error);
        roundtrip!(r4q, ReadyForQuery, &ctx);
    }

    #[test]
    fn test_error_response() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let mut error = ErrorResponse::default();
        error.fields.push((b'R', "ERROR".to_owned()));
        error.fields.push((b'K', "cli".to_owned()));

        roundtrip!(error, ErrorResponse, &ctx);
    }

    #[test]
    fn test_notice_response() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let mut error = NoticeResponse::default();
        error.fields.push((b'R', "NOTICE".to_owned()));
        error.fields.push((b'K', "cli".to_owned()));

        roundtrip!(error, NoticeResponse, &ctx);
    }

    #[test]
    fn test_row_description() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let mut row_description = RowDescription::default();

        let mut f1 = FieldDescription::default();
        f1.name = "id".into();
        f1.table_id = 1001;
        f1.column_id = 10001;
        f1.type_id = 1083;
        f1.type_size = 4;
        f1.type_modifier = -1;
        f1.format_code = FORMAT_CODE_TEXT;
        row_description.fields.push(f1);

        let mut f2 = FieldDescription::default();
        f2.name = "name".into();
        f2.table_id = 1001;
        f2.column_id = 10001;
        f2.type_id = 1099;
        f2.type_size = -1;
        f2.type_modifier = -1;
        f2.format_code = FORMAT_CODE_TEXT;
        row_description.fields.push(f2);

        roundtrip!(row_description, RowDescription, &ctx);
    }

    #[test]
    fn test_data_row() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let mut row0 = DataRow::default();
        row0.data.put_i32(4);
        row0.data.put_slice("data".as_bytes());
        row0.data.put_i32(4);
        row0.data.put_i32(1001);
        row0.data.put_i32(-1);

        roundtrip!(row0, DataRow, &ctx);
    }

    #[test]
    fn test_terminate() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let terminate = Terminate::new();
        roundtrip!(terminate, Terminate, &ctx);
    }

    #[test]
    fn test_parse() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let parse = Parse::new(
            Some("find-user-by-id".to_owned()),
            "SELECT * FROM user WHERE id = ?".to_owned(),
            vec![1],
        );
        roundtrip!(parse, Parse, &ctx);
    }

    #[test]
    fn test_parse_65k() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let parse = Parse::new(
            Some("many-params".to_owned()),
            "it won't be parsed anyway".to_owned(),
            vec![25; u16::MAX as usize],
        );
        roundtrip!(parse, Parse, &ctx);
    }

    #[test]
    fn test_parse_complete() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let parse_complete = ParseComplete::new();
        roundtrip!(parse_complete, ParseComplete, &ctx);
    }

    #[test]
    fn test_close() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let close = Close::new(
            TARGET_TYPE_BYTE_STATEMENT,
            Some("find-user-by-id".to_owned()),
        );
        roundtrip!(close, Close, &ctx);
    }

    #[test]
    fn test_bind() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;
        let bind = Bind::new(
            Some("find-user-by-id-0".to_owned()),
            Some("find-user-by-id".to_owned()),
            vec![0],
            vec![Some(Bytes::from_static(b"1234"))],
            vec![0],
        );
        roundtrip!(bind, Bind, &ctx);
    }

    #[test]
    fn test_bind_65k() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let bind = Bind::new(
            Some("lol".to_owned()),
            Some("kek".to_owned()),
            vec![0; u16::MAX as usize],
            vec![Some(Bytes::from_static(b"1234")); u16::MAX as usize],
            vec![0],
        );
        roundtrip!(bind, Bind, &ctx);
    }

    #[test]
    fn test_execute() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let exec = Execute::new(Some("find-user-by-id-0".to_owned()), 100);
        roundtrip!(exec, Execute, &ctx);
    }

    #[test]
    fn test_sslrequest() {
        let ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);

        let sslreq = SslRequest::new();
        roundtrip!(sslreq, SslRequest, &ctx);
    }

    #[test]
    fn test_sslresponse() {
        let ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);

        let sslaccept = SslResponse::Accept;
        roundtrip!(sslaccept, SslResponse, &ctx);
        let sslrefuse = SslResponse::Refuse;
        roundtrip!(sslrefuse, SslResponse, &ctx);
    }

    #[test]
    fn test_saslresponse() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let saslinitialresp =
            SASLInitialResponse::new("SCRAM-SHA-256".to_owned(), Some(Bytes::from_static(b"abc")));
        roundtrip!(saslinitialresp, SASLInitialResponse, &ctx);

        let saslresp = SASLResponse::new(Bytes::from_static(b"abc"));
        roundtrip!(saslresp, SASLResponse, &ctx);
    }

    #[test]
    fn test_parameter_description() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let param_desc = ParameterDescription::new(vec![100, 200]);
        roundtrip!(param_desc, ParameterDescription, &ctx);
    }

    #[test]
    fn test_password_family() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let password = Password::new("tomcat".to_owned());

        let mut buffer = BytesMut::new();
        password.encode(&mut buffer).unwrap();
        assert!(buffer.remaining() > 0);

        let item2 = PasswordMessageFamily::decode(&mut buffer, &ctx)
            .unwrap()
            .unwrap();
        assert_eq!(buffer.remaining(), 0);
        assert_eq!(password, item2.into_password().unwrap());

        let saslinitialresp =
            SASLInitialResponse::new("SCRAM-SHA-256".to_owned(), Some(Bytes::from_static(b"abc")));
        let mut buffer = BytesMut::new();
        saslinitialresp.encode(&mut buffer).unwrap();
        assert!(buffer.remaining() > 0);

        let item2 = PasswordMessageFamily::decode(&mut buffer, &ctx)
            .unwrap()
            .unwrap();
        assert_eq!(buffer.remaining(), 0);
        assert_eq!(saslinitialresp, item2.into_sasl_initial_response().unwrap());
    }

    #[test]
    fn test_no_data() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let nodata = NoData::new();
        roundtrip!(nodata, NoData, &ctx);
    }

    #[test]
    fn test_copy_data() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let copydata = CopyData::new(Bytes::from_static("tomcat".as_bytes()));
        roundtrip!(copydata, CopyData, &ctx);
    }

    #[test]
    fn test_copy_done() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let copydone = CopyDone::new();
        roundtrip!(copydone, CopyDone, &ctx);
    }

    #[test]
    fn test_copy_fail() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let copyfail = CopyFail::new("copy failed".to_owned());
        roundtrip!(copyfail, CopyFail, &ctx);
    }

    #[test]
    fn test_copy_response() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let copyresponse = CopyInResponse::new(0, 3, vec![0, 0, 0]);
        roundtrip!(copyresponse, CopyInResponse, &ctx);

        let copyresponse = CopyOutResponse::new(0, 3, vec![0, 0, 0]);
        roundtrip!(copyresponse, CopyOutResponse, &ctx);

        let copyresponse = CopyBothResponse::new(0, 3, vec![0, 0, 0]);
        roundtrip!(copyresponse, CopyBothResponse, &ctx);
    }

    #[test]
    fn test_notification_response() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let notification_response =
            NotificationResponse::new(10087, "channel".to_owned(), "payload".to_owned());
        roundtrip!(notification_response, NotificationResponse, &ctx);
    }

    #[test]
    fn test_negotiate_protocol_version() {
        let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
        ctx.awaiting_ssl = false;
        ctx.awaiting_startup = false;

        let negotiate_protocol_version =
            NegotiateProtocolVersion::new(2, vec!["database".to_owned(), "user".to_owned()]);
        roundtrip!(negotiate_protocol_version, NegotiateProtocolVersion, &ctx);
    }
}
