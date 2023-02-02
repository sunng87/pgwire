use bytes::{Buf, BufMut, BytesMut};

use crate::error::{PgWireError, PgWireResult};

pub trait Message: Sized {
    /// Return the type code of the message. In order to maintain backward
    /// compatibility, `Startup` has no message type.
    #[inline]
    fn message_type() -> Option<u8> {
        None
    }

    /// Return the length of the message, including the length integer itself.
    fn message_length(&self) -> usize;

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()>;

    fn decode_body(buf: &mut BytesMut, full_len: usize) -> PgWireResult<Self>;

    fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        if let Some(mt) = Self::message_type() {
            buf.put_u8(mt);
        }

        buf.put_i32(self.message_length() as i32);
        self.encode_body(buf)
    }

    fn decode(buf: &mut BytesMut) -> PgWireResult<Option<Self>> {
        let offset = Self::message_type().is_some().into();

        codec::decode_packet(buf, offset, |buf, full_len| {
            Self::decode_body(buf, full_len)
        })
    }
}

mod codec;
pub mod data;
pub mod extendedquery;
pub mod response;
pub mod simplequery;
pub mod startup;
pub mod terminate;

/// Messages sent from Frontend
#[derive(Debug)]
pub enum PgWireFrontendMessage {
    Startup(startup::Startup),
    PasswordMessageFamily(startup::PasswordMessageFamily),
    Password(startup::Password),
    SASLInitialResponse(startup::SASLInitialResponse),
    SASLResponse(startup::SASLResponse),

    Query(simplequery::Query),

    Parse(extendedquery::Parse),
    Close(extendedquery::Close),
    Bind(extendedquery::Bind),
    Describe(extendedquery::Describe),
    Execute(extendedquery::Execute),
    Flush(extendedquery::Flush),
    Sync(extendedquery::Sync),

    Terminate(terminate::Terminate),
}

impl PgWireFrontendMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        match self {
            Self::Startup(msg) => msg.encode(buf),
            Self::Password(msg) => msg.encode(buf),
            Self::SASLInitialResponse(msg) => msg.encode(buf),
            Self::SASLResponse(msg) => msg.encode(buf),
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
        }
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Option<Self>> {
        if buf.remaining() > 1 {
            let first_byte = buf[0];
            match first_byte {
                // Password, SASLInitialResponse, SASLResponse can only be
                // decoded under certain context
                startup::MESSAGE_TYPE_BYTE_PASWORD_MESSAGE_FAMILY => {
                    startup::PasswordMessageFamily::decode(buf)
                        .map(|v| v.map(Self::PasswordMessageFamily))
                }

                simplequery::MESSAGE_TYPE_BYTE_QUERY => {
                    simplequery::Query::decode(buf).map(|v| v.map(Self::Query))
                }

                extendedquery::MESSAGE_TYPE_BYTE_PARSE => {
                    extendedquery::Parse::decode(buf).map(|v| v.map(Self::Parse))
                }
                extendedquery::MESSAGE_TYPE_BYTE_BIND => {
                    extendedquery::Bind::decode(buf).map(|v| v.map(Self::Bind))
                }
                extendedquery::MESSAGE_TYPE_BYTE_CLOSE => {
                    extendedquery::Close::decode(buf).map(|v| v.map(Self::Close))
                }
                extendedquery::MESSAGE_TYPE_BYTE_DESCRIBE => {
                    extendedquery::Describe::decode(buf).map(|v| v.map(Self::Describe))
                }
                extendedquery::MESSAGE_TYPE_BYTE_EXECUTE => {
                    extendedquery::Execute::decode(buf).map(|v| v.map(Self::Execute))
                }
                extendedquery::MESSAGE_TYPE_BYTE_SYNC => {
                    extendedquery::Sync::decode(buf).map(|v| v.map(Self::Sync))
                }

                terminate::MESSAGE_TYPE_BYTE_TERMINATE => {
                    terminate::Terminate::decode(buf).map(|v| v.map(Self::Terminate))
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

    // data
    ParameterDescription(data::ParameterDescription),
    RowDescription(data::RowDescription),
    DataRow(data::DataRow),
}

impl PgWireBackendMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        match self {
            Self::Authentication(msg) => msg.encode(buf),
            Self::ParameterStatus(msg) => msg.encode(buf),
            Self::BackendKeyData(msg) => msg.encode(buf),

            Self::ParseComplete(msg) => msg.encode(buf),
            Self::BindComplete(msg) => msg.encode(buf),
            Self::CloseComplete(msg) => msg.encode(buf),
            Self::PortalSuspended(msg) => msg.encode(buf),

            Self::CommandComplete(msg) => msg.encode(buf),
            Self::EmptyQueryResponse(msg) => msg.encode(buf),
            Self::ReadyForQuery(msg) => msg.encode(buf),
            Self::ErrorResponse(msg) => msg.encode(buf),
            Self::NoticeResponse(msg) => msg.encode(buf),

            Self::ParameterDescription(msg) => msg.encode(buf),
            Self::RowDescription(msg) => msg.encode(buf),
            Self::DataRow(msg) => msg.encode(buf),
        }
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Option<Self>> {
        if buf.remaining() > 1 {
            let first_byte = buf[0];
            match first_byte {
                startup::MESSAGE_TYPE_BYTE_AUTHENTICATION => {
                    startup::Authentication::decode(buf).map(|v| v.map(Self::Authentication))
                }
                startup::MESSAGE_TYPE_BYTE_PARAMETER_STATUS => {
                    startup::ParameterStatus::decode(buf).map(|v| v.map(Self::ParameterStatus))
                }
                startup::MESSAGE_TYPE_BYTE_BACKEND_KEY_DATA => {
                    startup::BackendKeyData::decode(buf).map(|v| v.map(Self::BackendKeyData))
                }

                extendedquery::MESSAGE_TYPE_BYTE_PARSE_COMPLETE => {
                    extendedquery::ParseComplete::decode(buf).map(|v| v.map(Self::ParseComplete))
                }
                extendedquery::MESSAGE_TYPE_BYTE_BIND_COMPLETE => {
                    extendedquery::BindComplete::decode(buf).map(|v| v.map(Self::BindComplete))
                }
                extendedquery::MESSAGE_TYPE_BYTE_CLOSE_COMPLETE => {
                    extendedquery::CloseComplete::decode(buf).map(|v| v.map(Self::CloseComplete))
                }
                extendedquery::MESSAGE_TYPE_BYTE_PORTAL_SUSPENDED => {
                    extendedquery::PortalSuspended::decode(buf)
                        .map(|v| v.map(PgWireBackendMessage::PortalSuspended))
                }

                response::MESSAGE_TYPE_BYTE_COMMAND_COMPLETE => {
                    response::CommandComplete::decode(buf).map(|v| v.map(Self::CommandComplete))
                }
                response::MESSAGE_TYPE_BYTE_EMPTY_QUERY_RESPONSE => {
                    response::EmptyQueryResponse::decode(buf)
                        .map(|v| v.map(Self::EmptyQueryResponse))
                }
                response::MESSAGE_TYPE_BYTE_READY_FOR_QUERY => {
                    response::ReadyForQuery::decode(buf).map(|v| v.map(Self::ReadyForQuery))
                }
                response::MESSAGE_TYPE_BYTE_ERROR_RESPONSE => {
                    response::ErrorResponse::decode(buf).map(|v| v.map(Self::ErrorResponse))
                }
                response::MESSAGE_TYPE_BYTE_NOTICE_RESPONSE => {
                    response::NoticeResponse::decode(buf).map(|v| v.map(Self::NoticeResponse))
                }

                data::MESSAGE_TYPE_BYTE_PARAMETER_DESCRITION => {
                    data::ParameterDescription::decode(buf)
                        .map(|v| v.map(Self::ParameterDescription))
                }
                data::MESSAGE_TYPE_BYTE_ROW_DESCRITION => {
                    data::RowDescription::decode(buf).map(|v| v.map(Self::RowDescription))
                }
                data::MESSAGE_TYPE_BYTE_DATA_ROW => {
                    data::DataRow::decode(buf).map(|v| v.map(Self::DataRow))
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
    use super::data::*;
    use super::extendedquery::*;
    use super::response::*;
    use super::simplequery::*;
    use super::startup::*;
    use super::terminate::*;
    use super::Message;
    use bytes::{Buf, Bytes, BytesMut};

    macro_rules! roundtrip {
        ($ins:ident, $st:ty) => {
            let mut buffer = BytesMut::new();
            $ins.encode(&mut buffer).unwrap();

            assert!(buffer.remaining() > 0);

            let item2 = <$st>::decode(&mut buffer).unwrap().unwrap();

            assert_eq!(buffer.remaining(), 0);
            assert_eq!($ins, item2);
        };
    }

    #[test]
    fn test_startup() {
        let mut s = Startup::default();
        s.parameters_mut()
            .insert("user".to_owned(), "tomcat".to_owned());

        roundtrip!(s, Startup);
    }

    #[test]
    fn test_authentication() {
        let ss = vec![
            Authentication::Ok,
            Authentication::CleartextPassword,
            Authentication::KerberosV5,
        ];
        for s in ss {
            roundtrip!(s, Authentication);
        }

        let md5pass = Authentication::MD5Password(vec![b'p', b's', b't', b'g']);
        roundtrip!(md5pass, Authentication);
    }

    #[test]
    fn test_password() {
        let s = Password::new("pgwire".to_owned());
        roundtrip!(s, Password);
    }

    #[test]
    fn test_parameter_status() {
        let pps = ParameterStatus::new("cli".to_owned(), "psql".to_owned());
        roundtrip!(pps, ParameterStatus);
    }

    #[test]
    fn test_query() {
        let query = Query::new("SELECT 1".to_owned());
        roundtrip!(query, Query);
    }

    #[test]
    fn test_command_complete() {
        let cc = CommandComplete::new("DELETE 5".to_owned());
        roundtrip!(cc, CommandComplete);
    }

    #[test]
    fn test_ready_for_query() {
        let r4q = ReadyForQuery::new(b'I');
        roundtrip!(r4q, ReadyForQuery);
    }

    #[test]
    fn test_error_response() {
        let mut error = ErrorResponse::default();
        error.fields_mut().push((b'R', "ERROR".to_owned()));
        error.fields_mut().push((b'K', "cli".to_owned()));

        roundtrip!(error, ErrorResponse);
    }

    #[test]
    fn test_notice_response() {
        let mut error = NoticeResponse::default();
        error.fields_mut().push((b'R', "NOTICE".to_owned()));
        error.fields_mut().push((b'K', "cli".to_owned()));

        roundtrip!(error, NoticeResponse);
    }

    #[test]
    fn test_row_description() {
        let mut row_description = RowDescription::default();

        let mut f1 = FieldDescription::default();
        f1.set_name("id".into());
        f1.set_table_id(1001);
        f1.set_column_id(10001);
        f1.set_type_id(1083);
        f1.set_type_size(4);
        f1.set_type_modifier(-1);
        f1.set_format_code(FORMAT_CODE_TEXT);
        row_description.fields_mut().push(f1);

        let mut f2 = FieldDescription::default();
        f2.set_name("name".into());
        f2.set_table_id(1001);
        f2.set_column_id(10001);
        f2.set_type_id(1099);
        f2.set_type_size(-1);
        f2.set_type_modifier(-1);
        f2.set_format_code(FORMAT_CODE_TEXT);
        row_description.fields_mut().push(f2);

        roundtrip!(row_description, RowDescription);
    }

    #[test]
    fn test_data_row() {
        let mut row0 = DataRow::default();
        row0.fields_mut().push(Some(Bytes::from_static(b"1")));
        row0.fields_mut().push(Some(Bytes::from_static(b"abc")));
        row0.fields_mut().push(None);

        roundtrip!(row0, DataRow);
    }

    #[test]
    fn test_terminate() {
        let terminate = Terminate::new();
        roundtrip!(terminate, Terminate);
    }

    #[test]
    fn test_parse() {
        let parse = Parse::new(
            Some("find-user-by-id".to_owned()),
            "SELECT * FROM user WHERE id = ?".to_owned(),
            vec![1],
        );
        roundtrip!(parse, Parse);
    }

    #[test]
    fn test_parse_complete() {
        let parse_complete = ParseComplete::new();
        roundtrip!(parse_complete, ParseComplete);
    }

    #[test]
    fn test_close() {
        let close = Close::new(
            TARGET_TYPE_BYTE_STATEMENT,
            Some("find-user-by-id".to_owned()),
        );
        roundtrip!(close, Close);
    }

    #[test]
    fn test_bind() {
        let bind = Bind::new(
            Some("find-user-by-id-0".to_owned()),
            Some("find-user-by-id".to_owned()),
            vec![0],
            vec![Some(Bytes::from_static(b"1234"))],
            vec![0],
        );
        roundtrip!(bind, Bind);
    }

    #[test]
    fn test_execute() {
        let exec = Execute::new(Some("find-user-by-id-0".to_owned()), 100);
        roundtrip!(exec, Execute);
    }

    #[test]
    fn test_sslrequest() {
        let sslreq = SslRequest::new();
        roundtrip!(sslreq, SslRequest);
    }

    #[test]
    fn test_saslresponse() {
        let saslinitialresp =
            SASLInitialResponse::new("SCRAM-SHA-256".to_owned(), Some(Bytes::from_static(b"abc")));
        roundtrip!(saslinitialresp, SASLInitialResponse);

        let saslresp = SASLResponse::new(Bytes::from_static(b"abc"));
        roundtrip!(saslresp, SASLResponse);
    }

    #[test]
    fn test_parameter_description() {
        let param_desc = ParameterDescription::new(vec![100, 200]);
        roundtrip!(param_desc, ParameterDescription);
    }

    #[test]
    fn test_password_family() {
        let password = Password::new("tomcat".to_owned());

        let mut buffer = BytesMut::new();
        password.encode(&mut buffer).unwrap();
        assert!(buffer.remaining() > 0);

        let item2 = PasswordMessageFamily::decode(&mut buffer).unwrap().unwrap();
        assert_eq!(buffer.remaining(), 0);
        assert_eq!(password, item2.into_password().unwrap());

        let saslinitialresp =
            SASLInitialResponse::new("SCRAM-SHA-256".to_owned(), Some(Bytes::from_static(b"abc")));
        let mut buffer = BytesMut::new();
        saslinitialresp.encode(&mut buffer).unwrap();
        assert!(buffer.remaining() > 0);

        let item2 = PasswordMessageFamily::decode(&mut buffer).unwrap().unwrap();
        assert_eq!(buffer.remaining(), 0);
        assert_eq!(saslinitialresp, item2.into_sasl_initial_response().unwrap());
    }
}
