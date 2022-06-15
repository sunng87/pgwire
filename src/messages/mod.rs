use std::io;

use bytes::{BufMut, BytesMut};

pub(crate) trait Message: Sized {
    /// Return the type code of the message. In order to maintain backward
    /// compatibility, `Startup` has no message type.
    #[inline]
    fn message_type() -> Option<u8> {
        None
    }

    /// Return the length of the message, including the length integer itself.
    fn message_length(&self) -> i32;
    fn encode_body(&self, buf: &mut BytesMut) -> io::Result<()>;
    fn decode_body(buf: &mut BytesMut) -> io::Result<Self>;

    fn encode(&self, buf: &mut BytesMut) -> io::Result<()> {
        if let Some(mt) = Self::message_type() {
            buf.put_u8(mt);
        }

        buf.put_i32(self.message_length());
        self.encode_body(buf)
    }

    fn decode(buf: &mut BytesMut) -> io::Result<Option<Self>> {
        if let Some(mt) = Self::message_type() {
            codec::get_and_ensure_message_type(buf, mt)?;
        }

        codec::decode_packet(buf, |buf, _| Self::decode_body(buf))
    }
}

mod codec;
mod response;
mod simplequery;
mod startup;

pub enum PgWireMessage {
    Startup(startup::Startup),
    Authentication(startup::Authentication),
    Password(startup::Password),
}

#[cfg(test)]
mod test {
    use super::response::*;
    use super::simplequery::Query;
    use super::startup::*;
    use super::Message;
    use bytes::{Buf, BytesMut};

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
}
