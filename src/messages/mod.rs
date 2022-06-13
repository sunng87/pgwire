use std::io;

use bytes::BytesMut;

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

mod codec;
mod response;
mod simplequery;
mod startup;

pub enum Message {
    Startup(startup::Startup),
    Authentication(startup::Authentication),
    Password(startup::Password),
}

#[cfg(test)]
mod test {
    use super::startup::*;
    use super::{Codec, Message};
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
}
