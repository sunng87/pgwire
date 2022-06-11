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
mod startup;

pub enum Message {
    Startup(startup::Startup),
    Authentication(startup::Authentication),
    Password(startup::Password),
}

#[cfg(test)]
mod test {
    use super::startup::Startup;
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
    fn test_roundtrip() {
        let mut s = Startup::default();
        s.parameters_mut()
            .insert("user".to_owned(), "tomcat".to_owned());

        roundtrip!(s, Startup);
    }
}
