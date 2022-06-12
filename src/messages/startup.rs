use std::collections::BTreeMap;

use bytes::{Buf, BufMut, BytesMut};

use super::codec;
use super::{Codec, MessageLength, MessageType};

/// postgresql wire protocol startup message, sent by frontend
/// the strings are null-ternimated string, which is a string
/// terminated by a zero byte.
/// the key-value parameter pairs are terminated by a zero byte, too.
///
#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct Startup {
    protocol_number_major: u16,
    protocol_number_minor: u16,

    parameters: BTreeMap<String, String>,
}

impl Default for Startup {
    fn default() -> Startup {
        Startup {
            protocol_number_major: 3,
            protocol_number_minor: 0,
            parameters: BTreeMap::default(),
        }
    }
}

impl MessageType for Startup {}
impl MessageLength for Startup {
    fn message_length(&self) -> i32 {
        let param_length: i32 = self
            .parameters
            .iter()
            .map(|(k, v)| k.as_bytes().len() + v.as_bytes().len() + 2)
            .sum::<usize>() as i32;
        // length:4 + protocol_number:4 + param.len + nullbyte:1
        9 + param_length
    }
}

impl Codec for Startup {
    fn encode(&self, buf: &mut BytesMut) -> std::io::Result<()> {
        buf.put_i32(self.message_length());

        // version number
        buf.put_u16(self.protocol_number_major);
        buf.put_u16(self.protocol_number_minor);

        // parameters
        for (k, v) in self.parameters.iter() {
            codec::put_cstring(buf, &k);
            codec::put_cstring(buf, &v);
        }
        // ends with empty cstring, a \0
        codec::put_cstring(buf, "");

        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> std::io::Result<Option<Self>> {
        codec::decode_packet(buf, |buf: &mut BytesMut, _| {
            let mut msg = Startup::default();
            // parse
            msg.set_protocol_number_major(buf.get_u16());
            msg.set_protocol_number_minor(buf.get_u16());

            // end by reading the last \0
            while let Some(key) = codec::get_cstring(buf) {
                let value = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());
                msg.parameters_mut().insert(key, value);
            }

            Ok(msg)
        })
    }
}

/// authentication response family, sent by backend
#[derive(PartialEq, Eq, Debug)]
pub enum Authentication {
    Ok,                // code 0
    CleartextPassword, // code 3
    KerberosV5,        // code 2
    MD5Password(Vec<u8>), // code 5, with 4 bytes of md5 salt

                       // TODO: more types
                       // AuthenticationSCMCredential
                       //
                       // AuthenticationGSS
                       // AuthenticationGSSContinue
                       // AuthenticationSSPI
                       // AuthenticationSASL
                       // AuthenticationSASLContinue
                       // AuthenticationSASLFinal
}

impl MessageType for Authentication {
    #[inline]
    fn message_type(&self) -> Option<u8> {
        Some(b'R')
    }
}

impl MessageLength for Authentication {
    #[inline]
    fn message_length(&self) -> i32 {
        match self {
            Authentication::Ok | Authentication::CleartextPassword | Authentication::KerberosV5 => {
                8
            }
            Authentication::MD5Password(_) => 12,
        }
    }
}

impl Codec for Authentication {
    fn encode(&self, buf: &mut BytesMut) -> std::io::Result<()> {
        buf.put_u8(self.message_type().unwrap());
        buf.put_i32(self.message_length());

        match self {
            Authentication::Ok => buf.put_i32(0),
            Authentication::CleartextPassword => buf.put_i32(3),
            Authentication::KerberosV5 => buf.put_i32(2),
            Authentication::MD5Password(salt) => {
                buf.put_i32(5);
                buf.put_slice(salt.as_ref());
            }
        }
        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> std::io::Result<Option<Self>> {
        // TODO: consider refactor this
        let msg_type = buf.get_u8();
        // ensure the type is corrent
        if msg_type != b'R' {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid message type",
            ));
        }

        codec::decode_packet(buf, |buf, _| {
            let code = buf.get_i32();
            let msg = match code {
                0 => Authentication::Ok,
                2 => Authentication::KerberosV5,
                3 => Authentication::CleartextPassword,
                5 => {
                    let salt = buf.split_to(4);
                    Authentication::MD5Password(salt.to_vec())
                }
                _ => unreachable!(),
            };

            Ok(msg)
        })
    }
}

/// password packet sent from frontend
#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct Password {
    password: String,
}

impl Password {
    pub fn new(password: String) -> Password {
        Password { password }
    }
}

impl MessageType for Password {
    #[inline]
    fn message_type(&self) -> Option<u8> {
        Some(b'p')
    }
}

impl MessageLength for Password {
    fn message_length(&self) -> i32 {
        (5 + self.password.as_bytes().len()) as i32
    }
}

impl Codec for Password {
    fn encode(&self, buf: &mut BytesMut) -> std::io::Result<()> {
        buf.put_u8(self.message_type().unwrap());
        buf.put_i32(self.message_length());
        codec::put_cstring(buf, &self.password);

        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> std::io::Result<Option<Self>> {
        // TODO: consider refactor this
        let msg_type = buf.get_u8();
        // ensure the type is corrent
        if msg_type != b'p' {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid message type",
            ));
        }

        codec::decode_packet(buf, |buf, _| {
            let pass = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());

            Ok(Password::new(pass))
        })
    }
}
