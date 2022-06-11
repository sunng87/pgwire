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
            .map(|(k, v)| k.len() + v.len() + 2)
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
        codec::put_cstring(buf, "");

        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> std::io::Result<Option<Self>> {
        if buf.remaining() > 4 {
            let msg_len = (&buf[..4]).get_i32() as usize;
            if buf.remaining() >= msg_len {
                // skip msg_len
                let _ = buf.get_i32();

                let mut msg = Startup::default();
                // parse
                msg.set_protocol_number_major(buf.get_u16());
                msg.set_protocol_number_minor(buf.get_u16());

                // end by reading the last \0
                while let Some(key) = codec::get_cstring(buf) {
                    let value = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());
                    msg.parameters_mut().insert(key, value);
                }

                return Ok(Some(msg));
            }
        }
        Ok(None)
    }
}

/// authentication response family, sent by backend
pub enum Authentication {
    Ok,                // code 0
    CleartextPassword, // code 3
    KerberosV5,        // code 2
    MD5Password((u8, u8, u8, u8)), // code 5, with 4 bytes of md5 salt

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

/// password packet sent from frontend
#[derive(Getters, Setters, MutGetters)]
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
        (5 + self.password.len()) as i32
    }
}
