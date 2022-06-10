use std::collections::BTreeMap;

use super::{MessageLength, MessageType};

/// postgresql wire protocol startup message, sent by frontend
/// the strings are null-ternimated string, which is a string
/// terminated by a zero byte.
/// the key-value parameter pairs are terminated by a zero byte, too.
///
#[derive(Getters, Setters, MutGetters)]
pub struct Startup {
    protocol_number_major: u16,
    protocol_number_minor: u16,

    user: String,

    parameters: BTreeMap<String, String>,
}

impl Default for Startup {
    fn default() -> Startup {
        Startup {
            protocol_number_major: 3,
            protocol_number_minor: 0,

            user: "".to_owned(),

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
        let user_length = self.user.len() as i32;
        // length:4 + protocol_number:4 + user:5 + user.len(nullbyte:1) + param.len + nullbyte:1
        15 + user_length + param_length
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
