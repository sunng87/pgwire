use std::collections::BTreeMap;

use super::{MessageLength, MessageType};

/// postgresql wire protocol startup message, sent by frontend
/// the strings are null-ternimated string, which is a string
/// terminated by a zero byte.
/// the key-value parameter pairs are terminated by a zero byte, too.
///
#[derive(Getters, Setters, MutGetters)]
pub struct StartupMessage {
    protocol_number_major: u16,
    protocol_number_minor: u16,

    user: String,

    parameters: BTreeMap<String, String>,
}

impl Default for StartupMessage {
    fn default() -> StartupMessage {
        StartupMessage {
            protocol_number_major: 3,
            protocol_number_minor: 0,

            user: "".to_owned(),

            parameters: BTreeMap::default(),
        }
    }
}

impl MessageType for StartupMessage {}
impl MessageLength for StartupMessage {
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
