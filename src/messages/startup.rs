use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::codec;
use super::Message;
use crate::error::{PgWireError, PgWireResult};

/// Postgresql wire protocol startup message.
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct Startup {
    #[new(value = "3")]
    pub protocol_number_major: u16,
    #[new(value = "0")]
    pub protocol_number_minor: u16,
    #[new(default)]
    pub parameters: BTreeMap<String, String>,
}

impl Default for Startup {
    fn default() -> Startup {
        Startup::new()
    }
}

impl Startup {
    const MINIMUM_STARTUP_MESSAGE_LEN: usize = 8;

    fn is_protocol_version_supported(version: i32) -> bool {
        version == 196608
    }
}

impl Message for Startup {
    fn message_length(&self) -> usize {
        let param_length = self
            .parameters
            .iter()
            .map(|(k, v)| k.len() + v.len() + 2)
            .sum::<usize>();
        // length:4 + protocol_number:4 + param.len + nullbyte:1
        9 + param_length
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        // version number
        buf.put_u16(self.protocol_number_major);
        buf.put_u16(self.protocol_number_minor);

        // parameters
        for (k, v) in self.parameters.iter() {
            codec::put_cstring(buf, k);
            codec::put_cstring(buf, v);
        }
        // ends with empty cstring, a \0
        codec::put_cstring(buf, "");

        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> PgWireResult<Option<Self>> {
        // packet len + protocol version
        // check if packet is valid
        if buf.remaining() >= Self::MINIMUM_STARTUP_MESSAGE_LEN {
            let packet_version = (&buf[4..8]).get_i32();
            if !Self::is_protocol_version_supported(packet_version) {
                return Err(PgWireError::InvalidProtocolVersion(packet_version));
            }
        }

        codec::decode_packet(buf, 0, Self::decode_body)
    }

    fn decode_body(buf: &mut BytesMut, msg_len: usize) -> PgWireResult<Self> {
        // double check to ensure that the packet has more than 8 bytes
        // `codec::decode_packet` has its validation to ensure buf remaining is
        // larger than `msg_len`. So with both checks, we should not have issue
        // with reading protocol numbers.
        if msg_len <= Self::MINIMUM_STARTUP_MESSAGE_LEN {
            return Err(PgWireError::InvalidStartupMessage);
        }

        // parse
        let protocol_number_major = buf.get_u16();
        let protocol_number_minor = buf.get_u16();

        // end by reading the last \0
        let mut parameters = BTreeMap::new();
        while let Some(key) = codec::get_cstring(buf) {
            let value = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());
            parameters.insert(key, value);
        }

        Ok(Startup {
            protocol_number_major,
            protocol_number_minor,
            parameters,
        })
    }
}

/// authentication response family, sent by backend
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug)]
pub enum Authentication {
    Ok,                   // code 0
    CleartextPassword,    // code 3
    KerberosV5,           // code 2
    MD5Password(Vec<u8>), // code 5, with 4 bytes of md5 salt

    SASL(Vec<String>),   // code 10, with server supported sasl mechanisms
    SASLContinue(Bytes), // code 11, with authentication data
    SASLFinal(Bytes),    // code 12, with additional authentication data

                         // TODO: more types
                         // AuthenticationSCMCredential
                         //
                         // AuthenticationGSS
                         // AuthenticationGSSContinue
                         // AuthenticationSSPI
}

pub const MESSAGE_TYPE_BYTE_AUTHENTICATION: u8 = b'R';

impl Message for Authentication {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_AUTHENTICATION)
    }

    #[inline]
    fn message_length(&self) -> usize {
        match self {
            Authentication::Ok | Authentication::CleartextPassword | Authentication::KerberosV5 => {
                8
            }
            Authentication::MD5Password(_) => 12,
            Authentication::SASL(methods) => {
                8 + methods.iter().map(|v| v.len() + 1).sum::<usize>() + 1
            }
            Authentication::SASLContinue(data) => 8 + data.len(),
            Authentication::SASLFinal(data) => 8 + data.len(),
        }
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        match self {
            Authentication::Ok => buf.put_i32(0),
            Authentication::CleartextPassword => buf.put_i32(3),
            Authentication::KerberosV5 => buf.put_i32(2),
            Authentication::MD5Password(salt) => {
                buf.put_i32(5);
                buf.put_slice(salt.as_ref());
            }
            Authentication::SASL(methods) => {
                buf.put_i32(10);
                for method in methods {
                    codec::put_cstring(buf, method);
                }
                buf.put_u8(b'\0');
            }
            Authentication::SASLContinue(data) => {
                buf.put_i32(11);
                buf.put_slice(data.as_ref());
            }
            Authentication::SASLFinal(data) => {
                buf.put_i32(12);
                buf.put_slice(data.as_ref());
            }
        }
        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, msg_len: usize) -> PgWireResult<Self> {
        let code = buf.get_i32();
        let msg = match code {
            0 => Authentication::Ok,
            2 => Authentication::KerberosV5,
            3 => Authentication::CleartextPassword,
            5 => {
                let mut salt_vec = vec![0; 4];
                buf.copy_to_slice(&mut salt_vec);
                Authentication::MD5Password(salt_vec)
            }
            10 => {
                let mut methods = Vec::new();
                while let Some(method) = codec::get_cstring(buf) {
                    methods.push(method);
                }
                Authentication::SASL(methods)
            }
            11 => Authentication::SASLContinue(buf.split_to(msg_len - 8).freeze()),
            12 => Authentication::SASLFinal(buf.split_to(msg_len - 8).freeze()),
            _ => {
                return Err(PgWireError::InvalidAuthenticationMessageCode(code));
            }
        };

        Ok(msg)
    }
}

pub const MESSAGE_TYPE_BYTE_PASWORD_MESSAGE_FAMILY: u8 = b'p';

/// In postgres wire protocol, there are several message types share the same
/// message type 'p':
///
/// * Password
/// * SASLInitialResponse
/// * SASLResponse
/// * GSSResponse
///
/// We cannot decode these messages without a context. So here we define this
/// `PasswordMessageFamily` to include all of them and provide methods to
/// coerce it into particular concrete type.
///
/// When using this message in startup handlers, call
/// `into_password`/`into_sasl_initial_response`/... methods to them
#[non_exhaustive]
#[derive(Debug)]
pub enum PasswordMessageFamily {
    /// The type of message is unknown.
    Raw(BytesMut),
    /// Password message
    Password(Password),
    /// SASLInitialResponse
    SASLInitialResponse(SASLInitialResponse),
    /// SASLResponse
    SASLResponse(SASLResponse),
}

impl Message for PasswordMessageFamily {
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_PASWORD_MESSAGE_FAMILY)
    }

    fn message_length(&self) -> usize {
        match self {
            PasswordMessageFamily::Raw(body) => body.len() + 4,
            PasswordMessageFamily::Password(inner) => inner.message_length(),
            PasswordMessageFamily::SASLInitialResponse(inner) => inner.message_length(),
            PasswordMessageFamily::SASLResponse(inner) => inner.message_length(),
        }
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        match self {
            PasswordMessageFamily::Raw(body) => {
                buf.put_slice(body.as_ref());
                Ok(())
            }
            PasswordMessageFamily::Password(inner) => inner.encode_body(buf),
            PasswordMessageFamily::SASLInitialResponse(inner) => inner.encode_body(buf),
            PasswordMessageFamily::SASLResponse(inner) => inner.encode_body(buf),
        }
    }

    fn decode_body(buf: &mut BytesMut, full_len: usize) -> PgWireResult<Self> {
        let body = buf.split_to(full_len - 4);
        Ok(PasswordMessageFamily::Raw(body))
    }
}

impl PasswordMessageFamily {
    /// Coerce the raw message into `Password`
    ///
    /// # Panics
    ///
    /// Panic when the message is already coerced into concrete type.
    pub fn into_password(self) -> PgWireResult<Password> {
        if let PasswordMessageFamily::Raw(mut body) = self {
            let len = body.len() + 4;
            Password::decode_body(&mut body, len)
        } else {
            unreachable!(
                "Do not coerce password message when it has a concrete type {:?}",
                self
            )
        }
    }

    /// Coerce the raw message into `SASLInitialResponse`
    ///
    /// # Panics
    ///
    /// Panic when the message is already coerced into concrete type.
    pub fn into_sasl_initial_response(self) -> PgWireResult<SASLInitialResponse> {
        if let PasswordMessageFamily::Raw(mut body) = self {
            let len = body.len() + 4;
            SASLInitialResponse::decode_body(&mut body, len)
        } else {
            unreachable!(
                "Do not coerce password message when it has a concrete type {:?}",
                self
            )
        }
    }

    /// Coerce the raw message into `SASLResponse`
    ///
    /// # Panics
    ///
    /// Panic when the message is already coerced into concrete type.
    pub fn into_sasl_response(self) -> PgWireResult<SASLResponse> {
        if let PasswordMessageFamily::Raw(mut body) = self {
            let len = body.len() + 4;
            SASLResponse::decode_body(&mut body, len)
        } else {
            unreachable!(
                "Do not coerce password message when it has a concrete type {:?}",
                self
            )
        }
    }
}

/// password packet sent from frontend
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct Password {
    pub password: String,
}

impl Message for Password {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_PASWORD_MESSAGE_FAMILY)
    }

    fn message_length(&self) -> usize {
        5 + self.password.len()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        codec::put_cstring(buf, &self.password);

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize) -> PgWireResult<Self> {
        let pass = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());

        Ok(Password::new(pass))
    }
}

/// parameter ack sent from backend after authentication success
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct ParameterStatus {
    pub name: String,
    pub value: String,
}

pub const MESSAGE_TYPE_BYTE_PARAMETER_STATUS: u8 = b'S';

impl Message for ParameterStatus {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_PARAMETER_STATUS)
    }

    fn message_length(&self) -> usize {
        4 + 2 + self.name.len() + self.value.len()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        codec::put_cstring(buf, &self.name);
        codec::put_cstring(buf, &self.value);

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize) -> PgWireResult<Self> {
        let name = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());
        let value = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());

        Ok(ParameterStatus::new(name, value))
    }
}

/// `BackendKeyData` message, sent from backend to frontend for issuing
/// `CancelRequestMessage`
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct BackendKeyData {
    pub pid: i32,
    pub secret_key: i32,
}

pub const MESSAGE_TYPE_BYTE_BACKEND_KEY_DATA: u8 = b'K';

impl Message for BackendKeyData {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_BACKEND_KEY_DATA)
    }

    #[inline]
    fn message_length(&self) -> usize {
        12
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_i32(self.pid);
        buf.put_i32(self.secret_key);

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize) -> PgWireResult<Self> {
        let pid = buf.get_i32();
        let secret_key = buf.get_i32();

        Ok(BackendKeyData { pid, secret_key })
    }
}

/// `Sslrequest` sent from frontend to negotiate with backend to check if the
/// backend supports secure connection. The packet has no message type and
/// contains only a length(4) and an i32 value.
///
/// The backend sends a single byte 'S' or 'N' to indicate its support. Upon 'S'
/// the frontend should close the connection and reinitialize a new TLS
/// connection.
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct SslRequest;

impl SslRequest {
    pub const BODY_MAGIC_NUMBER: i32 = 80877103;
    pub const BODY_SIZE: usize = 8;
}

impl Message for SslRequest {
    #[inline]
    fn message_type() -> Option<u8> {
        None
    }

    #[inline]
    fn message_length(&self) -> usize {
        Self::BODY_SIZE
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_i32(Self::BODY_MAGIC_NUMBER);
        Ok(())
    }

    fn decode_body(_buf: &mut BytesMut, _full_len: usize) -> PgWireResult<Self> {
        unreachable!();
    }

    /// Try to decode and check if the packet is a `SslRequest`.
    fn decode(buf: &mut BytesMut) -> PgWireResult<Option<Self>> {
        if buf.remaining() >= 8 && (&buf[4..8]).get_i32() == Self::BODY_MAGIC_NUMBER {
            buf.advance(8);
            Ok(Some(SslRequest))
        } else {
            Ok(None)
        }
    }
}

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct SASLInitialResponse {
    pub auth_method: String,
    pub data: Option<Bytes>,
}

impl Message for SASLInitialResponse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_PASWORD_MESSAGE_FAMILY)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4 + self.auth_method.len() + 1 + 4 + self.data.as_ref().map(|b| b.len()).unwrap_or(0)
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        codec::put_cstring(buf, &self.auth_method);
        if let Some(ref data) = self.data {
            buf.put_i32(data.len() as i32);
            buf.put_slice(data.as_ref());
        } else {
            buf.put_i32(-1);
        }
        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _full_len: usize) -> PgWireResult<Self> {
        let auth_method = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());
        let data_len = buf.get_i32();
        let data = if data_len == -1 {
            None
        } else {
            Some(buf.split_to(data_len as usize).freeze())
        };

        Ok(SASLInitialResponse { auth_method, data })
    }
}

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct SASLResponse {
    pub data: Bytes,
}

impl Message for SASLResponse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_PASWORD_MESSAGE_FAMILY)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4 + self.data.len()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_slice(self.data.as_ref());
        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, full_len: usize) -> PgWireResult<Self> {
        let data = buf.split_to(full_len - 4).freeze();
        Ok(SASLResponse { data })
    }
}
