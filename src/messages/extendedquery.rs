use bytes::{Buf, BufMut, Bytes};
use postgres_types::Oid;

use super::{codec, Message};
use crate::error::PgWireResult;

/// Request from frontend to parse a prepared query string
#[derive(PartialEq, Eq, Debug, new)]
pub struct Parse {
    pub name: Option<String>,
    pub query: String,
    pub type_oids: Vec<Oid>,
    #[new(default)]
    _hidden: (),
}

pub const MESSAGE_TYPE_BYTE_PARSE: u8 = b'P';

impl Message for Parse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_PARSE)
    }

    fn message_length(&self) -> usize {
        4 + codec::option_string_len(&self.name) // name
            + (1 + self.query.as_bytes().len()) // query
            + (4 * self.type_oids.len()) // type oids
    }

    fn encode_body(&self, buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        codec::put_option_cstring(buf, &self.name);
        codec::put_cstring(buf, &self.query);

        buf.put_i16(self.type_oids.len() as i16);
        for oid in &self.type_oids {
            buf.put_u32(*oid);
        }

        Ok(())
    }

    fn decode_body(buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        let name = codec::get_cstring(buf);
        let query = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());
        let type_oid_count = buf.get_i16();

        let mut type_oids = Vec::with_capacity(type_oid_count as usize);
        for _ in 0..type_oid_count {
            type_oids.push(buf.get_u32());
        }

        Ok(Parse {
            name,
            query,
            type_oids,
            _hidden: (),
        })
    }
}

/// Response for Parse command, sent from backend to frontend
#[derive(PartialEq, Eq, Debug, new)]
pub struct ParseComplete;

pub const MESSAGE_TYPE_BYTE_PARSE_COMPLETE: u8 = b'1';

impl Message for ParseComplete {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_PARSE_COMPLETE)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4
    }

    #[inline]
    fn encode_body(&self, _buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    #[inline]
    fn decode_body(_buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        Ok(ParseComplete)
    }
}

/// Closing the prepared statement or portal
#[derive(PartialEq, Eq, Debug, new)]
pub struct Close {
    pub target_type: u8,
    pub name: Option<String>,
    #[new(default)]
    _hidden: (),
}

pub const TARGET_TYPE_BYTE_STATEMENT: u8 = b'S';
pub const TARGET_TYPE_BYTE_PORTAL: u8 = b'P';

pub const MESSAGE_TYPE_BYTE_CLOSE: u8 = b'C';

impl Message for Close {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_CLOSE)
    }

    fn message_length(&self) -> usize {
        4 + 1 + codec::option_string_len(&self.name)
    }

    fn encode_body(&self, buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        buf.put_u8(self.target_type);
        codec::put_option_cstring(buf, &self.name);
        Ok(())
    }

    fn decode_body(buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        let target_type = buf.get_u8();
        let name = codec::get_cstring(buf);

        Ok(Close {
            target_type,
            name,
            _hidden: (),
        })
    }
}

/// Response for Close command, sent from backend to frontend
#[derive(PartialEq, Eq, Debug, new)]
pub struct CloseComplete;

pub const MESSAGE_TYPE_BYTE_CLOSE_COMPLETE: u8 = b'3';

impl Message for CloseComplete {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_CLOSE_COMPLETE)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4
    }

    #[inline]
    fn encode_body(&self, _buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    #[inline]
    fn decode_body(_buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        Ok(CloseComplete)
    }
}

/// Bind command, for executing prepared statement
#[derive(PartialEq, Eq, Debug, new)]
pub struct Bind {
    pub portal_name: Option<String>,
    pub statement_name: Option<String>,
    pub parameter_format_codes: Vec<i16>,
    // None for Null data, TODO: consider wrapping this together with DataRow in
    // data.rs
    pub parameters: Vec<Option<Bytes>>,

    pub result_column_format_codes: Vec<i16>,
    #[new(default)]
    _hidden: (),
}

pub const MESSAGE_TYPE_BYTE_BIND: u8 = b'B';

impl Message for Bind {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_BIND)
    }

    fn message_length(&self) -> usize {
        4 + codec::option_string_len(&self.portal_name) + codec::option_string_len(&self.statement_name)
            + 2 // parameter_format_code len
            + (2 * self.parameter_format_codes.len()) // parameter_format_codes
            + 2 // parameters len
            + self.parameters.iter().map(|p| 4 + p.as_ref().map(|data| data.len()).unwrap_or(0)).sum::<usize>() // parameters
            + 2 // result_format_code len
            + (2 * self.result_column_format_codes.len()) // result_format_codes
    }

    fn encode_body(&self, buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        codec::put_option_cstring(buf, &self.portal_name);
        codec::put_option_cstring(buf, &self.statement_name);

        buf.put_i16(self.parameter_format_codes.len() as i16);
        for c in &self.parameter_format_codes {
            buf.put_i16(*c);
        }

        buf.put_i16(self.parameters.len() as i16);
        for v in &self.parameters {
            if let Some(v) = v {
                buf.put_i32(v.len() as i32);
                buf.put_slice(v.as_ref());
            } else {
                buf.put_i32(-1);
            }
        }

        buf.put_i16(self.result_column_format_codes.len() as i16);
        for c in &self.result_column_format_codes {
            buf.put_i16(*c);
        }

        Ok(())
    }

    fn decode_body(buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        let portal_name = codec::get_cstring(buf);
        let statement_name = codec::get_cstring(buf);

        let parameter_format_code_len = buf.get_i16();
        let mut parameter_format_codes = Vec::with_capacity(parameter_format_code_len as usize);

        for _ in 0..parameter_format_code_len {
            parameter_format_codes.push(buf.get_i16());
        }

        let parameter_len = buf.get_i16();
        let mut parameters = Vec::with_capacity(parameter_len as usize);
        for _ in 0..parameter_len {
            let data_len = buf.get_i32();

            if data_len >= 0 {
                parameters.push(Some(buf.split_to(data_len as usize).freeze()));
            } else {
                parameters.push(None);
            }
        }

        let result_column_format_code_len = buf.get_i16();
        let mut result_column_format_codes =
            Vec::with_capacity(result_column_format_code_len as usize);
        for _ in 0..result_column_format_code_len {
            result_column_format_codes.push(buf.get_i16());
        }

        Ok(Bind {
            portal_name,
            statement_name,

            parameter_format_codes,
            parameters,

            result_column_format_codes,

            _hidden: (),
        })
    }
}

/// Success response for `Bind`
#[derive(PartialEq, Eq, Debug, new)]
pub struct BindComplete;

pub const MESSAGE_TYPE_BYTE_BIND_COMPLETE: u8 = b'2';

impl Message for BindComplete {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_BIND_COMPLETE)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4
    }

    #[inline]
    fn encode_body(&self, _buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    #[inline]
    fn decode_body(_buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        Ok(BindComplete)
    }
}

/// Describe command fron frontend to backend. For getting information of
/// particular portal or statement
#[derive(PartialEq, Eq, Debug, new)]
pub struct Describe {
    pub target_type: u8,
    pub name: Option<String>,
    #[new(default)]
    _hidden: (),
}

pub const MESSAGE_TYPE_BYTE_DESCRIBE: u8 = b'D';

impl Message for Describe {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_DESCRIBE)
    }

    fn message_length(&self) -> usize {
        4 + 1 + codec::option_string_len(&self.name)
    }

    fn encode_body(&self, buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        buf.put_u8(self.target_type);
        codec::put_option_cstring(buf, &self.name);
        Ok(())
    }

    fn decode_body(buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        let target_type = buf.get_u8();
        let name = codec::get_cstring(buf);

        Ok(Describe {
            target_type,
            name,
            _hidden: (),
        })
    }
}

/// Execute portal by its name
#[derive(PartialEq, Eq, Debug, new)]
pub struct Execute {
    pub name: Option<String>,
    pub max_rows: i32,
    #[new(default)]
    _hidden: (),
}

pub const MESSAGE_TYPE_BYTE_EXECUTE: u8 = b'E';

impl Message for Execute {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_EXECUTE)
    }

    fn message_length(&self) -> usize {
        4 + codec::option_string_len(&self.name) + 4
    }

    fn encode_body(&self, buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        codec::put_option_cstring(buf, &self.name);
        buf.put_i32(self.max_rows);
        Ok(())
    }

    fn decode_body(buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        let name = codec::get_cstring(buf);
        let max_rows = buf.get_i32();

        Ok(Execute {
            name,
            max_rows,
            _hidden: (),
        })
    }
}

#[derive(PartialEq, Eq, Debug, new)]
pub struct Flush;

pub const MESSAGE_TYPE_BYTE_FLUSH: u8 = b'H';

impl Message for Flush {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_FLUSH)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4
    }

    fn encode_body(&self, _buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    fn decode_body(_buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        Ok(Flush)
    }
}

/// Execute portal by its name
#[derive(PartialEq, Eq, Debug, new)]
pub struct Sync;

pub const MESSAGE_TYPE_BYTE_SYNC: u8 = b'S';

impl Message for Sync {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_SYNC)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4
    }

    fn encode_body(&self, _buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    fn decode_body(_buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        Ok(Sync)
    }
}

#[derive(PartialEq, Eq, Debug, new)]
pub struct PortalSuspended;

pub const MESSAGE_TYPE_BYTE_PORTAL_SUSPENDED: u8 = b's';

impl Message for PortalSuspended {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_PORTAL_SUSPENDED)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4
    }

    fn encode_body(&self, _buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    fn decode_body(_buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        Ok(PortalSuspended)
    }
}
