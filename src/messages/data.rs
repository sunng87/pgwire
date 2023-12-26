use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_types::Oid;

use super::codec;
use super::Message;
use crate::error::PgWireResult;

pub(crate) const FORMAT_CODE_TEXT: i16 = 0;
pub(crate) const FORMAT_CODE_BINARY: i16 = 1;

#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct FieldDescription {
    // the field name
    pub name: String,
    // the object ID of table, default to 0 if not a table
    pub table_id: i32,
    // the attribute number of the column, default to 0 if not a column from table
    pub column_id: i16,
    // the object ID of the data type
    pub type_id: Oid,
    // the size of data type, negative values denote variable-width types
    pub type_size: i16,
    // the type modifier
    pub type_modifier: i32,
    // the format code being used for the filed, will be 0 or 1 for now
    pub format_code: i16,
}

#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct RowDescription {
    pub fields: Vec<FieldDescription>,
    #[new(default)]
    _hidden: (),
}

pub const MESSAGE_TYPE_BYTE_ROW_DESCRITION: u8 = b'T';

impl Message for RowDescription {
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_ROW_DESCRITION)
    }

    fn message_length(&self) -> usize {
        4 + 2
            + self
                .fields
                .iter()
                .map(|f| f.name.as_bytes().len() + 1 + 4 + 2 + 4 + 2 + 4 + 2)
                .sum::<usize>()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_i16(self.fields.len() as i16);

        for field in &self.fields {
            codec::put_cstring(buf, &field.name);
            buf.put_i32(field.table_id);
            buf.put_i16(field.column_id);
            buf.put_u32(field.type_id);
            buf.put_i16(field.type_size);
            buf.put_i32(field.type_modifier);
            buf.put_i16(field.format_code);
        }

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize) -> PgWireResult<Self> {
        let fields_len = buf.get_i16();
        let mut fields = Vec::with_capacity(fields_len as usize);

        for _ in 0..fields_len {
            let field = FieldDescription {
                name: codec::get_cstring(buf).unwrap_or_else(|| "".to_owned()),
                table_id: buf.get_i32(),
                column_id: buf.get_i16(),
                type_id: buf.get_u32(),
                type_size: buf.get_i16(),
                type_modifier: buf.get_i32(),
                format_code: buf.get_i16(),
            };

            fields.push(field);
        }

        Ok(RowDescription { fields, _hidden: () })
    }
}

/// Data structure returned when frontend describes a statement
#[derive(PartialEq, Eq, Debug, Default, new, Clone)]
pub struct ParameterDescription {
    /// parameter types
    pub types: Vec<Oid>,
    #[new(default)]
    _hidden: (),
}

pub const MESSAGE_TYPE_BYTE_PARAMETER_DESCRITION: u8 = b't';

impl Message for ParameterDescription {
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_PARAMETER_DESCRITION)
    }

    fn message_length(&self) -> usize {
        4 + 2 + self.types.len() * 4
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_i16(self.types.len() as i16);

        for t in &self.types {
            buf.put_i32(*t as i32);
        }

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize) -> PgWireResult<Self> {
        let types_len = buf.get_i16();
        let mut types = Vec::with_capacity(types_len as usize);

        for _ in 0..types_len {
            types.push(buf.get_i32() as Oid);
        }

        Ok(ParameterDescription { types, _hidden: () })
    }
}

/// Data structure for postgresql wire protocol `DataRow` message.
///
/// Data can be represented as text or binary format as specified by format
/// codes from previous `RowDescription` message.
#[derive(PartialEq, Eq, Debug, Default, new, Clone)]
pub struct DataRow {
    pub fields: Vec<Option<Bytes>>,
    #[new(default)]
    _hidden: (),
}

impl DataRow {}

pub const MESSAGE_TYPE_BYTE_DATA_ROW: u8 = b'D';

impl Message for DataRow {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_DATA_ROW)
    }

    fn message_length(&self) -> usize {
        4 + 2
            + self
                .fields
                .iter()
                .map(|b| b.as_ref().map(|b| b.len() + 4).unwrap_or(4))
                .sum::<usize>()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_i16(self.fields.len() as i16);
        for field in &self.fields {
            if let Some(bytes) = field {
                buf.put_i32(bytes.len() as i32);
                buf.put_slice(bytes.as_ref());
            } else {
                buf.put_i32(-1);
            }
        }

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _msg_len: usize) -> PgWireResult<Self> {
        let field_count = buf.get_i16() as usize;

        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let field_len = buf.get_i32();
            if field_len >= 0 {
                fields.push(Some(buf.split_to(field_len as usize).freeze()));
            } else {
                fields.push(None);
            }
        }

        Ok(DataRow { fields, _hidden: () })
    }
}

/// postgres response when query returns no data, sent from backend to frontend
/// in extended query
#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct NoData;

pub const MESSAGE_TYPE_BYTE_NO_DATA: u8 = b'n';

impl Message for NoData {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_NO_DATA)
    }

    fn message_length(&self) -> usize {
        4
    }

    fn encode_body(&self, _buf: &mut BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    fn decode_body(_buf: &mut BytesMut, _: usize) -> PgWireResult<Self> {
        Ok(NoData::new())
    }
}
