use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_types::Oid;

use super::codec;
use super::Message;
use crate::error::PgWireResult;

pub const FORMAT_CODE_TEXT: i16 = 0;
pub const FORMAT_CODE_BINARY: i16 = 1;

#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug, Default, new)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct FieldDescription {
    // the field name
    name: String,
    // the object ID of table, default to 0 if not a table
    table_id: i32,
    // the attribute number of the column, default to 0 if not a column from table
    column_id: i16,
    // the object ID of the data type
    type_id: Oid,
    // the size of data type, negative values denote variable-width types
    type_size: i16,
    // the type modifier
    type_modifier: i32,
    // the format code being used for the filed, will be 0 or 1 for now
    format_code: i16,
}

#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug, Default, new)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct RowDescription {
    fields: Vec<FieldDescription>,
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
                name: codec::get_cstring(buf).unwrap(),
                table_id: buf.get_i32(),
                column_id: buf.get_i16(),
                type_id: buf.get_u32(),
                type_size: buf.get_i16(),
                type_modifier: buf.get_i32(),
                format_code: buf.get_i16(),
            };

            fields.push(field);
        }

        Ok(RowDescription { fields })
    }
}

/// Data structure for postgresql wire protocol `DataRow` message.
///
/// Data can be represented as text or binary format as specified by format
/// codes from previous `RowDescription` message.
#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug, Default, new, Clone)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct DataRow {
    fields: Vec<Option<Bytes>>,
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
                .map(|b| b.as_ref().map(|b| b.len() + 4).unwrap_or(2))
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

        Ok(DataRow { fields })
    }
}
