use bytes::{Buf, BufMut, BytesMut};
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

    fn decode_body(buf: &mut BytesMut) -> PgWireResult<Self> {
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
    // None for Null data
    fields: Vec<Option<Vec<u8>>>,
}

impl DataRow {
    /// resolve data row content as text, return None if the field not exists or
    /// the value is Null.
    ///
    /// FIXME: unwrap for utf8
    pub fn as_text(&self, index: usize) -> Option<String> {
        self.fields
            .get(index)
            .and_then(|f| f.as_ref().map(|bs| String::from_utf8(bs.clone()).unwrap()))
    }

    /// resolve data row content as binary, return None if the field not exists
    /// or the value is Null
    pub fn as_binary(&self, index: usize) -> Option<&Vec<u8>> {
        self.fields.get(index).and_then(|f| f.as_ref())
    }

    /// count of fields
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

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
                .map(|f| 4 + f.as_ref().map(|d| d.len()).unwrap_or(0))
                .sum::<usize>()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_i16(self.fields.len() as i16);

        for field in &self.fields {
            if let Some(data) = field {
                buf.put_i32(data.len() as i32);
                buf.put_slice(data.as_ref());
            } else {
                buf.put_i32(-1);
            }
        }

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut) -> PgWireResult<Self> {
        let fields_len = buf.get_i16();
        let mut fields = Vec::with_capacity(fields_len as usize);

        for _ in 0..fields_len {
            let data_len = buf.get_i32();

            if data_len >= 0 {
                fields.push(Some(buf.split_to(data_len as usize).to_vec()));
            } else {
                fields.push(None);
            }
        }

        Ok(DataRow { fields })
    }
}
