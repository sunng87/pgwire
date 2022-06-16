use bytes::{Buf, BufMut, BytesMut};

use super::codec;
use super::Message;

#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug, Default)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct FieldDescription {
    // the field name
    name: String,
    // the object ID of table, default to 0 if not a table
    table_id: i32,
    // the attribute number of the column, default to 0 if not a column from table
    column_id: i16,
    // the object ID of the data type
    type_id: i32,
    // the size of data type, negative values denote variable-width types
    type_size: i16,
    // the type modifier
    type_modifier: i32,
    // the format code being used for the filed, will be 0 or 1 for now
    format_code: i16,
}

#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug, Default)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct RowDescription {
    fields: Vec<FieldDescription>,
}

impl Message for RowDescription {
    fn message_type() -> Option<u8> {
        Some(b'T')
    }

    fn message_length(&self) -> i32 {
        4 + 2
            + self
                .fields
                .iter()
                .map(|f| f.name.as_bytes().len() + 1 + 4 + 2 + 4 + 2 + 4 + 2)
                .sum::<usize>() as i32
    }

    fn encode_body(&self, buf: &mut BytesMut) -> std::io::Result<()> {
        buf.put_i16(self.fields.len() as i16);

        for field in &self.fields {
            buf.put_slice(field.name.as_bytes());
            buf.put_i32(field.table_id);
            buf.put_i16(field.column_id);
            buf.put_i32(field.type_id);
            buf.put_i16(field.type_size);
            buf.put_i32(field.type_modifier);
            buf.put_i16(field.format_code);
        }

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut) -> std::io::Result<Self> {
        let fields_len = buf.get_i16();
        let mut fields = Vec::with_capacity(fields_len as usize);

        for _ in 0..fields_len {
            let mut field = FieldDescription::default();

            field.name = codec::get_cstring(buf).unwrap();
            field.table_id = buf.get_i32();
            field.column_id = buf.get_i16();
            field.type_id = buf.get_i32();
            field.type_size = buf.get_i16();
            field.type_modifier = buf.get_i32();
            field.format_code = buf.get_i16();

            fields.push(field);
        }

        Ok(RowDescription { fields })
    }
}

#[derive(Getters, Setters, MutGetters, PartialEq, Eq, Debug, Default)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct DataRow {
    // None for Null data
    fields: Vec<Option<Vec<u8>>>,
}

impl Message for DataRow {
    fn message_type() -> Option<u8> {
        Some(b'D')
    }

    fn message_length(&self) -> i32 {
        4 + 2
            + self
                .fields
                .iter()
                .map(|f| 4 + f.as_ref().map(|d| d.len()).unwrap_or(0))
                .sum::<usize>() as i32
    }

    fn encode_body(&self, buf: &mut BytesMut) -> std::io::Result<()> {
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

    fn decode_body(buf: &mut BytesMut) -> std::io::Result<Self> {
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
