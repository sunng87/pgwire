use bytes::Buf;
use postgres_types::FromSqlOwned;

use crate::api::results::{FieldFormat, FieldInfo};
use crate::error::{PgWireClientError, PgWireClientResult};
use crate::messages::data::DataRow;
use crate::types::FromSqlText;
use crate::types::format::FormatOptions;

#[derive(new, Debug)]
pub struct DataRowsReader {
    fields: Vec<FieldInfo>,
    rows: Vec<DataRow>,
}

impl DataRowsReader {
    pub fn empty() -> DataRowsReader {
        Self {
            fields: vec![],
            rows: vec![],
        }
    }

    /// Generate row decoder for next row
    pub fn next_row(&mut self) -> Option<DataRowDecoder<'_>> {
        if !self.rows.is_empty() {
            let row = self.rows.remove(0);
            Some(DataRowDecoder::new(self.fields.as_slice(), row))
        } else {
            None
        }
    }
}

#[derive(new, Debug)]
pub struct DataRowDecoder<'a> {
    fields: &'a [FieldInfo],
    row: DataRow,
    #[new(default)]
    read_index: usize,
}

impl DataRowDecoder<'_> {
    /// Get value from data row
    pub fn next_value<T>(&mut self) -> PgWireClientResult<Option<T>>
    where
        T: FromSqlOwned + for<'a> FromSqlText<'a>,
    {
        if let Some(field_info) = self.fields.get(self.read_index) {
            // advance read index
            self.read_index += 1;

            let byte_len = self.row.data.get_i32();
            if byte_len < 0 {
                Ok(None)
            } else {
                let bytes = self.row.data.split_to(byte_len as usize);

                if field_info.format() == FieldFormat::Text {
                    T::from_sql_text(
                        field_info.datatype(),
                        bytes.as_ref(),
                        &FormatOptions::default(),
                    )
                    .map_err(PgWireClientError::FromSqlError)
                    .map(Some)
                } else {
                    // binary
                    T::from_sql(field_info.datatype(), bytes.as_ref())
                        .map_err(PgWireClientError::FromSqlError)
                        .map(Some)
                }
            }
        } else {
            Err(PgWireClientError::DataRowIndexOutOfBounds)
        }
    }

    /// Length of fields
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

#[cfg(test)]
mod tests {}
