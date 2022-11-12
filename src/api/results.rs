use std::fmt::Debug;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::{
    stream::{BoxStream, StreamExt},
    Stream,
};
use postgres_types::{IsNull, ToSql, Type};

use crate::{
    error::{ErrorInfo, PgWireResult},
    messages::{
        data::{DataRow, FieldDescription, RowDescription, FORMAT_CODE_BINARY, FORMAT_CODE_TEXT},
        response::CommandComplete,
    },
};

#[derive(Debug, Eq, PartialEq)]
pub struct Tag {
    command: String,
    rows: Option<usize>,
}

impl Tag {
    pub fn new_for_query(rows: usize) -> Tag {
        Tag {
            command: "SELECT".to_owned(),
            rows: Some(rows),
        }
    }

    pub fn new_for_execution(command: &str, rows: Option<usize>) -> Tag {
        Tag {
            command: command.to_owned(),
            rows,
        }
    }
}

impl From<Tag> for CommandComplete {
    fn from(tag: Tag) -> CommandComplete {
        let tag_string = if let Some(rows) = tag.rows {
            format!("{} {rows}", tag.command)
        } else {
            tag.command
        };
        CommandComplete::new(tag_string)
    }
}

#[derive(Debug, new, Eq, PartialEq, Clone)]
pub struct FieldInfo {
    name: String,
    table_id: Option<i32>,
    column_id: Option<i16>,
    datatype: Type,
    #[new(value = "FORMAT_CODE_BINARY")]
    format: i16,
}

impl From<FieldInfo> for FieldDescription {
    fn from(fi: FieldInfo) -> Self {
        FieldDescription::new(
            fi.name,                   // name
            fi.table_id.unwrap_or(0),  // table_id
            fi.column_id.unwrap_or(0), // column_id
            fi.datatype.oid(),         // type_id
            // TODO: type size and modifier
            0,
            0,
            fi.format,
        )
    }
}

pub(crate) fn into_row_description(fields: Vec<FieldInfo>) -> RowDescription {
    RowDescription::new(fields.into_iter().map(Into::into).collect())
}

#[derive(Getters)]
#[getset(get = "pub")]
pub struct QueryResponse {
    pub(crate) row_schema: Vec<FieldInfo>,
    pub(crate) data_rows: BoxStream<'static, PgWireResult<DataRow>>,
}

pub struct BinaryDataRowEncoder {
    row_schema: Arc<Vec<FieldInfo>>,
    col_index: usize,
    buffer: DataRow,
}

impl BinaryDataRowEncoder {
    pub fn new(row_schema: Arc<Vec<FieldInfo>>) -> BinaryDataRowEncoder {
        BinaryDataRowEncoder {
            buffer: DataRow::new(Vec::with_capacity(row_schema.len())),
            col_index: 0,
            row_schema,
        }
    }

    pub fn append_field<T>(&mut self, value: &T) -> PgWireResult<()>
    where
        T: ToSql + Sized,
    {
        let mut buffer = BytesMut::with_capacity(8);
        if let IsNull::No = value.to_sql(&self.row_schema[self.col_index].datatype, &mut buffer)? {
            self.buffer.fields_mut().push(Some(buffer.split().freeze()));
        } else {
            self.buffer.fields_mut().push(None);
        };
        self.col_index += 1;
        Ok(())
    }

    pub fn finish(self) -> PgWireResult<DataRow> {
        Ok(self.buffer)
    }
}

pub struct TextDataRowEncoder {
    buffer: DataRow,
}

impl TextDataRowEncoder {
    pub fn new(nfields: usize) -> TextDataRowEncoder {
        TextDataRowEncoder {
            buffer: DataRow::new(Vec::with_capacity(nfields)),
        }
    }

    pub fn append_field<T>(&mut self, value: Option<&T>) -> PgWireResult<()>
    where
        T: ToString,
    {
        if let Some(value) = value {
            self.buffer
                .fields_mut()
                .push(Some(Bytes::copy_from_slice(value.to_string().as_bytes())));
        } else {
            self.buffer.fields_mut().push(None);
        }
        Ok(())
    }

    pub fn finish(self) -> PgWireResult<DataRow> {
        Ok(self.buffer)
    }
}

pub fn text_query_response<S>(mut field_defs: Vec<FieldInfo>, row_stream: S) -> QueryResponse
where
    S: Stream<Item = PgWireResult<DataRow>> + Send + Unpin + 'static,
{
    field_defs
        .iter_mut()
        .for_each(|f| f.format = FORMAT_CODE_TEXT);

    QueryResponse {
        row_schema: field_defs,
        data_rows: row_stream.boxed(),
    }
}

pub fn binary_query_response<S>(field_defs: Arc<Vec<FieldInfo>>, row_stream: S) -> QueryResponse
where
    S: Stream<Item = PgWireResult<DataRow>> + Send + Unpin + 'static,
{
    let mut row_schema = (*field_defs).clone();
    row_schema
        .iter_mut()
        .for_each(|f| f.format = FORMAT_CODE_BINARY);

    QueryResponse {
        row_schema,
        data_rows: row_stream.boxed(),
    }
}

/// Query response types:
///
/// * Query: the response contains data rows
/// * Execution: response for ddl/dml execution
/// * Error: error response
pub enum Response {
    Query(QueryResponse),
    Execution(Tag),
    Error(Box<ErrorInfo>),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_command_complete() {
        let tag = Tag::new_for_execution("INSERT", Some(100));
        let cc = CommandComplete::from(tag);

        assert_eq!(cc.tag(), "INSERT 100");
    }
}
