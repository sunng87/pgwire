use std::fmt::Debug;

use bytes::{Bytes, BytesMut};
use postgres_types::{IsNull, ToSql, Type};

use crate::{
    error::{PgWireError, PgWireResult},
    messages::{
        data::{DataRow, FieldDescription, RowDescription, FORMAT_CODE_BINARY, FORMAT_CODE_TEXT},
        response::{CommandComplete, ErrorResponse},
    },
};

#[derive(Debug)]
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
            format!("{:?} {:?}", tag.command, rows)
        } else {
            tag.command
        };
        CommandComplete::new(tag_string)
    }
}

#[derive(Debug, new)]
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

#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct QueryResponse {
    pub(crate) row_schema: Vec<FieldInfo>,
    pub(crate) data_rows: Vec<DataRow>,
    pub(crate) tag: Tag,
}

pub struct QueryResponseBuilder {
    row_schema: Vec<FieldInfo>,
    rows: Vec<DataRow>,
    format: i16,

    buffer: BytesMut,
    current_row: DataRow,
    col_index: usize,
}

impl QueryResponseBuilder {
    pub fn new(fields: Vec<FieldInfo>) -> QueryResponseBuilder {
        let fields_count = fields.len();
        let current_row = DataRow::new(Vec::with_capacity(fields_count));
        QueryResponseBuilder {
            row_schema: fields,
            rows: Vec::new(),
            buffer: BytesMut::with_capacity(8),
            format: FORMAT_CODE_BINARY,

            current_row,
            col_index: 0,
        }
    }

    pub fn text_format(&mut self) {
        self.format = FORMAT_CODE_TEXT;
    }

    pub fn binary_format(&mut self) {
        self.format = FORMAT_CODE_BINARY;
    }

    pub fn append_field_binary<T>(&mut self, t: T) -> PgWireResult<()>
    where
        T: ToSql + Sized,
    {
        if self.format != FORMAT_CODE_BINARY {
            panic!("Conflict format. Call binary_format() to switch.");
        }

        let col_type = &self.row_schema[self.col_index].datatype;
        if let IsNull::No = t.to_sql(col_type, &mut self.buffer)? {
            self.current_row
                .fields_mut()
                .push(Some(self.buffer.split().freeze()));
        } else {
            self.current_row.fields_mut().push(None);
        };

        self.buffer.clear();
        self.col_index += 1;

        Ok(())
    }

    pub fn append_field_text<T>(&mut self, data: Option<T>) -> PgWireResult<()>
    where
        T: ToString,
    {
        if self.format != FORMAT_CODE_TEXT {
            panic!("Conflict format. Call text_format() to switch.");
        }

        if let Some(data) = data {
            self.current_row
                .fields_mut()
                .push(Some(Bytes::copy_from_slice(data.to_string().as_ref())));
        } else {
            self.current_row.fields_mut().push(None);
        }

        Ok(())
    }

    pub fn finish_row(&mut self) {
        let row = std::mem::replace(
            &mut self.current_row,
            DataRow::new(Vec::with_capacity(self.row_schema.len())),
        );
        self.rows.push(row);

        self.col_index = 0;
    }

    pub fn build(mut self) -> QueryResponse {
        let row_count = self.rows.len();

        // set column format
        for r in self.row_schema.iter_mut() {
            r.format = self.format;
        }

        QueryResponse {
            row_schema: self.row_schema,
            data_rows: self.rows,
            tag: Tag::new_for_query(row_count),
        }
    }
}

impl From<PgWireError> for ErrorResponse {
    fn from(_e: PgWireError) -> Self {
        // TODO: carry inforamtion with PgWireError
        ErrorResponse::default()
    }
}

/// Query response types:
///
/// * Query: the response contains data rows
/// * Execution: response for ddl/dml execution
/// * Error: error response
#[derive(Debug)]
pub enum Response {
    Query(QueryResponse),
    Execution(Tag),
    Error(PgWireError),
}
