use std::fmt::Debug;

use bytes::BytesMut;
use postgres_types::{ToSql, Type};

use crate::{
    error::PgWireResult,
    messages::{
        data::{DataRow, FieldDescription, FORMAT_CODE_BINARY},
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
            tag.command.clone()
        };
        CommandComplete::new(tag_string)
    }
}

#[derive(Debug, new)]
pub struct FieldInfo {
    name: String,
    table_id: i32,
    column_id: i16,
    datatype: Type,
}

impl From<FieldInfo> for FieldDescription {
    fn from(fi: FieldInfo) -> Self {
        FieldDescription::new(
            fi.name,           // name
            fi.table_id,       // table_id
            fi.column_id,      // column_id
            fi.datatype.oid(), // type_id
            // TODO: type size and modifier
            0,
            0,
            FORMAT_CODE_BINARY,
        )
    }
}

// TODO: do not hold the reference to Sized or ToSql type,
// use a write method instead to write down them into bytebuf
#[derive(new)]
pub struct QueryResponse<I, T>
where
    I: Iterator<Item = Vec<Box<dyn ?Sized + ToSql>>>,
{
    row_schema: Vec<FieldInfo>,
    data_rows: I,
    tag: Tag,
}

impl<I> Debug for QueryResponse<I>
where
    I: Iterator<Item = Vec<Box<dyn ToSql>>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResponse")
            .field("row_schema", &self.row_schema)
            .field("tag", &self.tag)
            .finish()
    }
}

impl<I> QueryResponse<I>
where
    I: Iterator<Item = Vec<Box<dyn ToSql>>>,
{
    fn into_rows_iter(&mut self) -> impl Iterator<Item = PgWireResult<DataRow>> {
        let columns = self.row_schema.len();
        self.data_rows.map(|raw_vec: Vec<Box<dyn ToSql>>| {
            let mut result = Vec::with_capacity(columns);
            for idx in 0..columns {
                // default buffer size 64
                let mut bytes = BytesMut::with_capacity(64);
                raw_vec[idx].to_sql(&self.row_schema[idx].datatype, &mut bytes)?;

                result.push(bytes);
            }
            Ok(DataRow::new(result))
        })
    }
}

#[derive(Debug)]
pub enum Response<I>
where
    I: Iterator<Item = Vec<Box<dyn ToSql>>>,
{
    Query(QueryResponse<I>),
    Execution(Tag),
    Error(ErrorResponse),
}
