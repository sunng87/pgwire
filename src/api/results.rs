use std::fmt::Debug;
use std::pin::Pin;
use std::task::Poll;

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

pub struct BinaryQueryResponseBuilder<S> {
    row_schema: Vec<FieldInfo>,
    row_counter: usize,
    row_stream: S,
}

impl<S, R, F> BinaryQueryResponseBuilder<S>
where
    S: Stream<Item = R> + Send + Unpin + 'static,
    R: IntoIterator<Item = F> + 'static,
    F: ToSql + Sized,
{
    pub fn new(mut field_defs: Vec<FieldInfo>, row_stream: S) -> BinaryQueryResponseBuilder<S> {
        field_defs
            .iter_mut()
            .for_each(|f| f.format = FORMAT_CODE_BINARY);

        BinaryQueryResponseBuilder {
            row_schema: field_defs,
            row_counter: 0,
            row_stream,
        }
    }

    fn map_row(&self, from_row: R) -> PgWireResult<DataRow>
where {
        let mut buffer = BytesMut::with_capacity(8);
        let mut row = DataRow::new(Vec::with_capacity(self.row_schema.len()));
        for (idx, field) in from_row.into_iter().enumerate() {
            let col_type = &self.row_schema[idx].datatype;
            if let IsNull::No = field.to_sql(col_type, &mut buffer)? {
                row.fields_mut().push(Some(buffer.split().freeze()));
            } else {
                row.fields_mut().push(None);
            };

            buffer.clear();
        }
        Ok(row)
    }

    pub fn into_response(self) -> QueryResponse {
        QueryResponse {
            row_schema: self.row_schema.clone(),
            data_rows: self.boxed(),
        }
    }
}

impl<S, R, F> Stream for BinaryQueryResponseBuilder<S>
where
    S: Stream<Item = R> + Unpin + Send + 'static,
    R: IntoIterator<Item = F> + 'static,
    F: ToSql + Sized,
{
    type Item = PgWireResult<DataRow>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let s = Pin::new(&mut self.row_stream);
        match s.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                self.row_counter = self.row_counter + 1;
                Poll::Ready(Some(self.map_row(item)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.row_stream.size_hint()
    }
}

pub struct TextQueryResponseBuilder<S> {
    row_schema: Vec<FieldInfo>,
    row_counter: usize,
    row_stream: S,
}

impl<S, R, F> TextQueryResponseBuilder<S>
where
    S: Stream<Item = R> + Send + Unpin + 'static,
    R: IntoIterator<Item = Option<F>> + 'static,
    F: ToString,
{
    pub fn new(mut field_defs: Vec<FieldInfo>, row_stream: S) -> TextQueryResponseBuilder<S> {
        field_defs
            .iter_mut()
            .for_each(|f| f.format = FORMAT_CODE_TEXT);

        TextQueryResponseBuilder {
            row_schema: field_defs,
            row_counter: 0,
            row_stream,
        }
    }

    fn map_row(&self, from_row: R) -> PgWireResult<DataRow> {
        let mut row = DataRow::new(Vec::with_capacity(self.row_schema.len()));
        for field in from_row.into_iter() {
            if let Some(data) = field {
                row.fields_mut()
                    .push(Some(Bytes::copy_from_slice(data.to_string().as_ref())));
            } else {
                row.fields_mut().push(None);
            }
        }
        Ok(row)
    }

    pub fn into_response(self) -> QueryResponse {
        QueryResponse {
            row_schema: self.row_schema.clone(),
            data_rows: self.boxed(),
        }
    }
}

impl<S, R, F> Stream for TextQueryResponseBuilder<S>
where
    S: Stream<Item = R> + Unpin + Send + 'static,
    R: IntoIterator<Item = Option<F>> + 'static,
    F: ToString,
{
    type Item = PgWireResult<DataRow>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let s = Pin::new(&mut self.row_stream);
        match s.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                self.row_counter = self.row_counter + 1;
                Poll::Ready(Some(self.map_row(item)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.row_stream.size_hint()
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
