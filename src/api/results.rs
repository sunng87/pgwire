use std::fmt::Debug;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use futures::Stream;
use postgres_types::{IsNull, Oid, ToSql, Type};
use tokio::sync::{Mutex, MutexGuard};

use crate::{
    error::{ErrorInfo, PgWireResult},
    messages::{
        data::{DataRow, FieldDescription, RowDescription, FORMAT_CODE_BINARY, FORMAT_CODE_TEXT},
        response::CommandComplete,
    },
    types::ToSqlText,
};

#[derive(Debug, Eq, PartialEq)]
pub struct Tag {
    command: String,
    oid: Option<Oid>,
    rows: Option<usize>,
}

impl Tag {
    pub fn new(command: &str) -> Tag {
        Tag {
            command: command.to_owned(),
            oid: None,
            rows: None,
        }
    }

    pub fn with_rows(mut self, rows: usize) -> Tag {
        self.rows = Some(rows);
        self
    }

    pub fn with_oid(mut self, oid: Oid) -> Tag {
        self.oid = Some(oid);
        self
    }
}

impl From<Tag> for CommandComplete {
    fn from(tag: Tag) -> CommandComplete {
        let tag_string = if let (Some(oid), Some(rows)) = (tag.oid, tag.rows) {
            format!("{} {oid} {rows}", tag.command)
        } else if let Some(rows) = tag.rows {
            format!("{} {rows}", tag.command)
        } else {
            tag.command
        };
        CommandComplete::new(tag_string)
    }
}

/// Describe encoding of a data field.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum FieldFormat {
    Text,
    Binary,
}

impl FieldFormat {
    /// Get format code for the encoding.
    pub fn value(&self) -> i16 {
        match self {
            Self::Text => FORMAT_CODE_TEXT,
            Self::Binary => FORMAT_CODE_BINARY,
        }
    }

    /// Parse FieldFormat from format code.
    ///
    /// 0 for text format, 1 for binary format. If the input is neither 0 nor 1,
    /// here we return text as default value.
    pub fn from(code: i16) -> Self {
        if code == FORMAT_CODE_BINARY {
            FieldFormat::Binary
        } else {
            FieldFormat::Text
        }
    }
}

#[derive(Debug, new, Eq, PartialEq, Clone)]
pub struct FieldInfo {
    name: String,
    table_id: Option<i32>,
    column_id: Option<i16>,
    datatype: Type,
    format: FieldFormat,
}

impl FieldInfo {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn table_id(&self) -> Option<i32> {
        self.table_id
    }

    pub fn column_id(&self) -> Option<i16> {
        self.column_id
    }

    pub fn datatype(&self) -> &Type {
        &self.datatype
    }

    pub fn format(&self) -> FieldFormat {
        self.format
    }
}

impl From<&FieldInfo> for FieldDescription {
    fn from(fi: &FieldInfo) -> Self {
        FieldDescription::new(
            fi.name.clone(),           // name
            fi.table_id.unwrap_or(0),  // table_id
            fi.column_id.unwrap_or(0), // column_id
            fi.datatype.oid(),         // type_id
            // TODO: type size and modifier
            0,
            0,
            fi.format.value(),
        )
    }
}

impl From<FieldDescription> for FieldInfo {
    fn from(value: FieldDescription) -> Self {
        FieldInfo::new(
            value.name,
            Some(value.table_id),
            Some(value.column_id),
            Type::from_oid(value.type_id).unwrap_or(Type::UNKNOWN),
            FieldFormat::from(value.format_code),
        )
    }
}

pub(crate) fn into_row_description(fields: &[FieldInfo]) -> RowDescription {
    RowDescription::new(fields.iter().map(Into::into).collect())
}

pub type BoxRowStream = Box<dyn Stream<Item = PgWireResult<DataRow>> + Send + Sync + Unpin>;

pub struct QueryResponse<RowStream> {
    command_tag: String,
    row_schema: Arc<Vec<FieldInfo>>,
    data_rows: Arc<Mutex<RowStream>>,
}

impl<RowStream> Debug for QueryResponse<RowStream> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResponse")
            .field("command_tag", &self.command_tag)
            .field("row_schema", &self.row_schema)
            .finish()
    }
}

impl<RowStream> QueryResponse<RowStream> {
    /// Create `QueryResponse` from column schemas and stream of data row.
    /// Sets "SELECT" as the command tag.
    pub fn new(field_defs: Arc<Vec<FieldInfo>>, row_stream: RowStream) -> QueryResponse<RowStream> {
        QueryResponse {
            command_tag: "SELECT".to_owned(),
            row_schema: field_defs,
            data_rows: Arc::new(Mutex::new(row_stream)),
        }
    }

    /// Get the command tag
    pub fn command_tag(&self) -> &str {
        &self.command_tag
    }

    /// Set the command tag
    pub fn set_command_tag(&mut self, command_tag: &str) {
        command_tag.clone_into(&mut self.command_tag);
    }

    /// Get schema of columns
    pub fn row_schema(&self) -> Arc<Vec<FieldInfo>> {
        self.row_schema.clone()
    }

    /// Get access to data rows stream
    pub async fn lock_data_rows(&self) -> MutexGuard<'_, RowStream> {
        self.data_rows.lock().await
    }
}

pub struct DataRowEncoder {
    schema: Arc<Vec<FieldInfo>>,
    row_buffer: BytesMut,
    col_index: usize,
}

impl DataRowEncoder {
    /// New DataRowEncoder from schema of column
    pub fn new(fields: Arc<Vec<FieldInfo>>) -> DataRowEncoder {
        Self {
            schema: fields,
            row_buffer: BytesMut::with_capacity(128),
            col_index: 0,
        }
    }

    /// Encode value with custom type and format
    ///
    /// This encode function ignores data type and format information from
    /// schema of this encoder.
    pub fn encode_field_with_type_and_format<T>(
        &mut self,
        value: &T,
        data_type: &Type,
        format: FieldFormat,
    ) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized,
    {
        // remember the position of the 4-byte length field
        let prev_index = self.row_buffer.len();
        // write value length as -1 ahead of time
        self.row_buffer.put_i32(-1);

        let is_null = if format == FieldFormat::Text {
            value.to_sql_text(data_type, &mut self.row_buffer)?
        } else {
            value.to_sql(data_type, &mut self.row_buffer)?
        };

        if let IsNull::No = is_null {
            let value_length = self.row_buffer.len() - prev_index - 4;
            let mut length_bytes = &mut self.row_buffer[prev_index..(prev_index + 4)];
            length_bytes.put_i32(value_length as i32);
        }

        self.col_index += 1;

        Ok(())
    }

    /// Encode value using type and format, defined by schema
    ///
    /// Panic when encoding more columns than provided as schema.
    pub fn encode_field<T>(&mut self, value: &T) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized,
    {
        let data_type = self.schema[self.col_index].datatype().clone();
        let format = self.schema[self.col_index].format();

        self.encode_field_with_type_and_format(value, &data_type, format)
    }

    pub fn finish(self) -> PgWireResult<DataRow> {
        Ok(DataRow::new(self.row_buffer, self.col_index as i16))
    }
}

/// Get response data for a `Describe` command
pub trait DescribeResponse {
    fn parameters(&self) -> Option<&[Type]>;

    fn fields(&self) -> &[FieldInfo];

    /// Create an no_data instance of `DescribeResponse`. This is typically used
    /// when client tries to describe an empty query.
    fn no_data() -> Self;

    /// Return true if the `DescribeResponse` is empty/nodata
    fn is_no_data(&self) -> bool;
}

/// Response for frontend describe statement requests.
#[non_exhaustive]
#[derive(Debug, new)]
pub struct DescribeStatementResponse {
    pub parameters: Vec<Type>,
    pub fields: Vec<FieldInfo>,
}

impl DescribeResponse for DescribeStatementResponse {
    fn parameters(&self) -> Option<&[Type]> {
        Some(self.parameters.as_ref())
    }

    fn fields(&self) -> &[FieldInfo] {
        &self.fields
    }

    /// Create an no_data instance of `DescribeStatementResponse`. This is typically used
    /// when client tries to describe an empty query.
    fn no_data() -> Self {
        DescribeStatementResponse {
            parameters: vec![],
            fields: vec![],
        }
    }

    /// Return true if the `DescribeStatementResponse` is empty/nodata
    fn is_no_data(&self) -> bool {
        self.parameters.is_empty() && self.fields.is_empty()
    }
}

/// Response for frontend describe portal requests.
#[non_exhaustive]
#[derive(Debug, new)]
pub struct DescribePortalResponse {
    pub fields: Vec<FieldInfo>,
}

impl DescribeResponse for DescribePortalResponse {
    fn parameters(&self) -> Option<&[Type]> {
        None
    }

    fn fields(&self) -> &[FieldInfo] {
        &self.fields
    }

    /// Create an no_data instance of `DescribePortalResponse`. This is typically used
    /// when client tries to describe an empty query.
    fn no_data() -> Self {
        DescribePortalResponse { fields: vec![] }
    }

    /// Return true if the `DescribePortalResponse` is empty/nodata
    fn is_no_data(&self) -> bool {
        self.fields.is_empty()
    }
}

/// Response for copy operations
#[non_exhaustive]
#[derive(Debug, new)]
pub struct CopyResponse {
    pub format: i8,
    pub columns: usize,
    pub column_formats: Vec<i16>,
}

/// Query response types:
///
/// * Query: the response contains data rows
/// * Execution: response for ddl/dml execution
/// * Error: error response
/// * EmptyQuery: when client sends an empty query
/// * TransactionStart: indicate previous statement just started a transaction
/// * TransactionEnd: indicate previous statement just ended a transaction
/// * CopyIn: response for a copy-in request
/// * CopyOut: response for a copy-out request
/// * CopuBoth: response for a copy-both request
pub enum Response<RowStream> {
    EmptyQuery,
    Query(QueryResponse<RowStream>),
    Execution(Tag),
    TransactionStart(Tag),
    TransactionEnd(Tag),
    Error(Box<ErrorInfo>),
    CopyIn(CopyResponse),
    CopyOut(CopyResponse),
    CopyBoth(CopyResponse),
}

#[cfg(test)]
mod test {
    use std::time::SystemTime;

    use super::*;

    #[test]
    fn test_command_complete() {
        let tag = Tag::new("INSERT").with_rows(100);
        let cc = CommandComplete::from(tag);

        assert_eq!(cc.tag, "INSERT 100");

        let tag = Tag::new("INSERT").with_oid(0).with_rows(100);
        let cc = CommandComplete::from(tag);

        assert_eq!(cc.tag, "INSERT 0 100");
    }

    #[test]
    fn test_data_row_encoder() {
        let schema = Arc::new(vec![
            FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text),
            FieldInfo::new("ts".into(), None, None, Type::TIMESTAMP, FieldFormat::Text),
        ]);
        let now = SystemTime::now();
        let mut encoder = DataRowEncoder::new(schema);
        encoder.encode_field(&2001).unwrap();
        encoder.encode_field(&"udev").unwrap();
        encoder.encode_field(&now).unwrap();

        let row = encoder.finish().unwrap();

        assert_eq!(row.field_count, 3);

        let mut expected = BytesMut::new();
        expected.put_i32(4);
        expected.put_slice("2001".as_bytes());
        expected.put_i32(4);
        expected.put_slice("udev".as_bytes());
        expected.put_i32(26);
        let _ = now.to_sql_text(&Type::TIMESTAMP, &mut expected);
        assert_eq!(row.data, expected);
    }
}
