use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

use bytes::{BufMut, Bytes, BytesMut};
use futures::{Stream, StreamExt, future, stream};
use postgres_types::{IsNull, Oid, ToSql, Type};

use crate::error::{ErrorInfo, PgWireError, PgWireResult};
use crate::messages::copy::CopyData;
use crate::messages::data::{
    DataRow, FORMAT_CODE_BINARY, FORMAT_CODE_TEXT, FieldDescription, RowDescription,
};
use crate::messages::response::CommandComplete;
use crate::types::ToSqlText;
use crate::types::format::FormatOptions;
use smol_str::SmolStr;

#[derive(Debug, Eq, PartialEq, Clone)]
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

/// Options for COPY text format.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CopyTextOptions {
    pub delimiter: SmolStr,
    pub null_string: SmolStr,
}

impl Default for CopyTextOptions {
    fn default() -> Self {
        Self {
            delimiter: "\t".into(),
            null_string: "\\N".into(),
        }
    }
}

/// Options for COPY CSV format.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CopyCsvOptions {
    pub delimiter: SmolStr,
    pub quote: SmolStr,
    pub escape: SmolStr,
    pub null_string: SmolStr,
    pub force_quote: Vec<usize>,
}

impl Default for CopyCsvOptions {
    fn default() -> Self {
        Self {
            delimiter: ",".into(),
            quote: "\"".into(),
            escape: "\"".into(),
            null_string: "".into(),
            force_quote: vec![],
        }
    }
}

// Default format options that are cloned in `FieldInfo::new` to avoid `Arc` allocation.
//
// Using thread-local storage avoids contention when multiple threads concurrently
// clone the same `Arc<FormatOptions>` in `DataRowEncoder::encode_field`. Each thread
// now clones its own thread-local instance rather than contending for a shared
// global instance.
//
// This can be made a regular static if we remove format options cloning from
// `DataRowEncoder::encode_field`.
//
// The issue with contention was observed in `examples/bench` benchmark:
// https://github.com/sunng87/pgwire/pull/366#discussion_r2621917771
thread_local! {
    static DEFAULT_FORMAT_OPTIONS: LazyLock<Arc<FormatOptions>> = LazyLock::new(Default::default);
}

#[derive(Debug, new, Eq, PartialEq, Clone)]
pub struct FieldInfo {
    name: String,
    table_id: Option<i32>,
    column_id: Option<i16>,
    datatype: Type,
    format: FieldFormat,
    #[new(value = "DEFAULT_FORMAT_OPTIONS.with(|opts| Arc::clone(&*opts))")]
    format_options: Arc<FormatOptions>,
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

    pub fn format_options(&self) -> &Arc<FormatOptions> {
        &self.format_options
    }

    pub fn with_format_options(mut self, format_options: Arc<FormatOptions>) -> Self {
        self.format_options = format_options;
        self
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

pub type SendableRowStream = Pin<Box<dyn Stream<Item = PgWireResult<DataRow>> + Send>>;

pub type SendableCopyDataStream = Pin<Box<dyn Stream<Item = PgWireResult<CopyData>> + Send>>;

#[non_exhaustive]
pub struct QueryResponse {
    pub command_tag: String,
    pub row_schema: Arc<Vec<FieldInfo>>,
    pub data_rows: SendableRowStream,
}

impl Debug for QueryResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResponse")
            .field("command_tag", &self.command_tag)
            .field("row_schema", &self.row_schema)
            .finish()
    }
}

impl QueryResponse {
    /// Create `QueryResponse` from column schemas and stream of data row.
    /// Sets "SELECT" as the command tag.
    pub fn new<S>(field_defs: Arc<Vec<FieldInfo>>, row_stream: S) -> QueryResponse
    where
        S: Stream<Item = PgWireResult<DataRow>> + Send + 'static,
    {
        QueryResponse {
            command_tag: "SELECT".to_owned(),
            row_schema: field_defs,
            data_rows: Box::pin(row_stream),
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
    pub fn data_rows(&mut self) -> &mut SendableRowStream {
        &mut self.data_rows
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
        format_options: &FormatOptions,
    ) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized,
    {
        // remember the position of the 4-byte length field
        let prev_index = self.row_buffer.len();
        // write value length as -1 ahead of time
        self.row_buffer.put_i32(-1);

        let is_null = if format == FieldFormat::Text {
            value.to_sql_text(data_type, &mut self.row_buffer, format_options)?
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
        let field = &self.schema[self.col_index];

        let data_type = field.datatype().clone();
        let format = field.format();
        let format_options = field.format_options().clone();

        self.encode_field_with_type_and_format(value, &data_type, format, format_options.as_ref())
    }

    #[deprecated(
        since = "0.37.0",
        note = "DataRowEncoder is reusable since 0.37, use `take_row() instead`"
    )]
    pub fn finish(self) -> PgWireResult<DataRow> {
        Ok(DataRow::new(self.row_buffer, self.col_index as i16))
    }

    /// Takes the current row from the encoder, resetting the encoder for reuse.
    ///
    /// This method splits the inner buffer, taking the current row data and leaving the
    /// encoder with an empty buffer (but retaining the capacity) enabling buffer reuse.
    pub fn take_row(&mut self) -> DataRow {
        let row = DataRow::new(self.row_buffer.split(), self.col_index as i16);
        self.col_index = 0;
        row
    }
}

/// Internal COPY format representation.
#[derive(Debug, Clone, Eq, PartialEq)]
enum CopyFormat {
    Binary,
    Text {
        delimiter: SmolStr,
        null_string: SmolStr,
    },
    Csv {
        delimiter: SmolStr,
        quote: SmolStr,
        escape: SmolStr,
        null_string: SmolStr,
        force_quote: Vec<usize>,
    },
}

/// Encoder for COPY operations.
///
/// This encoder produces CopyData messages for PGCOPY binary, text, and CSV formats.
pub struct CopyEncoder {
    schema: Arc<Vec<FieldInfo>>,
    buffer: BytesMut,
    format: CopyFormat,
    col_index: usize,
    header_written: bool,
}

impl CopyEncoder {
    /// Create a new binary format COPY encoder.
    pub fn new_binary(schema: Arc<Vec<FieldInfo>>) -> Self {
        Self {
            schema,
            buffer: BytesMut::with_capacity(128),
            format: CopyFormat::Binary,
            col_index: 0,
            header_written: false,
        }
    }

    /// Create a new text format COPY encoder.
    pub fn new_text(schema: Arc<Vec<FieldInfo>>, options: CopyTextOptions) -> Self {
        Self {
            schema,
            buffer: BytesMut::with_capacity(128),
            format: CopyFormat::Text {
                delimiter: options.delimiter,
                null_string: options.null_string,
            },
            col_index: 0,
            header_written: false,
        }
    }

    /// Create a new CSV format COPY encoder.
    pub fn new_csv(schema: Arc<Vec<FieldInfo>>, options: CopyCsvOptions) -> Self {
        Self {
            schema,
            buffer: BytesMut::with_capacity(128),
            format: CopyFormat::Csv {
                delimiter: options.delimiter,
                quote: options.quote,
                escape: options.escape,
                null_string: options.null_string,
                force_quote: options.force_quote,
            },
            col_index: 0,
            header_written: false,
        }
    }

    /// Encode a field value.
    ///
    /// This method uses the type and format information from the schema.
    pub fn encode_field<T>(&mut self, value: &T) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized,
    {
        let datatype = self.schema[self.col_index].datatype().clone();
        let col_index = self.col_index;
        let num_fields = self.schema.len();

        match &self.format {
            CopyFormat::Binary => self.encode_field_binary(value, &datatype)?,
            CopyFormat::Text { .. } => {
                let is_last = col_index == num_fields - 1;
                self.encode_field_text(value, &datatype, is_last)?;
            }
            CopyFormat::Csv { .. } => {
                let is_last = col_index == num_fields - 1;
                self.encode_field_csv(value, &datatype, is_last)?;
            }
        }

        self.col_index += 1;
        Ok(())
    }

    /// Encode a field in binary format (same as DataRow encoding).
    fn encode_field_binary<T>(&mut self, value: &T, datatype: &Type) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText,
    {
        let prev_index = self.buffer.len();
        self.buffer.put_i32(-1);

        let is_null = value.to_sql(datatype, &mut self.buffer)?;

        if let IsNull::No = is_null {
            let value_length = self.buffer.len() - prev_index - 4;
            let mut length_bytes = &mut self.buffer[prev_index..(prev_index + 4)];
            length_bytes.put_i32(value_length as i32);
        }

        Ok(())
    }

    /// Encode a field in text format.
    fn encode_field_text<T>(
        &mut self,
        value: &T,
        datatype: &Type,
        is_last: bool,
    ) -> PgWireResult<()>
    where
        T: ToSqlText,
    {
        if let CopyFormat::Text {
            delimiter,
            null_string,
        } = &self.format
        {
            let mut temp_buffer = BytesMut::new();
            let is_null =
                value.to_sql_text(datatype, &mut temp_buffer, &FormatOptions::default())?;

            if let IsNull::Yes = is_null {
                self.buffer.put_slice(null_string.as_bytes());
            } else {
                // Backslash escape special characters
                for &byte in temp_buffer.as_ref() {
                    match byte {
                        b'\n' => {
                            self.buffer.put_slice(b"\\n");
                        }
                        b'\r' => {
                            self.buffer.put_slice(b"\\r");
                        }
                        b'\t' => {
                            self.buffer.put_slice(b"\\t");
                        }
                        b'\\' => {
                            self.buffer.put_slice(b"\\\\");
                        }
                        b if byte == delimiter.as_bytes()[0] => {
                            self.buffer.put_u8(b'\\');
                            self.buffer.put_u8(byte);
                        }
                        _ => {
                            self.buffer.put_u8(byte);
                        }
                    }
                }
            }

            // Add delimiter between fields
            if !is_last {
                self.buffer.put_slice(delimiter.as_bytes());
            }

            Ok(())
        } else {
            Err(PgWireError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Text format expected",
            )))
        }
    }

    /// Encode a field in CSV format.
    fn encode_field_csv<T>(&mut self, value: &T, datatype: &Type, is_last: bool) -> PgWireResult<()>
    where
        T: ToSqlText,
    {
        if let CopyFormat::Csv {
            delimiter,
            quote,
            null_string,
            force_quote,
            escape: _,
        } = &self.format
        {
            let col_index = self.col_index;
            let mut temp_buffer = BytesMut::new();
            let is_null =
                value.to_sql_text(datatype, &mut temp_buffer, &FormatOptions::default())?;

            let delimiter_byte = delimiter.as_bytes()[0];
            let quote_byte = quote.as_bytes()[0];
            let null_string_bytes = null_string.as_bytes();

            let should_quote = force_quote.contains(&col_index)
                || match is_null {
                    IsNull::Yes => false, // NULL values are never quoted in CSV (handled by null_string)
                    IsNull::No => {
                        let data = temp_buffer.as_ref();
                        data.contains(&delimiter_byte)
                            || data.contains(&quote_byte)
                            || data.contains(&b'\n')
                            || data.contains(&b'\r')
                            || (!null_string_bytes.is_empty()
                                && data
                                    .windows(null_string_bytes.len())
                                    .any(|w| w == null_string_bytes))
                    }
                };

            if let IsNull::Yes = is_null {
                self.buffer.put_slice(null_string_bytes);
            } else if should_quote {
                self.buffer.put_u8(quote_byte);

                for &byte in temp_buffer.as_ref() {
                    if byte == quote_byte {
                        // Double the quote character
                        self.buffer.put_u8(byte);
                    }
                    self.buffer.put_u8(byte);
                }

                self.buffer.put_u8(quote_byte);
            } else {
                self.buffer.put_slice(temp_buffer.as_ref());
            }

            // Add delimiter between fields
            if !is_last {
                self.buffer.put_slice(delimiter.as_bytes());
            }

            Ok(())
        } else {
            Err(PgWireError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "CSV format expected",
            )))
        }
    }

    /// Take the current row as a CopyData message.
    ///
    /// For binary format: first call includes PGCOPY header.
    /// For text/CSV format: each call returns one row with a trailing newline.
    pub fn take_copy(&mut self) -> CopyData {
        match &self.format {
            CopyFormat::Binary => {
                if !self.header_written {
                    // Prepend header to field data
                    let field_data = self.buffer.split();
                    self.write_pgcop_header();
                    self.buffer.put_i16(self.schema.len() as i16);
                    self.buffer.extend_from_slice(&field_data);
                    self.header_written = true;
                } else {
                    // Prepend field count before field data
                    let field_data = self.buffer.split();
                    self.buffer.put_i16(self.schema.len() as i16);
                    self.buffer.extend_from_slice(&field_data);
                }
            }
            CopyFormat::Text { .. } | CopyFormat::Csv { .. } => {
                // Add newline at end of row
                self.buffer.put_u8(b'\n');
            }
        }

        self.col_index = 0;
        CopyData::new(self.buffer.split().freeze())
    }

    /// Finish the COPY operation of binary format.
    ///
    /// For binary format: returns trailer (-1).
    /// Note that this trailer is automatically appended to stream if you use
    /// `CopyResponse` API.
    pub fn finish_copy_binary() -> CopyData {
        CopyData::new(Bytes::from_static(&[0xFF, 0xFF]))
    }

    /// Write PGCOPY binary header.
    fn write_pgcop_header(&mut self) {
        self.buffer.put_slice(b"PGCOPY\n\xFF\r\n\x00");
        self.buffer.put_i32(0); // Flags (no OIDs)
        self.buffer.put_i32(0); // Header extension length
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
pub struct CopyResponse {
    pub format: i8,
    pub columns: usize,
    pub data_stream: SendableCopyDataStream,
}

impl std::fmt::Debug for CopyResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyResponse")
            .field("format", &self.format)
            .field("columns", &self.columns)
            .finish()
    }
}

impl CopyResponse {
    pub fn new<S>(format: i8, columns: usize, data_stream: S) -> CopyResponse
    where
        S: Stream<Item = PgWireResult<CopyData>> + Send + 'static,
    {
        if format == 1 {
            let data_stream = data_stream.chain(stream::once(future::ready(Ok(
                CopyEncoder::finish_copy_binary(),
            ))));
            CopyResponse {
                format,
                columns,
                data_stream: Box::pin(data_stream),
            }
        } else {
            CopyResponse {
                format,
                columns,
                data_stream: Box::pin(data_stream),
            }
        }
    }

    pub fn data_stream(&mut self) -> &mut SendableCopyDataStream {
        &mut self.data_stream
    }

    pub fn column_formats(&self) -> Vec<i16> {
        (0..self.columns).map(|_| self.format as i16).collect()
    }
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
#[derive(Debug)]
pub enum Response {
    EmptyQuery,
    Query(QueryResponse),
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
    #[cfg(feature = "pg-type-chrono")]
    fn test_data_row_encoder() {
        use std::time::SystemTime;

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

        let row = encoder.take_row();

        assert_eq!(row.field_count, 3);

        let mut expected = BytesMut::new();
        expected.put_i32(4);
        expected.put_slice("2001".as_bytes());
        expected.put_i32(4);
        expected.put_slice("udev".as_bytes());
        expected.put_i32(26);
        let _ = now.to_sql_text(&Type::TIMESTAMP, &mut expected, &FormatOptions::default());
        assert_eq!(row.data, expected);
    }

    #[test]
    fn test_copy_text_options_default() {
        let opts = CopyTextOptions::default();
        assert_eq!(opts.delimiter, "\t");
        assert_eq!(opts.null_string, "\\N");
    }

    #[test]
    fn test_copy_csv_options_default() {
        let opts = CopyCsvOptions::default();
        assert_eq!(opts.delimiter, ",");
        assert_eq!(opts.quote, "\"");
        assert_eq!(opts.escape, "\"");
        assert_eq!(opts.null_string, "");
        assert!(opts.force_quote.is_empty());
    }

    #[test]
    fn test_copy_binary_header() {
        let schema = Arc::new(vec![FieldInfo::new(
            "id".into(),
            None,
            None,
            Type::INT4,
            FieldFormat::Binary,
        )]);
        let mut encoder = CopyEncoder::new_binary(schema.clone());

        // First take_copy should include header
        encoder.encode_field(&42).unwrap();
        let copy_data = encoder.take_copy();

        let data = copy_data.data.as_ref();
        assert_eq!(&data[0..11], b"PGCOPY\n\xFF\r\n\0");

        // Check flags (4 bytes, no OIDs = 0)
        assert_eq!(&data[11..15], &[0x00, 0x00, 0x00, 0x00]);

        // Check extension length (4 bytes, no extensions = 0)
        assert_eq!(&data[15..19], &[0x00, 0x00, 0x00, 0x00]);

        // Check field count (2 bytes)
        assert_eq!(&data[19..21], &[0x00, 0x01]); // 1 field

        // Check field length (4 bytes)
        assert_eq!(&data[21..25], &[0x00, 0x00, 0x00, 0x04]); // 4 bytes

        // Check field value (42 in network byte order)
        assert_eq!(&data[25..29], &[0x00, 0x00, 0x00, 0x2A]);
    }

    #[test]
    fn test_copy_binary_trailer() {
        let copy_data = CopyEncoder::finish_copy_binary();
        let data = copy_data.data.as_ref();

        // Trailer is -1 as i16 (0xFFFF in network byte order)
        assert_eq!(data, &[0xFF, 0xFF]);
    }

    #[test]
    fn test_copy_text_default_delimiter() {
        let schema = Arc::new(vec![
            FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        ]);
        let mut encoder = CopyEncoder::new_text(schema, CopyTextOptions::default());

        encoder.encode_field(&1).unwrap();
        encoder.encode_field(&"Alice").unwrap();
        let copy_data = encoder.take_copy();

        // Expected: "1\tAlice\n"
        assert_eq!(copy_data.data.as_ref(), b"1\tAlice\n");
    }

    #[test]
    fn test_copy_text_custom_delimiter() {
        let schema = Arc::new(vec![
            FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        ]);
        let mut encoder = CopyEncoder::new_text(
            schema,
            CopyTextOptions {
                delimiter: "|".into(),
                null_string: "\\N".into(),
            },
        );

        encoder.encode_field(&1).unwrap();
        encoder.encode_field(&"Alice").unwrap();
        let copy_data = encoder.take_copy();

        // Expected: "1|Alice\n"
        assert_eq!(copy_data.data.as_ref(), b"1|Alice\n");
    }

    #[test]
    fn test_copy_text_null_handling() {
        let schema = Arc::new(vec![
            FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        ]);
        let mut encoder = CopyEncoder::new_text(schema, CopyTextOptions::default());

        encoder.encode_field(&1).unwrap();
        encoder.encode_field(&None::<String>).unwrap();
        let copy_data = encoder.take_copy();

        // Expected: "1\t\\N\n"
        assert_eq!(copy_data.data.as_ref(), b"1\t\\N\n");
    }

    #[test]
    fn test_copy_text_backslash_escaping() {
        let schema = Arc::new(vec![FieldInfo::new(
            "value".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        )]);
        let mut encoder = CopyEncoder::new_text(schema, CopyTextOptions::default());

        encoder.encode_field(&"a\nb\tc\rd\\e").unwrap();
        let copy_data = encoder.take_copy();

        // Expected: "a\\nb\\tc\\rd\\\\e\n"
        assert_eq!(copy_data.data.as_ref(), b"a\\nb\\tc\\rd\\\\e\n");
    }

    #[test]
    fn test_copy_csv_default() {
        let schema = Arc::new(vec![
            FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        ]);
        let mut encoder = CopyEncoder::new_csv(schema, CopyCsvOptions::default());

        encoder.encode_field(&1).unwrap();
        encoder.encode_field(&"Alice").unwrap();
        let copy_data = encoder.take_copy();

        // Expected: "1,Alice\n"
        assert_eq!(copy_data.data.as_ref(), b"1,Alice\n");
    }

    #[test]
    fn test_copy_csv_quoting() {
        let schema = Arc::new(vec![FieldInfo::new(
            "value".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        )]);
        let mut encoder = CopyEncoder::new_csv(schema, CopyCsvOptions::default());

        encoder.encode_field(&"a,b\"c\nd").unwrap();
        let copy_data = encoder.take_copy();

        // Should be quoted because it contains comma and newline
        assert_eq!(copy_data.data.as_ref(), b"\"a,b\"\"c\nd\"\n");
    }

    #[test]
    fn test_copy_csv_force_quote() {
        let schema = Arc::new(vec![
            FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        ]);
        let mut encoder = CopyEncoder::new_csv(
            schema,
            CopyCsvOptions {
                force_quote: vec![1],
                ..Default::default()
            },
        );

        encoder.encode_field(&1).unwrap();
        encoder.encode_field(&"Alice").unwrap();
        let copy_data = encoder.take_copy();

        // Expected: "1,\"Alice\"\n" - second column force quoted
        assert_eq!(copy_data.data.as_ref(), b"1,\"Alice\"\n");
    }

    #[test]
    fn test_copy_binary_multiple_rows() {
        let schema = Arc::new(vec![
            FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Binary),
            FieldInfo::new(
                "name".into(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Binary,
            ),
        ]);
        let mut encoder = CopyEncoder::new_binary(schema);

        // First row
        encoder.encode_field(&1i32).unwrap();
        encoder.encode_field(&"Alice".to_string()).unwrap();
        let copy_data1 = encoder.take_copy();

        // Second row
        encoder.encode_field(&2i32).unwrap();
        encoder.encode_field(&"Bob".to_string()).unwrap();
        let copy_data2 = encoder.take_copy();

        // Verify first row format
        let data1 = copy_data1.data.as_ref();

        // Header is 19 bytes, then field count (2 bytes)
        assert_eq!(&data1[19..21], &[0x00, 0x02]); // 2 fields

        // Verify second row format
        let data2 = copy_data2.data.as_ref();

        // Field count should be at the beginning (no header on second row)
        assert_eq!(&data2[0..2], &[0x00, 0x02]); // 2 fields
    }
}
