mod binary;
mod csv;
mod text;

use bytes::{BufMut, BytesMut};

use self::binary::BinaryDecoder;
use self::csv::CsvDecoder;
use self::text::TextDecoder;
use crate::api::results::{CopyCsvOptions, CopyTextOptions};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::copy::CopyData;
use crate::messages::data::DataRow;

/// Incrementally decodes PostgreSQL binary, text, or CSV COPY data into rows.
///
/// A decoder accepts arbitrary [`CopyData`] message boundaries. Complete rows
/// are returned by [`push`](Self::push), while a valid partial row is retained
/// until more data arrives or [`finish`](Self::finish) is called.
///
/// Keep one decoder for the duration of a COPY operation. Call `push` from the
/// COPY data handler and `finish` when COPY completes. After an error or a call
/// to `finish`, discard the decoder; subsequent calls return an error.
///
/// ```
/// use bytes::Bytes;
/// use pgwire::api::copy::CopyDecoder;
/// use pgwire::api::results::CopyTextOptions;
/// use pgwire::messages::copy::CopyData;
///
/// # fn example() -> pgwire::error::PgWireResult<()> {
/// let mut decoder = CopyDecoder::new_text(2, CopyTextOptions::default())?;
/// assert!(decoder.push(CopyData::new(Bytes::from_static(b"first\t")))?.is_empty());
/// let rows = decoder.push(CopyData::new(Bytes::from_static(b"second\n")))?;
/// assert_eq!(rows.len(), 1);
/// assert!(decoder.finish()?.is_empty());
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct CopyDecoder {
    state: DecoderState,
    terminal: bool,
}

#[derive(Debug)]
enum DecoderState {
    Binary(BinaryDecoder),
    Text(TextDecoder),
    Csv(CsvDecoder),
}

impl CopyDecoder {
    /// Create a decoder for PostgreSQL binary COPY data.
    pub fn new_binary(columns: usize) -> PgWireResult<Self> {
        let columns = validate_columns(columns)?;
        Ok(Self {
            state: DecoderState::Binary(BinaryDecoder::new(columns)),
            terminal: false,
        })
    }

    /// Create a decoder for PostgreSQL text COPY data.
    pub fn new_text(columns: usize, options: CopyTextOptions) -> PgWireResult<Self> {
        let columns = validate_columns(columns)?;
        let delimiter = validate_delimiter(&options.delimiter)?;
        if b"\\.abcdefghijklmnopqrstuvwxyz0123456789".contains(&delimiter) {
            return Err(invalid_option(
                "COPY text delimiter cannot be a backslash, period, lowercase letter, or digit",
            ));
        }
        let null_string = options.null_string.as_bytes().to_vec();
        validate_null_string(&null_string, delimiter, None)?;

        Ok(Self {
            state: DecoderState::Text(TextDecoder::new(columns, delimiter, null_string)),
            terminal: false,
        })
    }

    /// Create a decoder for PostgreSQL CSV COPY data.
    pub fn new_csv(columns: usize, options: CopyCsvOptions) -> PgWireResult<Self> {
        let columns = validate_columns(columns)?;
        let delimiter = validate_delimiter(&options.delimiter)?;
        let quote = validate_single_byte("quote", &options.quote)?;
        let escape = validate_single_byte("escape", &options.escape)?;
        if delimiter == quote {
            return Err(invalid_option(
                "COPY CSV delimiter and quote must be different",
            ));
        }
        let null_string = options.null_string.as_bytes().to_vec();
        validate_null_string(&null_string, delimiter, Some(quote))?;

        Ok(Self {
            state: DecoderState::Csv(CsvDecoder::new(
                columns,
                delimiter,
                quote,
                escape,
                null_string,
            )),
            terminal: false,
        })
    }

    /// Decode one COPY data message and return every complete row it contains.
    pub fn push(&mut self, data: CopyData) -> PgWireResult<Vec<DataRow>> {
        if self.terminal {
            return Err(invalid_data("COPY decoder is already terminal"));
        }

        let result = match &mut self.state {
            DecoderState::Binary(decoder) => decoder.push(data.data.as_ref()),
            DecoderState::Text(decoder) => decoder.push(data.data.as_ref()),
            DecoderState::Csv(decoder) => decoder.push(data.data.as_ref()),
        };
        if result.is_err() {
            self.terminal = true;
        }
        result
    }

    /// Finish the COPY stream and return a final unterminated text or CSV row.
    pub fn finish(&mut self) -> PgWireResult<Vec<DataRow>> {
        if self.terminal {
            return Err(invalid_data("COPY decoder is already terminal"));
        }

        let result = match &mut self.state {
            DecoderState::Binary(decoder) => decoder.finish(),
            DecoderState::Text(decoder) => decoder.finish(),
            DecoderState::Csv(decoder) => decoder.finish(),
        };
        self.terminal = true;
        result
    }
}

fn validate_columns(columns: usize) -> PgWireResult<i16> {
    let columns = i16::try_from(columns)
        .map_err(|_| invalid_option("COPY column count must be in 1..=i16::MAX"))?;
    if columns == 0 {
        return Err(invalid_option("COPY column count must be in 1..=i16::MAX"));
    }
    Ok(columns)
}

fn validate_delimiter(value: &str) -> PgWireResult<u8> {
    let delimiter = validate_single_byte("delimiter", value)?;
    if matches!(delimiter, b'\r' | b'\n') {
        return Err(invalid_option(
            "COPY delimiter cannot be carriage return or newline",
        ));
    }
    Ok(delimiter)
}

fn validate_single_byte(name: &str, value: &str) -> PgWireResult<u8> {
    let bytes = value.as_bytes();
    if bytes.len() != 1 {
        return Err(invalid_option(format!(
            "COPY {name} must be exactly one byte"
        )));
    }
    Ok(bytes[0])
}

fn validate_null_string(
    null_string: &[u8],
    delimiter: u8,
    quote_byte: Option<u8>,
) -> PgWireResult<()> {
    if null_string.iter().any(|byte| {
        matches!(byte, b'\r' | b'\n') || *byte == delimiter || Some(*byte) == quote_byte
    }) {
        return Err(invalid_option(
            "COPY null string contains a delimiter, quote, carriage return, or newline",
        ));
    }
    Ok(())
}

fn invalid_option(message: impl Into<String>) -> PgWireError {
    PgWireError::InvalidOptionValue(message.into())
}

pub(super) fn invalid_data(message: impl Into<String>) -> PgWireError {
    PgWireError::IoError(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    ))
}

pub(super) fn build_row(columns: i16, fields: Vec<Option<Vec<u8>>>) -> PgWireResult<DataRow> {
    if fields.len() != columns as usize {
        return Err(invalid_data(format!(
            "COPY row has {} fields, expected {columns}",
            fields.len()
        )));
    }

    let mut data = BytesMut::new();
    for field in fields {
        match field {
            None => data.put_i32(-1),
            Some(value) => {
                let length = i32::try_from(value.len())
                    .map_err(|_| invalid_data("COPY field is larger than i32::MAX"))?;
                data.put_i32(length);
                data.extend_from_slice(&value);
            }
        }
    }
    Ok(DataRow::new(data, columns))
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum RecordEnding {
    Lf,
    Cr,
    CrLf,
}

pub(super) fn accept_record_ending(
    current: &mut Option<RecordEnding>,
    next: RecordEnding,
) -> PgWireResult<()> {
    match current {
        None => {
            *current = Some(next);
            Ok(())
        }
        Some(value) if *value == next => Ok(()),
        Some(_) => Err(invalid_data("mixed COPY record-ending styles")),
    }
}
