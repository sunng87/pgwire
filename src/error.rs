use postgres_types::Oid;
use thiserror::Error;

use crate::messages::response::{ErrorResponse, NoticeResponse};

#[derive(Error, Debug)]
pub enum PgWireError {
    #[error("Invalid message recevied, expect {0:?}, received {1:?}")]
    InvalidMessageType(u8, u8),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Portal not found for name: {0:?}")]
    PortalNotFound(String),
    #[error("Statement not found for name: {0:?}")]
    StatementNotFound(String),
    #[error("Unknown type: {0:?}")]
    UnknownTypeId(Oid),
    #[error("Parameter index out of bound: {0:?}")]
    ParameterIndexOutOfBound(usize),
    #[error("Parameter type index out of bound: {0:?}")]
    ParameterTypeIndexOutOfBound(usize),
    #[error("Cannot convert postgre type {0:?} to given rust type")]
    InvalidRustTypeForParameter(String),
    #[error("Failed to parse parameter: {0:?}")]
    FailedToParseParameter(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Api Error: {0:?}")]
    ApiError(Box<dyn std::error::Error + 'static + Send>),

    #[error("User Error")]
    UserError(ErrorNoticeFields),
}

pub type PgWireResult<T> = Result<T, PgWireError>;

// TODO: distinguish fatal error and recoverable error

// Postgres error and notice message fields
// This part of protocol is defined in
// https://www.postgresql.org/docs/8.2/protocol-error-fields.html
#[derive(new, Setters, Getters, Debug)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct ErrorNoticeFields {
    // severity can be one of `ERROR`, `FATAL`, or `PANIC` (in an error
    // message), or `WARNING`, `NOTICE`, `DEBUG`, `INFO`, or `LOG` (in a notice
    // message), or a localized translation of one of these.
    severity: String,
    // error code defined in
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    code: String,
    // readable message
    message: String,
    // optional secondary message
    #[new(default)]
    detail: Option<String>,
    // optional suggestion for fixing the issue
    #[new(default)]
    hint: Option<String>,
    // Position: the field value is a decimal ASCII integer, indicating an error
    // cursor position as an index into the original query string.
    #[new(default)]
    position: Option<String>,
    // Internal position: this is defined the same as the P field, but it is
    // used when the cursor position refers to an internally generated command
    // rather than the one submitted by the client
    #[new(default)]
    internal_position: Option<String>,
    // Internal query: the text of a failed internally-generated command.
    #[new(default)]
    internal_query: Option<String>,
    // Where: an indication of the context in which the error occurred.
    #[new(default)]
    where_context: Option<String>,
    // File: the file name of the source-code location where the error was
    // reported.
    #[new(default)]
    file_name: Option<String>,
    // Line: the line number of the source-code location where the error was
    // reported.
    #[new(default)]
    line: Option<usize>,
    // Routine: the name of the source-code routine reporting the error.
    #[new(default)]
    routine: Option<String>,
}

impl ErrorNoticeFields {
    fn into_fields(self) -> Vec<(u8, String)> {
        let mut fields = Vec::with_capacity(11);

        fields.push((b'S', self.severity));
        fields.push((b'C', self.code));
        fields.push((b'M', self.message));
        if let Some(value) = self.detail {
            fields.push((b'D', value));
        }
        if let Some(value) = self.hint {
            fields.push((b'H', value));
        }
        if let Some(value) = self.position {
            fields.push((b'P', value));
        }
        if let Some(value) = self.internal_position {
            fields.push((b'p', value));
        }
        if let Some(value) = self.internal_query {
            fields.push((b'q', value));
        }
        if let Some(value) = self.where_context {
            fields.push((b'W', value));
        }
        if let Some(value) = self.file_name {
            fields.push((b'F', value));
        }
        if let Some(value) = self.line {
            fields.push((b'L', value.to_string()));
        }
        if let Some(value) = self.routine {
            fields.push((b'R', value));
        }

        fields
    }
}

impl Into<ErrorResponse> for ErrorNoticeFields {
    fn into(self) -> ErrorResponse {
        ErrorResponse::new(self.into_fields())
    }
}

impl Into<NoticeResponse> for ErrorNoticeFields {
    fn into(self) -> NoticeResponse {
        NoticeResponse::new(self.into_fields())
    }
}
