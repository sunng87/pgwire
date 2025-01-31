use std::fmt::Display;
use std::io::{Error as IOError, ErrorKind};
use thiserror::Error;

use crate::messages::response::{ErrorResponse, NoticeResponse};

#[derive(Error, Debug)]
pub enum PgWireError {
    #[error("Invalid protocol version, received {0}")]
    InvalidProtocolVersion(i32),
    #[error("Invalid message recevied, received {0}")]
    InvalidMessageType(u8),
    #[error("Invalid target type, received {0}")]
    InvalidTargetType(u8),
    #[error("Invalid transaction status, received {0}")]
    InvalidTransactionStatus(u8),
    #[error("Invalid startup message")]
    InvalidStartupMessage,
    #[error("Invalid authentication message code: {0}")]
    InvalidAuthenticationMessageCode(i32),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Portal not found for name: {0}")]
    PortalNotFound(String),
    #[error("Statement not found for name: {0}")]
    StatementNotFound(String),
    #[error("Parameter index out of bound: {0}")]
    ParameterIndexOutOfBound(usize),
    #[error("Cannot convert postgre type {0} to given rust type")]
    InvalidRustTypeForParameter(String),
    #[error("Failed to parse parameter: {0}")]
    FailedToParseParameter(Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to parse scram message: {0}")]
    InvalidScramMessage(String),
    #[error("Certificate algorithm is not supported")]
    UnsupportedCertificateSignatureAlgorithm,
    #[error("Username is required")]
    UserNameRequired,
    #[error("Connection is not ready for query")]
    NotReadyForQuery,
    #[cfg(feature = "client-api")]
    #[error("Failed to parse connection config, invalid value for: {0}")]
    InvalidConfig(String),
    #[cfg(feature = "client-api")]
    #[error("Failed to parse connection config, unknown config: {0}")]
    UnknownConfig(String),
    #[cfg(feature = "client-api")]
    #[error("Failed to parse utf8 value")]
    InvalidUtf8ConfigValue(#[source] std::str::Utf8Error),
    #[cfg(feature = "client-api")]
    #[error("Failed to send front message")]
    ClientMessageSendError(
        #[source] tokio::sync::mpsc::error::SendError<crate::messages::PgWireFrontendMessage>,
    ),

    #[error(transparent)]
    ApiError(#[from] Box<dyn std::error::Error + 'static + Send + Sync>),

    #[error("User provided error: {0}")]
    UserError(Box<ErrorInfo>),
}

impl From<PgWireError> for IOError {
    fn from(e: PgWireError) -> Self {
        IOError::new(ErrorKind::Other, e)
    }
}

pub type PgWireResult<T> = Result<T, PgWireError>;

// Postgres error and notice message fields
// This part of protocol is defined in
// https://www.postgresql.org/docs/8.2/protocol-error-fields.html
#[non_exhaustive]
#[derive(new, Debug)]
pub struct ErrorInfo {
    // severity can be one of `ERROR`, `FATAL`, or `PANIC` (in an error
    // message), or `WARNING`, `NOTICE`, `DEBUG`, `INFO`, or `LOG` (in a notice
    // message), or a localized translation of one of these.
    pub severity: String,
    // error code defined in
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    pub code: String,
    // readable message
    pub message: String,
    // optional secondary message
    #[new(default)]
    pub detail: Option<String>,
    // optional suggestion for fixing the issue
    #[new(default)]
    pub hint: Option<String>,
    // Position: the field value is a decimal ASCII integer, indicating an error
    // cursor position as an index into the original query string.
    #[new(default)]
    pub position: Option<String>,
    // Internal position: this is defined the same as the P field, but it is
    // used when the cursor position refers to an internally generated command
    // rather than the one submitted by the client
    #[new(default)]
    pub internal_position: Option<String>,
    // Internal query: the text of a failed internally-generated command.
    #[new(default)]
    pub internal_query: Option<String>,
    // Where: an indication of the context in which the error occurred.
    #[new(default)]
    pub where_context: Option<String>,
    // File: the file name of the source-code location where the error was
    // reported.
    #[new(default)]
    pub file_name: Option<String>,
    // Line: the line number of the source-code location where the error was
    // reported.
    #[new(default)]
    pub line: Option<usize>,
    // Routine: the name of the source-code routine reporting the error.
    #[new(default)]
    pub routine: Option<String>,
}

impl Display for ErrorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ErrorInfo: {}, {}, {}",
            self.severity, self.code, self.message
        )
    }
}

impl ErrorInfo {
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

impl From<ErrorInfo> for ErrorResponse {
    fn from(ei: ErrorInfo) -> ErrorResponse {
        ErrorResponse::new(ei.into_fields())
    }
}

impl From<ErrorInfo> for NoticeResponse {
    fn from(ei: ErrorInfo) -> NoticeResponse {
        NoticeResponse::new(ei.into_fields())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_error_notice_info() {
        let error_info = ErrorInfo::new(
            "FATAL".to_owned(),
            "28P01".to_owned(),
            "Password authentication failed".to_owned(),
        );
        assert_eq!("FATAL", error_info.severity);
        assert_eq!("28P01", error_info.code);
        assert_eq!("Password authentication failed", error_info.message);
        assert!(error_info.file_name.is_none());
    }
}
