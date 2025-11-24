use std::fmt::Display;
use std::io::Error as IOError;
use thiserror::Error;

use crate::messages::response::{ErrorResponse, NoticeResponse};

#[derive(Error, Debug)]
pub enum PgWireError {
    #[error("Unsupported protocol version, received {0}.{1}")]
    UnsupportedProtocolVersion(u16, u16),
    #[error("Invalid CancelRequest message, code mismatch")]
    InvalidCancelRequest,
    #[error("Secret key length must be in [4, 256]")]
    InvalidSecretKey,
    #[error("Invalid message recevied, received {0}")]
    InvalidMessageType(u8),
    #[error("Invalid message length, expected max {0}, actual: {1}")]
    MessageTooLarge(usize, usize),
    #[error("Invalid target type, received {0}")]
    InvalidTargetType(u8),
    #[error("Invalid transaction status, received {0}")]
    InvalidTransactionStatus(u8),
    #[error("Invalid ssl request message")]
    InvalidSSLRequestMessage,
    #[error("Invalid gss encrypt request message")]
    InvalidGssEncRequestMessage,
    #[error("Invalid startup message")]
    InvalidStartupMessage,
    #[error("Invalid authentication message code: {0}")]
    InvalidAuthenticationMessageCode(i32),
    #[error("Invalid password message type, failed to coerce")]
    FailedToCoercePasswordMessage,
    #[error("Invalid SASL state")]
    InvalidSASLState,
    #[error("Unsupported SASL authentication method {0}")]
    UnsupportedSASLAuthMethod(String),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Portal not found for name: {0}")]
    PortalNotFound(String),
    #[error("Statement not found for name: {0}")]
    StatementNotFound(String),
    #[error("Parameter index out of bound: {0}")]
    ParameterIndexOutOfBound(usize),
    #[error("Cannot convert postgres type {0} to given rust type")]
    InvalidRustTypeForParameter(String),
    #[error("Failed to parse parameter: {0}")]
    FailedToParseParameter(Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to parse scram message: {0}")]
    InvalidScramMessage(String),
    #[error("Password authentication failed for user \"{0}\"")]
    InvalidPassword(String),
    #[error("Certificate algorithm is not supported")]
    UnsupportedCertificateSignatureAlgorithm,
    #[error("Username is required")]
    UserNameRequired,
    #[error("Connection is not ready for query")]
    NotReadyForQuery,
    #[error("Invalid option value {0}")]
    InvalidOptionValue(String),

    #[error(transparent)]
    ApiError(#[from] Box<dyn std::error::Error + 'static + Send + Sync>),

    #[error("User provided error: {0}")]
    UserError(Box<ErrorInfo>),
}

impl From<PgWireError> for IOError {
    fn from(e: PgWireError) -> Self {
        IOError::other(e)
    }
}

pub type PgWireResult<T> = Result<T, PgWireError>;

// Postgres error and notice message fields
// This part of protocol is defined in
// https://www.postgresql.org/docs/8.2/protocol-error-fields.html
#[non_exhaustive]
#[derive(new, Debug, Default)]
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

    pub fn is_fatal(&self) -> bool {
        self.severity == "FATAL"
    }
}

impl From<ErrorInfo> for ErrorResponse {
    fn from(ei: ErrorInfo) -> ErrorResponse {
        ErrorResponse::new(ei.into_fields())
    }
}

impl From<ErrorResponse> for ErrorInfo {
    fn from(value: ErrorResponse) -> Self {
        let mut error_info = ErrorInfo::default();
        for field in value.fields {
            let (key, value) = field;
            match key {
                b'S' => {
                    error_info.severity = value;
                }
                b'C' => {
                    error_info.code = value;
                }
                b'M' => {
                    error_info.message = value;
                }
                b'D' => {
                    error_info.detail = Some(value);
                }
                b'H' => {
                    error_info.hint = Some(value);
                }
                b'P' => {
                    error_info.position = Some(value);
                }
                b'p' => {
                    error_info.internal_position = Some(value);
                }
                b'q' => {
                    error_info.internal_query = Some(value);
                }
                b'W' => {
                    error_info.where_context = Some(value);
                }
                b'F' => {
                    error_info.file_name = Some(value);
                }
                b'L' => {
                    error_info.line = Some(value.parse().unwrap_or(0));
                }
                b'R' => {
                    error_info.routine = Some(value);
                }
                _ => {}
            }
        }
        error_info
    }
}

impl From<ErrorInfo> for NoticeResponse {
    fn from(ei: ErrorInfo) -> NoticeResponse {
        NoticeResponse::new(ei.into_fields())
    }
}

impl From<PgWireError> for ErrorInfo {
    fn from(error: PgWireError) -> Self {
        match error {
            PgWireError::UnsupportedProtocolVersion(_, _) => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::InvalidCancelRequest => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::InvalidMessageType(_) => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::InvalidTargetType(_) => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::MessageTooLarge(..) => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::InvalidTransactionStatus(_) => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::InvalidSSLRequestMessage => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::InvalidGssEncRequestMessage => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::InvalidStartupMessage => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::InvalidAuthenticationMessageCode(_) => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::FailedToCoercePasswordMessage => {
                ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), error.to_string())
            }
            PgWireError::InvalidSASLState => {
                ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), error.to_string())
            }
            PgWireError::UnsupportedSASLAuthMethod(_) => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::IoError(_) => {
                ErrorInfo::new("FATAL".to_owned(), "58030".to_owned(), error.to_string())
            }
            PgWireError::PortalNotFound(_) => {
                ErrorInfo::new("ERROR".to_owned(), "26000".to_owned(), error.to_string())
            }
            PgWireError::StatementNotFound(_) => {
                ErrorInfo::new("ERROR".to_owned(), "26000".to_owned(), error.to_string())
            }
            PgWireError::ParameterIndexOutOfBound(_) => {
                ErrorInfo::new("ERROR".to_owned(), "22023".to_owned(), error.to_string())
            }
            PgWireError::InvalidRustTypeForParameter(_) => {
                ErrorInfo::new("ERROR".to_owned(), "22023".to_owned(), error.to_string())
            }
            PgWireError::FailedToParseParameter(_) => {
                ErrorInfo::new("ERROR".to_owned(), "22P02".to_owned(), error.to_string())
            }
            PgWireError::InvalidScramMessage(_) => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::InvalidPassword(_) => {
                ErrorInfo::new("FATAL".to_owned(), "28P01".to_owned(), error.to_string())
            }
            PgWireError::UnsupportedCertificateSignatureAlgorithm => {
                ErrorInfo::new("FATAL".to_owned(), "0A000".to_owned(), error.to_string())
            }
            PgWireError::UserNameRequired => {
                ErrorInfo::new("FATAL".to_owned(), "28000".to_owned(), error.to_string())
            }
            PgWireError::NotReadyForQuery => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::InvalidSecretKey => {
                ErrorInfo::new("FATAL".to_owned(), "08P01".to_owned(), error.to_string())
            }
            PgWireError::ApiError(_) => {
                ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), error.to_string())
            }
            PgWireError::UserError(info) => *info,
            PgWireError::InvalidOptionValue(_) => {
                ErrorInfo::new("ERROR".to_owned(), "22023".to_owned(), error.to_string())
            }
        }
    }
}

#[cfg(feature = "client-api")]
#[derive(Error, Debug)]
pub enum PgWireClientError {
    #[error("Failed to parse connection config, invalid value for: {0}")]
    InvalidConfig(String),

    #[error("Failed to parse connection config, unknown config: {0}")]
    UnknownConfig(String),

    #[error("Failed to parse utf8 value")]
    InvalidUtf8ConfigValue(#[source] std::str::Utf8Error),

    #[error("Unexpected EOF")]
    UnexpectedEOF,

    #[error("Unexpected remote message")]
    UnexpectedMessage(Box<crate::messages::PgWireBackendMessage>),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    PgWireError(#[from] PgWireError),

    #[error("Error received from remote server: {0}")]
    RemoteError(Box<ErrorInfo>),

    #[error("Error parse command tag: {0}")]
    InvalidTag(Box<dyn std::error::Error>),

    #[error("ALPN postgresql is required for direct connect.")]
    AlpnRequired,

    #[error("Failed to parse data: {0}")]
    FromSqlError(Box<dyn std::error::Error + Send + Sync>),

    #[error("Index out of bounds")]
    DataRowIndexOutOfBounds,
}

#[cfg(feature = "client-api")]
impl From<ErrorInfo> for PgWireClientError {
    fn from(ei: ErrorInfo) -> PgWireClientError {
        PgWireClientError::RemoteError(Box::new(ei))
    }
}

#[cfg(feature = "client-api")]
pub type PgWireClientResult<T> = Result<T, PgWireClientError>;

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
