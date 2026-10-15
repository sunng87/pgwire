use std::fmt::Display;
use std::io::Error as IOError;
use thiserror::Error;

use crate::messages::response::{ErrorResponse, NoticeResponse};

pub const SEVERITY_DEBUG1: &str = "DEBUG1";
pub const SEVERITY_DEBUG2: &str = "DEBUG2";
pub const SEVERITY_DEBUG3: &str = "DEBUG3";
pub const SEVERITY_DEBUG4: &str = "DEBUG4";
pub const SEVERITY_DEBUG5: &str = "DEBUG5";
pub const SEVERITY_INFO: &str = "INFO";
pub const SEVERITY_LOG: &str = "LOG";
pub const SEVERITY_NOTICE: &str = "NOTICE";
pub const SEVERITY_WARNING: &str = "WARNING";
pub const SEVERITY_ERROR: &str = "ERROR";
pub const SEVERITY_FATAL: &str = "FATAL";
pub const SEVERITY_PANIC: &str = "PANIC";

/// Error type for pgwire server-side operations.
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
    #[error("Invalid element count {0}: {1} bytes required, {2} remaining")]
    InvalidElementCount(usize, usize, usize),
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
    #[error("Cannot fetch portal in its Initial state, call start() first")]
    PortalNotStarted,
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
    #[error("canceling statement due to user request")]
    QueryCanceled,
    #[error("Invalid option value {0}")]
    InvalidOptionValue(String),
    #[error("{0}")]
    InvalidOauthMessage(String),
    #[error("{0}")]
    OAuthAuthenticationFailed(String),
    #[error("{0}")]
    OAuthValidationError(String),
    #[error("{0}")]
    OauthAuthzIdError(String),
    #[error(transparent)]
    ApiError(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("User provided error: {0}")]
    UserError(Box<ErrorInfo>),
}

impl From<PgWireError> for IOError {
    fn from(e: PgWireError) -> Self {
        IOError::other(e)
    }
}

/// Result type for pgwire server-side operations.
pub type PgWireResult<T> = Result<T, PgWireError>;

/// Postgres error and notice message fields.
///
/// This part of protocol is defined in
/// <https://www.postgresql.org/docs/18/protocol-error-fields.html>
#[non_exhaustive]
#[derive(new, Debug, Default)]
pub struct ErrorInfo {
    /// Severity can be one of `ERROR`, `FATAL`, or `PANIC` (in an error
    /// message), or `WARNING`, `NOTICE`, `DEBUG`, `INFO`, or `LOG` (in a notice
    /// message), or a localized translation of one of these.
    pub severity: String,
    /// Error code defined in
    /// <https://www.postgresql.org/docs/current/errcodes-appendix.html>.
    pub code: String,
    /// Readable message.
    pub message: String,
    /// Optional secondary message.
    #[new(default)]
    pub detail: Option<String>,
    /// Optional suggestion for fixing the issue.
    #[new(default)]
    pub hint: Option<String>,
    /// Position: the field value is a decimal ASCII integer, indicating an error
    /// cursor position as an index into the original query string.
    #[new(default)]
    pub position: Option<String>,
    /// Internal position: this is defined the same as the P field, but it is
    /// used when the cursor position refers to an internally generated command
    /// rather than the one submitted by the client.
    #[new(default)]
    pub internal_position: Option<String>,
    /// Internal query: the text of a failed internally-generated command.
    #[new(default)]
    pub internal_query: Option<String>,
    /// Where: an indication of the context in which the error occurred.
    #[new(default)]
    pub where_context: Option<String>,
    /// File: the file name of the source-code location where the error was reported.
    #[new(default)]
    pub file_name: Option<String>,
    /// Line: the line number of the source-code location where the error was reported.
    #[new(default)]
    pub line: Option<usize>,
    /// Routine: the name of the source-code routine reporting the error.
    #[new(default)]
    pub routine: Option<String>,
    /// Severity: identical to the S field except that the contents are never localized.
    /// This is present only in messages generated by PostgreSQL versions 9.6 and later.
    #[new(default)]
    pub severity_nonlocalized: Option<String>,
    /// Schema name: if the error was associated with a specific database object,
    /// the name of the schema containing that object, if any.
    #[new(default)]
    pub schema: Option<String>,
    /// Table name: if the error was associated with a specific table, the name of the table.
    #[new(default)]
    pub table: Option<String>,
    /// Column name: if the error was associated with a specific table column,
    /// the name of the column.
    #[new(default)]
    pub column: Option<String>,
    /// Data type name: if the error was associated with a specific data type,
    /// the name of the data type.
    #[new(default)]
    pub datatype: Option<String>,
    /// Constraint name: if the error was associated with a specific constraint,
    /// the name of the constraint.
    #[new(default)]
    pub constraint: Option<String>,
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
        let mut fields = Vec::with_capacity(18);

        fields.push((b'S', self.severity));
        if let Some(value) = self.severity_nonlocalized {
            fields.push((b'V', value));
        }
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
        if let Some(value) = self.schema {
            fields.push((b's', value));
        }
        if let Some(value) = self.table {
            fields.push((b't', value));
        }
        if let Some(value) = self.column {
            fields.push((b'c', value));
        }
        if let Some(value) = self.datatype {
            fields.push((b'd', value));
        }
        if let Some(value) = self.constraint {
            fields.push((b'n', value));
        }

        fields
    }

    /// Returns `true` if the error severity is `FATAL` or `PANIC`.
    pub fn is_fatal(&self) -> bool {
        self.severity == SEVERITY_FATAL || self.severity == SEVERITY_PANIC
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
                b'V' => {
                    error_info.severity_nonlocalized = Some(value);
                }
                b's' => {
                    error_info.schema = Some(value);
                }
                b't' => {
                    error_info.table = Some(value);
                }
                b'c' => {
                    error_info.column = Some(value);
                }
                b'd' => {
                    error_info.datatype = Some(value);
                }
                b'n' => {
                    error_info.constraint = Some(value);
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

impl PgWireError {
    /// Returns the severity this error is sent to the client with. A `FATAL`
    /// error ends the connection.
    pub fn severity(&self) -> &str {
        match self {
            PgWireError::UnsupportedProtocolVersion(_, _)
            | PgWireError::InvalidCancelRequest
            | PgWireError::InvalidMessageType(_)
            | PgWireError::InvalidTargetType(_)
            | PgWireError::MessageTooLarge(..)
            | PgWireError::InvalidElementCount(..)
            | PgWireError::InvalidTransactionStatus(_)
            | PgWireError::InvalidSSLRequestMessage
            | PgWireError::InvalidGssEncRequestMessage
            | PgWireError::InvalidStartupMessage
            | PgWireError::InvalidAuthenticationMessageCode(_)
            | PgWireError::FailedToCoercePasswordMessage
            | PgWireError::InvalidSASLState
            | PgWireError::UnsupportedSASLAuthMethod(_)
            | PgWireError::IoError(_)
            | PgWireError::InvalidScramMessage(_)
            | PgWireError::InvalidPassword(_)
            | PgWireError::UnsupportedCertificateSignatureAlgorithm
            | PgWireError::UserNameRequired
            | PgWireError::NotReadyForQuery
            | PgWireError::InvalidSecretKey
            | PgWireError::InvalidOauthMessage(_)
            | PgWireError::OAuthAuthenticationFailed(_)
            | PgWireError::OAuthValidationError(_)
            | PgWireError::OauthAuthzIdError(_) => SEVERITY_FATAL,
            PgWireError::PortalNotFound(_)
            | PgWireError::PortalNotStarted
            | PgWireError::StatementNotFound(_)
            | PgWireError::ParameterIndexOutOfBound(_)
            | PgWireError::InvalidRustTypeForParameter(_)
            | PgWireError::FailedToParseParameter(_)
            | PgWireError::QueryCanceled
            | PgWireError::ApiError(_)
            | PgWireError::InvalidOptionValue(_) => SEVERITY_ERROR,
            PgWireError::UserError(info) => &info.severity,
        }
    }

    /// Returns `true` if the error severity is `FATAL` or `PANIC`.
    pub fn is_fatal(&self) -> bool {
        self.severity() == SEVERITY_FATAL || self.severity() == SEVERITY_PANIC
    }

    /// Returns the SQLSTATE code this error is sent to the client with.
    pub fn code(&self) -> &str {
        match self {
            PgWireError::UnsupportedProtocolVersion(_, _)
            | PgWireError::InvalidCancelRequest
            | PgWireError::InvalidMessageType(_)
            | PgWireError::InvalidTargetType(_)
            | PgWireError::MessageTooLarge(..)
            | PgWireError::InvalidElementCount(..)
            | PgWireError::InvalidTransactionStatus(_)
            | PgWireError::InvalidSSLRequestMessage
            | PgWireError::InvalidGssEncRequestMessage
            | PgWireError::InvalidStartupMessage
            | PgWireError::InvalidAuthenticationMessageCode(_)
            | PgWireError::UnsupportedSASLAuthMethod(_)
            | PgWireError::InvalidScramMessage(_)
            | PgWireError::NotReadyForQuery
            | PgWireError::InvalidSecretKey
            | PgWireError::InvalidOauthMessage(_)
            | PgWireError::OAuthAuthenticationFailed(_)
            | PgWireError::OAuthValidationError(_) => "08P01",
            PgWireError::FailedToCoercePasswordMessage
            | PgWireError::InvalidSASLState
            | PgWireError::PortalNotStarted
            | PgWireError::ApiError(_) => "XX000",
            PgWireError::IoError(_) => "58030",
            PgWireError::PortalNotFound(_) | PgWireError::StatementNotFound(_) => "26000",
            PgWireError::ParameterIndexOutOfBound(_)
            | PgWireError::InvalidRustTypeForParameter(_)
            | PgWireError::InvalidOptionValue(_) => "22023",
            PgWireError::FailedToParseParameter(_) => "22P02",
            PgWireError::InvalidPassword(_) => "28P01",
            PgWireError::UnsupportedCertificateSignatureAlgorithm
            | PgWireError::OauthAuthzIdError(_) => "0A000",
            PgWireError::UserNameRequired => "28000",
            PgWireError::QueryCanceled => "57014",
            PgWireError::UserError(info) => &info.code,
        }
    }
}

impl From<PgWireError> for ErrorInfo {
    fn from(error: PgWireError) -> Self {
        match error {
            PgWireError::UserError(info) => *info,
            error => ErrorInfo::new(
                error.severity().to_owned(),
                error.code().to_owned(),
                error.to_string(),
            ),
        }
    }
}

#[cfg(feature = "client-api")]
/// Error type for pgwire client-side operations.
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
    InvalidTag(Box<dyn std::error::Error + Send + Sync>),

    #[error("ALPN postgresql is required for direct connect.")]
    AlpnRequired,

    #[error("Failed to parse data: {0}")]
    FromSqlError(Box<dyn std::error::Error + Send + Sync>),

    #[error("Index out of bounds")]
    DataRowIndexOutOfBounds,

    #[error("Error from SCRAM authentication server: {0}")]
    ScramError(String),

    #[error("None of the server SASL auth methods is not supported by the client: {0:?}")]
    UnsupportedSASLAuthMethods(Vec<String>),
}

#[cfg(feature = "client-api")]
impl From<ErrorInfo> for PgWireClientError {
    fn from(ei: ErrorInfo) -> PgWireClientError {
        PgWireClientError::RemoteError(Box::new(ei))
    }
}

#[cfg(feature = "client-api")]
/// Result type for pgwire client-side operations.
pub type PgWireClientResult<T> = Result<T, PgWireClientError>;

#[cfg(test)]
mod test {
    use super::*;
    use std::error::Error;

    #[test]
    fn test_error_notice_info() {
        let error_info = ErrorInfo::new(
            SEVERITY_FATAL.to_owned(),
            "28P01".to_owned(),
            "Password authentication failed".to_owned(),
        );
        assert_eq!(SEVERITY_FATAL, error_info.severity);
        assert_eq!("28P01", error_info.code);
        assert_eq!("Password authentication failed", error_info.message);
        assert!(error_info.file_name.is_none());
    }

    #[test]
    fn test_error_send_and_sync() {
        fn is_error_send_and_sync<T: Error + Send + Sync + 'static>() {}
        is_error_send_and_sync::<PgWireError>();
        #[cfg(feature = "client-api")]
        is_error_send_and_sync::<PgWireClientError>();
    }
}
