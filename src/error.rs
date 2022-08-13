use postgres_types::Oid;
use thiserror::Error;

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
}

pub type PgWireResult<T> = Result<T, PgWireError>;

// TODO: distinguish fatal error and recoverable error
