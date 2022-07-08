use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgWireError {
    #[error("Invalid message recevied, expect {0:?}, received {1:?}")]
    InvalidMessageType(u8, u8),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Api Error")]
    ApiError(Box<dyn std::error::Error + 'static + Send>),
}

pub type PgWireResult<T> = Result<T, PgWireError>;
