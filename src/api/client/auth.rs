use async_trait::async_trait;

use crate::error::PgWireResult;
use crate::messages::response::ReadyForQuery;
use crate::messages::startup::{Authentication, BackendKeyData, ParameterStatus};

#[async_trait]
pub trait StartupHandler: Send + Sync {
    // TODO: client info
    async fn on_authentication(&self, message: Authentication) -> PgWireResult<()>;

    async fn on_parameter_status(&self, message: ParameterStatus) -> PgWireResult<()>;

    async fn on_backend_key(&self, message: BackendKeyData) -> PgWireResult<()>;

    async fn on_ready_for_query(&self, message: ReadyForQuery) -> PgWireResult<()>;
}
