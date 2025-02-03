use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;

use crate::error::{PgWireError, PgWireResult};
use crate::messages::response::ReadyForQuery;
use crate::messages::startup::{Authentication, BackendKeyData, ParameterStatus};
use crate::messages::PgWireFrontendMessage;
use crate::tokio::client::PgWireClient;

use super::{ClientInfo, PgWireClientHandlers};

#[async_trait]
pub trait StartupHandler: Send + Sync {
    async fn startup(&self, client: &PgWireClient) -> PgWireResult<()>;

    async fn on_authentication(
        &self,
        client: &PgWireClient,
        message: Authentication,
    ) -> PgWireResult<()>;

    async fn on_parameter_status(
        &self,
        client: &PgWireClient,
        message: ParameterStatus,
    ) -> PgWireResult<()>;

    async fn on_backend_key(
        &self,
        client: &PgWireClient,
        message: BackendKeyData,
    ) -> PgWireResult<()>;

    async fn on_ready_for_query(
        &self,
        client: &PgWireClient,
        message: ReadyForQuery,
    ) -> PgWireResult<()>;
}
q
