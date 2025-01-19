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
    async fn startup<H>(&self, client: &PgWireClient<H>) -> PgWireResult<()>
    where
        H: PgWireClientHandlers + Send + Sync + 'static;

    async fn on_authentication<H>(
        &self,
        client: &PgWireClient<H>,
        message: Authentication,
    ) -> PgWireResult<()>
    where
        H: PgWireClientHandlers + Send + Sync + 'static;

    async fn on_parameter_status<H>(
        &self,
        client: &PgWireClient<H>,
        message: ParameterStatus,
    ) -> PgWireResult<()>
    where
        H: PgWireClientHandlers + Send + Sync + 'static;

    async fn on_backend_key<H>(
        &self,
        client: &PgWireClient<H>,
        message: BackendKeyData,
    ) -> PgWireResult<()>
    where
        H: PgWireClientHandlers + Send + Sync + 'static;

    async fn on_ready_for_query<H>(
        &self,
        client: &PgWireClient<H>,
        message: ReadyForQuery,
    ) -> PgWireResult<()>
    where
        H: PgWireClientHandlers + Send + Sync + 'static;
}
