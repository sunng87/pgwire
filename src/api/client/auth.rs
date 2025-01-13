use std::fmt::Debug;

use async_trait::async_trait;
use futures::Sink;

use crate::error::{PgWireError, PgWireResult};
use crate::messages::response::ReadyForQuery;
use crate::messages::startup::{Authentication, BackendKeyData, ParameterStatus};
use crate::messages::PgWireFrontendMessage;

use super::ClientInfo;

#[async_trait]
pub trait StartupHandler: Send + Sync {
    async fn startup<C>(&self, client: &mut C) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn on_authentication<C>(
        &self,
        client: &mut C,
        message: Authentication,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn on_parameter_status<C>(
        &self,
        client: &mut C,
        message: ParameterStatus,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn on_backend_key<C>(&self, client: &mut C, message: BackendKeyData) -> PgWireResult<()>;

    async fn on_ready_for_query<C>(
        &self,
        client: &mut C,
        message: ReadyForQuery,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireFrontendMessage>>::Error>;
}
