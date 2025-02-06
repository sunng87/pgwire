use async_trait::async_trait;
use futures::Sink;

use crate::error::PgWireResult;
use crate::messages::response::ReadyForQuery;
use crate::messages::startup::{Authentication, BackendKeyData, ParameterStatus};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::{ClientInfo, ReadyState, ServerInformation};

#[async_trait]
pub trait StartupHandler: Send + Sync {
    async fn startup<C>(&mut self, client: &mut C) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send;

    async fn on_message<C>(
        &mut self,
        client: &mut C,
        message: PgWireBackendMessage,
    ) -> PgWireResult<ReadyState<ServerInformation>>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
    {
        match message {
            PgWireBackendMessage::Authentication(authentication) => {
                self.on_authentication(client, authentication).await?;
            }
            PgWireBackendMessage::ParameterStatus(parameter_status) => {
                self.on_parameter_status(client, parameter_status).await?;
            }
            PgWireBackendMessage::BackendKeyData(backend_key_data) => {
                self.on_backend_key(client, backend_key_data).await?;
            }
            PgWireBackendMessage::ReadyForQuery(ready) => {
                let server_information = self.on_ready_for_query(client, ready).await?;
                return Ok(ReadyState::Ready(server_information));
            }
            _ => {
                todo!("raise error on unexpected message")
            }
        }

        Ok(ReadyState::Pending)
    }

    async fn on_authentication<C>(
        &mut self,
        client: &mut C,
        message: Authentication,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send;

    async fn on_parameter_status<C>(
        &mut self,
        client: &mut C,
        message: ParameterStatus,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send;

    async fn on_backend_key<C>(&mut self, client: &C, message: BackendKeyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send;

    async fn on_ready_for_query<C>(
        &self,
        client: &C,
        message: ReadyForQuery,
    ) -> PgWireResult<ServerInformation>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send;
}
