use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};

use super::{ClientInfo, DefaultServerParameterProvider, StartupHandler};
use crate::api::PgWireConnectionState;
use crate::error::{PgWireError, PgWireResult};
use crate::messages::response::{ReadyForQuery, TransactionStatus};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

#[async_trait]
pub trait NoopStartupHandler: StartupHandler {
    async fn post_startup<C>(
        &self,
        _client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }
}

#[async_trait]
impl<H> StartupHandler for H
where
    H: NoopStartupHandler,
{
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(ref startup) = message {
            super::save_startup_parameters_to_metadata(client, startup);
            super::finish_authentication0(client, &DefaultServerParameterProvider::default())
                .await?;

            self.post_startup(client, message).await?;

            client
                .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                    TransactionStatus::Idle,
                )))
                .await?;
            client.set_state(PgWireConnectionState::ReadyForQuery);
        }

        Ok(())
    }
}

impl NoopStartupHandler for crate::api::NoopHandler {}
