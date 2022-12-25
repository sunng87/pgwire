use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::Sink;

use super::{ClientInfo, NoopServerParameterProvider, StartupHandler};
use crate::api::StatelessMakeHandler;
use crate::error::{PgWireError, PgWireResult};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

pub struct NoopStartupHandler;

#[async_trait]
impl StartupHandler for NoopStartupHandler {
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
            super::finish_authentication(client, &NoopServerParameterProvider).await;
        }
        Ok(())
    }
}

impl Into<StatelessMakeHandler<NoopStartupHandler>> for NoopStartupHandler {
    fn into(self) -> StatelessMakeHandler<NoopStartupHandler> {
        StatelessMakeHandler(Arc::new(self))
    }
}
