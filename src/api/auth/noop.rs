use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::Sink;

use super::{ClientInfo, StartupHandler};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

pub struct NoopStartupHandler;

#[async_trait]
impl StartupHandler for NoopStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: &PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(ref startup) = message {
            self.handle_startup_parameters(client, startup);
            self.finish_authentication(client).await;
        }
        Ok(())
    }
}
