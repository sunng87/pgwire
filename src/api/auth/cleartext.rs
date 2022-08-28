use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};

use super::{ClientInfo, PgWireConnectionState, StartupHandler};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::response::ErrorResponse;
use crate::messages::startup::Authentication;
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

#[async_trait]
pub trait CleartextPasswordAuthStartupHandler: StartupHandler {
    async fn verify_password(&self, password: &str) -> PgWireResult<bool>;

    fn server_parameters<C>(&self, _client: &C) -> HashMap<String, String>
    where
        C: ClientInfo;
}

#[async_trait]
impl<T> StartupHandler for T
where
    T: CleartextPasswordAuthStartupHandler,
{
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
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                self.handle_startup_parameters(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::Password(ref pwd) => {
                if let Ok(true) = self.verify_password(pwd.password()).await {
                    self.finish_authentication(client).await
                } else {
                    // TODO: error api
                    let info = vec![
                        (b'L', "FATAL".to_owned()),
                        (b'T', "FATAL".to_owned()),
                        (b'C', "28P01".to_owned()),
                        (b'M', "Password authentication failed".to_owned()),
                        (b'R', "auth_failed".to_owned()),
                    ];
                    let error = ErrorResponse::new(info);

                    client
                        .feed(PgWireBackendMessage::ErrorResponse(error))
                        .await?;
                    client.close().await?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn server_parameters<C>(&self, _client: &C) -> HashMap<String, String>
    where
        C: ClientInfo,
    {
        CleartextPasswordAuthStartupHandler::server_parameters(self, _client)
    }
}
