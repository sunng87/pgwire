use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};

use super::{
    ClientInfo, LoginInfo, Password, PasswordVerifier, PgWireConnectionState,
    ServerParameterProvider, StartupHandler,
};
use crate::error::{ErrorInfo, PgWireError, PgWireResult};
use crate::messages::response::ErrorResponse;
use crate::messages::startup::Authentication;
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

#[derive(new)]
pub struct CleartextPasswordAuthStartupHandler<V, P> {
    verifier: V,
    parameter_provider: P,
}

#[async_trait]
impl<V: PasswordVerifier, P: ServerParameterProvider> StartupHandler
    for CleartextPasswordAuthStartupHandler<V, P>
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
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                super::save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let login_info = LoginInfo::from_client_info(client);
                if let Ok(true) = self
                    .verifier
                    .verify_password(login_info, Password::ClearText(pwd.password()))
                    .await
                {
                    super::finish_authentication(client, &self.parameter_provider).await
                } else {
                    let error_info = ErrorInfo::new(
                        "FATAL".to_owned(),
                        "28P01".to_owned(),
                        "Password authentication failed".to_owned(),
                    );
                    let error = ErrorResponse::from(error_info);

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
}
