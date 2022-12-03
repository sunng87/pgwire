use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use rand;

use super::{
    ClientInfo, HashedPassword, LoginInfo, Password, PasswordVerifier, PgWireConnectionState,
    ServerParameterProvider, StartupHandler,
};
use crate::error::{ErrorInfo, PgWireError, PgWireResult};
use crate::messages::response::ErrorResponse;
use crate::messages::startup::Authentication;
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

#[derive(new)]
pub struct Md5PasswordAuthStartupHandler<V, P> {
    verifier: V,
    parameter_provider: P,
}

fn random_salt() -> [u8; 4] {
    let mut arr = [0u8; 4];
    for i in 0..3 {
        arr[i] = rand::random::<u8>();
    }

    arr
}

const PGWIRE_AUTH_SALT: &str = "pgwire_auth_salt";

#[async_trait]
impl<V: PasswordVerifier, P: ServerParameterProvider> StartupHandler
    for Md5PasswordAuthStartupHandler<V, P>
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
                super::save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                let salt = random_salt();
                client
                    .metadata_mut()
                    .insert(PGWIRE_AUTH_SALT.to_owned(), hex::encode(salt));
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::MD5Password(salt),
                    ))
                    .await?;
            }
            PgWireFrontendMessage::Password(ref pwd) => {
                let login_info = LoginInfo::from_client_info(client);
                // extract salt from client context
                let salt = client.metadata().get(PGWIRE_AUTH_SALT).unwrap();
                let salt_array = hex::decode(salt).unwrap();

                let passwd = Password::Hashed(HashedPassword::new(&salt_array, pwd.password()));
                if let Ok(true) = self.verifier.verify_password(login_info, passwd).await {
                    client.metadata_mut().remove(PGWIRE_AUTH_SALT);
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
