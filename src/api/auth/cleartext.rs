use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::Sink;

use super::{
    AuthDriver, AuthSource, ClientInfo, LoginInfo, ServerParameterProvider, StartupHandler,
    mechanism::AuthMechanism, mechanism::AuthMechanismFactory, mechanism::AuthState,
};
use crate::api::{ConnectionManager, PidSecretKeyGenerator};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::startup::Authentication;
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

/// Cleartext password authentication mechanism.
///
/// The auth source is asked for the user's password and compared literally
/// with the password message from the client.
#[derive(Debug)]
pub struct CleartextPasswordAuth<A> {
    auth_source: Arc<A>,
}

impl<A> CleartextPasswordAuth<A> {
    /// Creates a cleartext password authentication factory backed by
    /// `auth_source`.
    pub fn new(auth_source: Arc<A>) -> Self {
        Self { auth_source }
    }
}

impl<A: AuthSource + 'static> AuthMechanismFactory for CleartextPasswordAuth<A> {
    fn create(&self) -> Box<dyn AuthMechanism> {
        Box::new(CleartextPasswordMechanism {
            auth_source: self.auth_source.clone(),
        })
    }
}

/// Stateful cleartext password mechanism ([`CleartextPasswordAuth`] is its
/// factory).
pub struct CleartextPasswordMechanism<A> {
    auth_source: Arc<A>,
}

#[async_trait]
impl<A: AuthSource> AuthMechanism for CleartextPasswordMechanism<A> {
    async fn begin(&mut self, _client: &(dyn ClientInfo + Send + Sync)) -> PgWireResult<AuthState> {
        Ok(AuthState::InProgress(Authentication::CleartextPassword))
    }

    async fn next(
        &mut self,
        client: &(dyn ClientInfo + Send + Sync),
        message: crate::messages::startup::PasswordMessageFamily,
    ) -> PgWireResult<AuthState> {
        let message = message.into_password()?;
        let login_info = LoginInfo::from_client_info(client);
        let expected = self.auth_source.get_password(&login_info).await?;

        if expected.password() == message.password.as_bytes() {
            Ok(AuthState::Ok(None))
        } else {
            Ok(AuthState::Failed(PgWireError::InvalidPassword(
                login_info.user().unwrap_or_default().to_owned(),
            )))
        }
    }
}

/// Startup handler for cleartext password authentication.
///
/// Implements [`StartupHandler`] over an [`AuthDriver`] and a fixed
/// [`CleartextPasswordAuth`] factory; see the
/// [`mechanism`](super::mechanism) module for the composable building
/// blocks.
pub struct CleartextPasswordAuthStartupHandler<A, P> {
    driver: AuthDriver<P>,
    factory: CleartextPasswordAuth<A>,
}

impl<A, P> CleartextPasswordAuthStartupHandler<A, P>
where
    A: AuthSource + 'static,
    P: ServerParameterProvider,
{
    /// Creates a new cleartext password auth handler.
    pub fn new(auth_source: A, parameter_provider: P) -> Self {
        Self {
            driver: AuthDriver::new(parameter_provider),
            factory: CleartextPasswordAuth::new(Arc::new(auth_source)),
        }
    }

    /// Sets a custom PID/secret key generator.
    pub fn with_pid_secret_key_generator(
        mut self,
        generator: Arc<dyn PidSecretKeyGenerator>,
    ) -> Self {
        self.driver = self.driver.with_pid_secret_key_generator(generator);
        self
    }

    /// Sets a connection manager.
    pub fn with_connection_manager(mut self, manager: Arc<ConnectionManager>) -> Self {
        self.driver = self.driver.with_connection_manager(manager);
        self
    }
}

#[async_trait]
impl<A, P> StartupHandler for CleartextPasswordAuthStartupHandler<A, P>
where
    A: AuthSource + 'static,
    P: ServerParameterProvider,
{
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                super::protocol_negotiation(client, startup).await?;
                super::save_startup_parameters_to_metadata(client, startup);

                let mechanism = self.factory.create();
                self.driver.begin_authentication(client, mechanism).await?;
                Ok(())
            }
            PgWireFrontendMessage::PasswordMessageFamily(message) => {
                self.driver.advance_authentication(client, message).await?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::PgWireConnectionState;
    use crate::api::auth::test_util::{TestClient, startup_message};
    use crate::api::auth::{LoginInfo, Password};
    use crate::error::PgWireResult;
    use crate::messages::startup::{
        Authentication, Password as PasswordMessage, PasswordMessageFamily,
    };

    #[derive(Debug)]
    struct FixedSource {
        password: String,
    }

    #[async_trait]
    impl AuthSource for FixedSource {
        async fn get_password(&self, _login: &LoginInfo) -> PgWireResult<Password> {
            Ok(Password::new(None, self.password.as_bytes().to_vec()))
        }
    }

    #[tokio::test]
    async fn test_cleartext_handler_success() {
        let handler = CleartextPasswordAuthStartupHandler::new(
            FixedSource {
                password: "secret".to_owned(),
            },
            crate::api::auth::DefaultServerParameterProvider::default(),
        );
        let mut client = TestClient::new(Some("alice"));

        handler
            .on_startup(&mut client, startup_message("alice"))
            .await
            .unwrap();

        assert!(matches!(
            client.state(),
            PgWireConnectionState::AuthenticationInProgress
        ));
        assert!(matches!(
            client.sent.last(),
            Some(PgWireBackendMessage::Authentication(
                Authentication::CleartextPassword
            ))
        ));

        handler
            .on_startup(
                &mut client,
                PgWireFrontendMessage::PasswordMessageFamily(PasswordMessageFamily::Password(
                    PasswordMessage::new("secret".to_owned()),
                )),
            )
            .await
            .unwrap();

        assert!(matches!(
            client.state(),
            PgWireConnectionState::ReadyForQuery
        ));
        // CleartextPassword request, AuthenticationOk, parameters,
        // BackendKeyData, ReadyForQuery
        assert!(client.sent.iter().any(|msg| {
            matches!(
                msg,
                PgWireBackendMessage::Authentication(Authentication::Ok)
            )
        }));
    }

    #[tokio::test]
    async fn test_cleartext_handler_wrong_password() {
        let handler = CleartextPasswordAuthStartupHandler::new(
            FixedSource {
                password: "secret".to_owned(),
            },
            crate::api::auth::DefaultServerParameterProvider::default(),
        );
        let mut client = TestClient::new(Some("alice"));

        handler
            .on_startup(&mut client, startup_message("alice"))
            .await
            .unwrap();

        let result = handler
            .on_startup(
                &mut client,
                PgWireFrontendMessage::PasswordMessageFamily(PasswordMessageFamily::Password(
                    PasswordMessage::new("wrong".to_owned()),
                )),
            )
            .await;

        assert!(matches!(result, Err(PgWireError::InvalidPassword(_))));
    }
}
