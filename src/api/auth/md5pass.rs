use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::Sink;

use super::driver::AuthOutcome;
use super::{
    AuthDriver, AuthSource, ClientInfo, LoginInfo, ServerParameterProvider, StartupHandler,
    mechanism::AuthMechanism, mechanism::AuthMechanismFactory, mechanism::AuthState,
};
use crate::api::{ConnectionManager, PidSecretKeyGenerator};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::startup::Authentication;
use crate::messages::startup::PasswordMessageFamily;
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

/// MD5 password authentication mechanism.
///
/// The auth source must return a password already hashed in postgres format
/// (`md5` + md5 of (md5(password + username) + salt)); use
/// [`hash_md5_password`] to compute it from the per-connection salt.
#[derive(Debug)]
pub struct Md5PasswordAuth<A> {
    auth_source: Arc<A>,
}

impl<A> Md5PasswordAuth<A> {
    /// Creates an MD5 password authentication factory backed by
    /// `auth_source`.
    pub fn new(auth_source: Arc<A>) -> Self {
        Self { auth_source }
    }
}

impl<A: AuthSource + 'static> AuthMechanismFactory for Md5PasswordAuth<A> {
    fn create(&self) -> Box<dyn AuthMechanism> {
        Box::new(Md5PasswordMechanism {
            auth_source: self.auth_source.clone(),
            cached_password: None,
        })
    }
}

/// Stateful MD5 password mechanism ([`Md5PasswordAuth`] is its factory).
///
/// The salt (and expected hash) are fetched from the auth source in
/// [`AuthMechanism::begin`], since the per-connection salt must be sent to
/// the client in the initial request.
pub struct Md5PasswordMechanism<A> {
    auth_source: Arc<A>,
    /// password (md5-hashed) fetched from the auth source during `begin`
    cached_password: Option<Vec<u8>>,
}

#[async_trait]
impl<A: AuthSource> AuthMechanism for Md5PasswordMechanism<A> {
    async fn begin(&mut self, client: &(dyn ClientInfo + Send + Sync)) -> PgWireResult<AuthState> {
        let login_info = LoginInfo::from_client_info(client);
        let salt_and_pass = self.auth_source.get_password(&login_info).await?;

        let salt = salt_and_pass
            .salt()
            .ok_or_else(|| {
                PgWireError::InvalidOptionValue(
                    "Salt is required for Md5Password authentication".to_owned(),
                )
            })?
            .to_vec();
        self.cached_password = Some(salt_and_pass.password().to_vec());

        Ok(AuthState::InProgress(Authentication::MD5Password(salt)))
    }

    async fn next(
        &mut self,
        client: &(dyn ClientInfo + Send + Sync),
        message: PasswordMessageFamily,
    ) -> PgWireResult<AuthState> {
        let message = message.into_password()?;
        let expected = self.cached_password.as_deref().unwrap_or_default();

        if message.password.as_bytes() == expected {
            Ok(AuthState::Ok {
                final_message: None,
                identity: None,
            })
        } else {
            let login_info = LoginInfo::from_client_info(client);
            Ok(AuthState::Failed(PgWireError::InvalidPassword(
                login_info.user().unwrap_or_default().to_owned(),
            )))
        }
    }
}

/// Startup handler for MD5 password authentication.
///
/// Implements [`StartupHandler`] over an [`AuthDriver`] and a fixed
/// [`Md5PasswordAuth`] factory; see the
/// [`mechanism`](super::mechanism) module for the composable building
/// blocks.
pub struct Md5PasswordAuthStartupHandler<A, P> {
    driver: AuthDriver<Arc<P>>,
    factory: Md5PasswordAuth<A>,
}

impl<A, P> Md5PasswordAuthStartupHandler<A, P>
where
    A: AuthSource + 'static,
    P: ServerParameterProvider,
{
    /// Creates a new MD5 password auth handler.
    pub fn new(auth_source: Arc<A>, parameter_provider: Arc<P>) -> Self {
        Self {
            driver: AuthDriver::new(parameter_provider),
            factory: Md5PasswordAuth::new(auth_source),
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
impl<A, P> StartupHandler for Md5PasswordAuthStartupHandler<A, P>
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
                match self.driver.begin_authentication(client, mechanism).await? {
                    AuthOutcome::InProgress => Ok(()),
                    AuthOutcome::Authenticated { final_message, .. } => {
                        self.driver
                            .complete_authentication(client, final_message)
                            .await
                    }
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(message) => {
                match self.driver.advance_authentication(client, message).await? {
                    AuthOutcome::InProgress => Ok(()),
                    AuthOutcome::Authenticated { final_message, .. } => {
                        self.driver
                            .complete_authentication(client, final_message)
                            .await
                    }
                }
            }
            _ => Ok(()),
        }
    }
}

/// This function is to compute postgres standard md5 hashed password
///
/// concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
///
/// the input parameter `md5hashed_username_password` represents
/// `md5(concat(password, username))` so that your can store hashed password in
/// storage.
pub fn hash_md5_password(username: &str, password: &str, salt: &[u8]) -> String {
    let hashed_bytes = format!("{:x}", md5::compute(format!("{password}{username}")));
    let mut bytes = Vec::with_capacity(hashed_bytes.len() + 4);
    bytes.extend_from_slice(hashed_bytes.as_ref());
    bytes.extend_from_slice(salt);

    format!("md5{:x}", md5::compute(bytes))
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_hash_md5_passwd() {
        let salt = vec![20, 247, 107, 249];
        let username = "zmjiang";
        let password = "themanwhochangedchina";

        let result = "md521fe459d77d3e3ea9c9fcd5c11030d30";

        assert_eq!(result, super::hash_md5_password(username, password, &salt));
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;
    use crate::api::PgWireConnectionState;
    use crate::api::auth::DefaultServerParameterProvider;
    use crate::api::auth::test_util::{TestClient, startup_message};
    use crate::api::auth::{LoginInfo, Password};
    use crate::error::PgWireResult;
    use crate::messages::startup::{
        Authentication, Password as PasswordMessage, PasswordMessageFamily,
    };

    const SALT: [u8; 4] = [20, 247, 107, 249];

    #[derive(Debug)]
    struct Md5Source;

    #[async_trait]
    impl AuthSource for Md5Source {
        async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
            let user = login.user().unwrap_or_default();
            // stored password: md5(md5(password + user) + salt)
            Ok(Password::new(
                Some(SALT.to_vec()),
                hash_md5_password(user, "password", &SALT).into_bytes(),
            ))
        }
    }

    #[tokio::test]
    async fn test_md5_handler_success() {
        let handler = Md5PasswordAuthStartupHandler::new(
            Arc::new(Md5Source),
            Arc::new(DefaultServerParameterProvider::default()),
        );
        let mut client = TestClient::new(Some("zmjiang"));

        handler
            .on_startup(&mut client, startup_message("zmjiang"))
            .await
            .unwrap();

        assert!(matches!(
            client.sent.last(),
            Some(PgWireBackendMessage::Authentication(Authentication::MD5Password(salt)))
                if salt == &SALT
        ));

        // client responds with the md5 hash of (password, user, salt)
        let hashed = hash_md5_password("zmjiang", "password", &SALT);
        handler
            .on_startup(
                &mut client,
                PgWireFrontendMessage::PasswordMessageFamily(PasswordMessageFamily::Password(
                    PasswordMessage::new(hashed),
                )),
            )
            .await
            .unwrap();

        assert!(matches!(
            client.state(),
            PgWireConnectionState::ReadyForQuery
        ));
    }

    #[tokio::test]
    async fn test_md5_handler_wrong_password() {
        let handler = Md5PasswordAuthStartupHandler::new(
            Arc::new(Md5Source),
            Arc::new(DefaultServerParameterProvider::default()),
        );
        let mut client = TestClient::new(Some("zmjiang"));

        handler
            .on_startup(&mut client, startup_message("zmjiang"))
            .await
            .unwrap();

        let result = handler
            .on_startup(
                &mut client,
                PgWireFrontendMessage::PasswordMessageFamily(PasswordMessageFamily::Password(
                    PasswordMessage::new("md5deadbeef".to_owned()),
                )),
            )
            .await;

        assert!(matches!(result, Err(PgWireError::InvalidPassword(_))));
    }
}
