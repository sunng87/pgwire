use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};

use super::{
    AuthSource, ClientInfo, LoginInfo, PgWireConnectionState, ServerParameterProvider,
    StartupHandler,
};
use crate::api::{ConnectionManager, PidSecretKeyGenerator, RandomPidSecretKeyGenerator};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::startup::Authentication;
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

/// Startup handler for cleartext password authentication.
pub struct CleartextPasswordAuthStartupHandler<A, P> {
    auth_source: A,
    parameter_provider: P,
    pid_secret_key_generator: Arc<dyn PidSecretKeyGenerator>,
    connection_manager: Option<Arc<ConnectionManager>>,
}

impl<A, P> CleartextPasswordAuthStartupHandler<A, P> {
    /// Creates a new cleartext password auth handler.
    pub fn new(auth_source: A, parameter_provider: P) -> Self {
        Self {
            auth_source,
            parameter_provider,
            pid_secret_key_generator: Arc::new(RandomPidSecretKeyGenerator::default()),
            connection_manager: None,
        }
    }

    /// Sets a custom PID/secret key generator.
    pub fn with_pid_secret_key_generator(
        mut self,
        generator: Arc<dyn PidSecretKeyGenerator>,
    ) -> Self {
        self.pid_secret_key_generator = generator;
        self
    }

    /// Sets a connection manager.
    pub fn with_connection_manager(mut self, manager: Arc<ConnectionManager>) -> Self {
        self.connection_manager = Some(manager);
        self
    }
}

#[async_trait]
impl<V: AuthSource, P: ServerParameterProvider> StartupHandler
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
                super::protocol_negotiation(client, startup).await?;
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
                let pass = self.auth_source.get_password(&login_info).await?;
                if pass.password == pwd.password.as_bytes() {
                    let (pid, secret_key) = self.pid_secret_key_generator.generate(client);
                    client.set_pid_and_secret_key(pid, secret_key);
                    if let Some(manager) = &self.connection_manager {
                        super::register_connection(client, manager);
                    }
                    super::finish_authentication(client, &self.parameter_provider).await?;
                } else {
                    return Err(PgWireError::InvalidPassword(
                        login_info.user().map(|x| x.to_owned()).unwrap_or_default(),
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }
}
