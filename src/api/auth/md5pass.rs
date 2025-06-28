use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use tokio::sync::Mutex;

use super::{
    protocol_negotiation, AuthSource, ClientInfo, LoginInfo, PgWireConnectionState,
    ServerParameterProvider, StartupHandler,
};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::startup::Authentication;
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

pub struct Md5PasswordAuthStartupHandler<A, P> {
    auth_source: Arc<A>,
    parameter_provider: Arc<P>,
    cached_password: Mutex<Vec<u8>>,
}

impl<A, P> Md5PasswordAuthStartupHandler<A, P> {
    pub fn new(auth_source: Arc<A>, parameter_provider: Arc<P>) -> Self {
        Md5PasswordAuthStartupHandler {
            auth_source,
            parameter_provider,
            cached_password: Mutex::new(vec![]),
        }
    }
}

#[async_trait]
impl<A: AuthSource, P: ServerParameterProvider> StartupHandler
    for Md5PasswordAuthStartupHandler<A, P>
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
                protocol_negotiation(client, startup).await?;
                super::save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);

                let login_info = LoginInfo::from_client_info(client);
                let salt_and_pass = self.auth_source.get_password(&login_info).await?;

                let salt = salt_and_pass
                    .salt
                    .as_ref()
                    .expect("Salt is required for Md5Password authentication");

                self.cached_password
                    .lock()
                    .await
                    .clone_from(&salt_and_pass.password);

                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::MD5Password(salt.clone()),
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let cached_pass = self.cached_password.lock().await;

                if pwd.password.as_bytes() == *cached_pass {
                    super::finish_authentication(client, self.parameter_provider.as_ref()).await?;
                } else {
                    let login_info = LoginInfo::from_client_info(client);
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
