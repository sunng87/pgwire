use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use rand;

use super::{
    ClientInfo, HashedPassword, LoginInfo, Password, PasswordVerifier, PgWireConnectionState,
    ServerParameterProvider, StartupHandler,
};
use crate::api::MakeHandler;
use crate::error::{ErrorInfo, PgWireError, PgWireResult};
use crate::messages::response::ErrorResponse;
use crate::messages::startup::Authentication;
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

pub struct Md5PasswordAuthStartupHandler<V, P> {
    verifier: Arc<V>,
    parameter_provider: Arc<P>,
    salt: [u8; 4],
}

fn random_salt() -> [u8; 4] {
    let mut arr = [0u8; 4];
    for v in arr.iter_mut() {
        *v = rand::random::<u8>();
    }

    arr
}

#[async_trait]
impl<V: PasswordVerifier, P: ServerParameterProvider> StartupHandler
    for Md5PasswordAuthStartupHandler<V, P>
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
                        Authentication::MD5Password(self.salt),
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let login_info = LoginInfo::from_client_info(client);

                let passwd = Password::Hashed(HashedPassword::new(&self.salt, pwd.password()));
                if let Ok(true) = self.verifier.verify_password(login_info, passwd).await {
                    super::finish_authentication(client, self.parameter_provider.as_ref()).await
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

/// this function is to compute postgres standard md5 hashed password
///
/// concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
///
/// the input parameter `md5hashed_username_password` represents
/// `md5(concat(password, username))` so that your can store hashed password in
/// storage.
pub fn hash_md5_password(md5hashed_username_password: &String, salt: &[u8]) -> String {
    let hashed_bytes = md5hashed_username_password.as_bytes();
    let mut bytes = Vec::with_capacity(hashed_bytes.len() + 4);
    bytes.extend_from_slice(hashed_bytes);
    bytes.extend_from_slice(salt);

    format!("md5{:x}", md5::compute(bytes))
}

#[derive(Debug, new)]
pub struct MakeMd5PasswordAuthStartupHandler<V, P> {
    verifier: Arc<V>,
    parameter_provider: Arc<P>,
}

impl<V, P> MakeHandler for MakeMd5PasswordAuthStartupHandler<V, P> {
    type Handler = Arc<Md5PasswordAuthStartupHandler<V, P>>;

    fn make(&self) -> Self::Handler {
        Arc::new(Md5PasswordAuthStartupHandler {
            verifier: self.verifier.clone(),
            parameter_provider: self.parameter_provider.clone(),
            salt: random_salt(),
        })
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_hash_md5_passwd() {
        let salt = vec![20, 247, 107, 249];
        let username = "zmjiang";
        let password = "themanwhochangedchina";

        let passwdhash = format!("{:x}", md5::compute(format!("{}{}", password, username)));

        let result = "md521fe459d77d3e3ea9c9fcd5c11030d30";

        assert_eq!(result, super::hash_md5_password(&passwdhash, &salt));
    }
}
