use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::{Sink, SinkExt};

use crate::api::auth::md5pass::hash_md5_password;
use crate::error::{ErrorInfo, PgWireClientError, PgWireClientResult};
use crate::messages::response::ReadyForQuery;
use crate::messages::startup::{
    Authentication, BackendKeyData, ParameterStatus, Password, PasswordMessageFamily, Startup,
};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::{ClientInfo, ReadyState, ServerInformation};

#[async_trait]
pub trait StartupHandler: Send + Sync {
    async fn startup<C>(&mut self, client: &mut C) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn on_message<C>(
        &mut self,
        client: &mut C,
        message: PgWireBackendMessage,
    ) -> PgWireClientResult<ReadyState<ServerInformation>>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        match message {
            PgWireBackendMessage::Authentication(authentication) => {
                self.on_authentication(client, authentication).await?;
            }
            PgWireBackendMessage::ParameterStatus(parameter_status) => {
                self.on_parameter_status(client, parameter_status).await?;
            }
            PgWireBackendMessage::BackendKeyData(backend_key_data) => {
                self.on_backend_key(client, backend_key_data).await?;
            }
            PgWireBackendMessage::ReadyForQuery(ready) => {
                let server_information = self.on_ready_for_query(client, ready).await?;
                return Ok(ReadyState::Ready(server_information));
            }
            PgWireBackendMessage::ErrorResponse(error) => {
                let error_info = ErrorInfo::from(error);
                return Err(error_info.into());
            }
            _ => return Err(PgWireClientError::UnexpectedMessage(Box::new(message))),
        }

        Ok(ReadyState::Pending)
    }

    async fn on_authentication<C>(
        &mut self,
        client: &mut C,
        message: Authentication,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn on_parameter_status<C>(
        &mut self,
        client: &mut C,
        message: ParameterStatus,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn on_backend_key<C>(
        &mut self,
        client: &mut C,
        message: BackendKeyData,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn on_ready_for_query<C>(
        &mut self,
        client: &mut C,
        message: ReadyForQuery,
    ) -> PgWireClientResult<ServerInformation>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;
}

#[derive(new, Debug)]
pub struct DefaultStartupHandler {
    #[new(default)]
    server_parameters: BTreeMap<String, String>,
    #[new(default)]
    process_id: Option<i32>,
}

#[async_trait]
impl StartupHandler for DefaultStartupHandler {
    async fn startup<C>(&mut self, client: &mut C) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        let mut startup = Startup::new();

        let config = client.config();

        if let Some(application_name) = &config.application_name {
            startup
                .parameters
                .insert("application_name".to_string(), application_name.clone());
        }
        if let Some(user) = &config.user {
            startup.parameters.insert("user".to_string(), user.clone());
        }
        if let Some(dbname) = &config.dbname {
            startup
                .parameters
                .insert("database".to_string(), dbname.clone());
        }

        client.send(PgWireFrontendMessage::Startup(startup)).await?;
        Ok(())
    }

    async fn on_authentication<C>(
        &mut self,
        client: &mut C,
        message: Authentication,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        match message {
            Authentication::Ok => {}
            Authentication::CleartextPassword => {
                let pass = client
                    .config()
                    .password
                    .as_ref()
                    .map(|bs| String::from_utf8_lossy(bs).into_owned())
                    .unwrap_or_default();

                client
                    .send(PgWireFrontendMessage::PasswordMessageFamily(
                        PasswordMessageFamily::Password(Password::new(pass)),
                    ))
                    .await?;
            }
            Authentication::MD5Password(salt) => {
                let username = client.config().user.as_ref().map_or("", |s| s.as_str());

                let password = client
                    .config()
                    .password
                    .as_ref()
                    .map(|bs| String::from_utf8_lossy(bs).into_owned())
                    .unwrap_or_default();

                let hashed_password = hash_md5_password(username, &password, &salt);
                client
                    .send(PgWireFrontendMessage::PasswordMessageFamily(
                        PasswordMessageFamily::Password(Password::new(hashed_password)),
                    ))
                    .await?;
            }
            // TODO: scram
            _ => {}
        }

        Ok(())
    }

    async fn on_parameter_status<C>(
        &mut self,
        _client: &mut C,
        message: ParameterStatus,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
    {
        self.server_parameters.insert(message.name, message.value);
        Ok(())
    }

    async fn on_backend_key<C>(
        &mut self,
        _client: &mut C,
        message: BackendKeyData,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
    {
        self.process_id = Some(message.pid);
        Ok(())
    }

    async fn on_ready_for_query<C>(
        &mut self,
        _client: &mut C,
        _message: ReadyForQuery,
    ) -> PgWireClientResult<ServerInformation>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
    {
        Ok(ServerInformation {
            parameters: self.server_parameters.clone(),
            process_id: self.process_id.unwrap_or(-1),
        })
    }
}
