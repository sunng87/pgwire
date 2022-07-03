use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use tokio::net::TcpListener;

use pgwire::api::auth::StartupHandler;
use pgwire::api::query::{QueryResponse, SimpleQueryHandler};
use pgwire::api::ClientInfo;
use pgwire::api::PgWireConnectionState;
use pgwire::messages::data::{DataRow, FieldDescription, RowDescription, FORMAT_CODE_TEXT};
use pgwire::messages::response::CommandComplete;
use pgwire::messages::startup::Authentication;
use pgwire::messages::PgWireMessage;
use pgwire::tokio::process_socket;

pub struct DummyAuthenticator;

#[async_trait]
impl StartupHandler for DummyAuthenticator {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: &PgWireMessage,
    ) -> Result<(), std::io::Error>
    where
        C: ClientInfo + Sink<PgWireMessage> + Unpin + Send,
        C::Error: Debug,
    {
        println!("{:?}, {:?}", client.socket_addr(), message);
        match message {
            PgWireMessage::Startup(ref startup) => {
                self.handle_startup_parameters(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await
                    .unwrap();
            }
            PgWireMessage::Password(ref _password) => self.finish_authentication(client).await,
            _ => {}
        }
        Ok(())
    }

    fn server_parameters<C>(&self, _client: &C) -> std::collections::HashMap<String, String>
    where
        C: ClientInfo + Sink<PgWireMessage> + Unpin + Send,
        C::Error: Debug,
    {
        let mut data = HashMap::new();
        data.insert("application_name".into(), "psql".into());
        data.insert("integer_datetimes".into(), "on".into());
        data.insert("server_version".into(), "0.0.1".into());

        data
    }
}

pub struct DummyQueryHandler;

#[async_trait]
impl SimpleQueryHandler for DummyQueryHandler {
    async fn do_query<C>(&self, _client: &C, query: &str) -> Result<QueryResponse, std::io::Error>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("{:?}", query);
        if query.starts_with("SELECT") {
            let mut rd = RowDescription::new();
            // column 0
            rd.fields_mut().push(FieldDescription::new(
                "id".into(),
                123,
                123,
                123,
                132,
                -1,
                FORMAT_CODE_TEXT,
            ));
            // column 1
            rd.fields_mut().push(FieldDescription::new(
                "name".into(),
                123,
                123,
                123,
                132,
                -1,
                FORMAT_CODE_TEXT,
            ));

            let mut data_row = DataRow::new();
            *data_row.fields_mut() = vec![Some("0".as_bytes().to_vec()), None];

            let rows = vec![data_row.clone(), data_row.clone(), data_row.clone()];

            let status = CommandComplete::new("SELECT 3".to_owned());
            Ok(QueryResponse::Data(rd, rows, status))
        } else {
            Ok(QueryResponse::Empty(CommandComplete::new(
                "OK 1".to_owned(),
            )))
        }
    }
}

#[tokio::main]
pub async fn main() {
    let authenticator = Arc::new(DummyAuthenticator);
    let querier = Arc::new(DummyQueryHandler);

    let server_addr = "127.0.0.1:5433";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        process_socket(incoming_socket, authenticator.clone(), querier.clone());
    }
}
