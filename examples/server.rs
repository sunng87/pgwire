use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use futures::StreamExt;
use futures::{Sink, SinkExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use pgwire::api::auth::StartupHandler;
use pgwire::api::query::{QueryResponse, SimpleQueryHandler};
use pgwire::api::PgWireConnectionState;
use pgwire::api::{ClientInfo, ClientInfoHolder};
use pgwire::messages::data::{DataRow, FieldDescription, RowDescription, FORMAT_CODE_TEXT};
use pgwire::messages::response::CommandComplete;
use pgwire::messages::startup::Authentication;
use pgwire::messages::PgWireMessage;
use pgwire::tokio::PgWireMessageServerCodec;

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
    let server_addr = "127.0.0.1:5433";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let client_info = ClientInfoHolder::new(addr);
            let framed_socket = Framed::new(socket, PgWireMessageServerCodec::new(client_info));
            let authenticator = DummyAuthenticator;
            let querier = DummyQueryHandler;
            process_socket(framed_socket, authenticator, querier).await;
        });
    }
}

async fn process_socket<A, Q>(
    mut socket: Framed<TcpStream, PgWireMessageServerCodec>,
    authenticator: A,
    query_handler: Q,
) where
    A: StartupHandler,
    Q: SimpleQueryHandler,
{
    // client ssl request, return
    loop {
        match socket.next().await {
            Some(Ok(msg)) => {
                println!("{:?}", msg);
                match socket.codec().client_info().state() {
                    PgWireConnectionState::AwaitingSslRequest => {
                        if matches!(msg, PgWireMessage::SslRequest(_)) {
                            socket
                                .codec_mut()
                                .client_info_mut()
                                .set_state(PgWireConnectionState::AwaitingStartup);
                            socket.send(PgWireMessage::SslResponse(b'N')).await.unwrap();
                        } else {
                            // TODO: raise error here for invalid packet read
                            unreachable!()
                        }
                    }
                    PgWireConnectionState::AwaitingStartup
                    | PgWireConnectionState::AuthenticationInProgress => {
                        authenticator.on_startup(&mut socket, &msg).await.unwrap();
                    }
                    _ => {
                        if matches!(&msg, PgWireMessage::Query(_)) {
                            query_handler.on_query(&mut socket, &msg).await.unwrap();
                        } else {
                            //todo:
                        }
                    }
                }
            }
            Some(Err(_)) | None => {
                break;
            }
        }
    }
}
