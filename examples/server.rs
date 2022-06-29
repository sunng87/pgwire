#[macro_use]
extern crate tokio;

use std::fmt::Debug;

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use futures::stream;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tokio_stream:: StreamExt;
use rand;

use pgwire::api::{ClientInfo, ClientInfoHolder};
use pgwire::messages::PgWireMessage;
use pgwire::messages::startup::{Authentication, ParameterStatus, BackendKeyData};
use pgwire::messages::response::{READY_STATUS_IDLE, ReadyForQuery};
use pgwire::tokio::PgWireMessageServerCodec;
use pgwire::api::PgWireConnectionState;
use pgwire::api::auth::Authenticator;

pub struct DummyAuthenticator;

#[async_trait]
impl Authenticator for DummyAuthenticator {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: &PgWireMessage,
    ) -> Result<(), std::io::Error> where C: ClientInfo + Sink<PgWireMessage> + Unpin + Send, C::Error: Debug {
        println!("{:?}, {:?}", client.socket_addr(), message);
        match message {
            PgWireMessage::Startup(ref startup) => {
                // TODO: update metadata with startup parameters
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client.send(PgWireMessage::Authentication(Authentication::CleartextPassword)).await.unwrap();
            },
            PgWireMessage::Password(ref password) => {
                client.set_state(PgWireConnectionState::ReadyForQuery);

                // TODO: auto generate these responses
                let messages = vec![PgWireMessage::Authentication(Authentication::Ok),
                                    PgWireMessage::ParameterStatus(ParameterStatus::new("application_name".into(), "psql".into())),
                                    PgWireMessage::ParameterStatus(ParameterStatus::new("integer_datetimes".into(), "on".into())),
                                    PgWireMessage::BackendKeyData(BackendKeyData::new(std::process::id() as i32, rand::random::<i32>())),
                                    PgWireMessage::ReadyForQuery(ReadyForQuery::new(READY_STATUS_IDLE))
                        ];
                let mut message_stream = stream::iter(messages.into_iter().map(Ok));
                client.send_all(&mut message_stream).await.unwrap();
            },
            _ => {}
        }
        Ok(())
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
            process_socket(framed_socket, authenticator).await;
        });
    }
}


async fn process_socket<A>(mut socket: Framed<TcpStream, PgWireMessageServerCodec>, authenticator: A) where A: Authenticator {
    // client ssl request, return
    loop {
        match socket.next().await {
            Some(Ok(msg)) => {
                println!("{:?}", msg);
                match socket.codec().client_info().state() {
                    PgWireConnectionState::AwaitingSslRequest => {
                        if matches!(msg, PgWireMessage::SslRequest(_)) {
                            socket.codec_mut().client_info_mut().set_state(PgWireConnectionState::AwaitingStartup);
                            socket.send(PgWireMessage::SslResponse(b'N')).await.unwrap();
                        } else {
                            // TODO: raise error here for invalid packet read
                            unreachable!()
                        }
                    },
                    PgWireConnectionState::AwaitingStartup | PgWireConnectionState::AuthenticationInProgress => {
                        authenticator.on_startup(&mut socket, &msg).await.unwrap();
                    },
                    _ => {
                        // TODO: query handler
                    }
                }
            },
            Some(Err(_)) | None => {
                break;
            },
        }
    }
}
