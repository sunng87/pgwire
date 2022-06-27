#[macro_use]
extern crate tokio;

use futures::SinkExt;
use futures::stream;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tokio_stream:: StreamExt;
use rand;

use pgwire::api::ClientInfo;
use pgwire::messages::PgWireMessage;
use pgwire::messages::startup::{Authentication, ParameterStatus, BackendKeyData};
use pgwire::messages::response::{READY_STATUS_IDLE, ReadyForQuery};
use pgwire::tokio::PgWireMessageServerCodec;
use pgwire::api::PgWireConnectionState;
use pgwire::api::auth::{Authenticator, DummyAuthenticator};

#[tokio::main]
pub async fn main() {
    let listener = TcpListener::bind("127.0.0.1:5433").await.unwrap();
    println!("Listening to 127.0.0.1:5433");
    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let client_info = ClientInfo::new(addr);
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
                            //TODO: error
                        }
                    },
                    PgWireConnectionState::AwaitingStartup => {
                        // todo: design api for sending message back
                        authenticator.on_startup(socket.codec_mut().client_info_mut(), &msg).unwrap();
                    },
                    _ => {}
                }



                match msg {
                    PgWireMessage::SslRequest(_) => {
                        // todo: give response according to server config
                    },
                    PgWireMessage::Startup(ref _startup) => {
                        // TODO:


                        // todo: make this an authentication handler
                        socket.codec_mut().client_info_mut().set_state(PgWireConnectionState::AuthenticationInProgress);
                        socket.send(PgWireMessage::Authentication(Authentication::CleartextPassword)).await.unwrap();
                    },
                    PgWireMessage::Password(_pwd) => {
                        // todo: make this part of an authentication handler
                        socket.codec_mut().client_info_mut().set_state(PgWireConnectionState::ReadyForQuery);

                        let messages = vec![PgWireMessage::Authentication(Authentication::Ok),
                                            PgWireMessage::ParameterStatus(ParameterStatus::new("application_name".into(), "psql".into())),
                                            PgWireMessage::ParameterStatus(ParameterStatus::new("integer_datetimes".into(), "on".into())),
                                            PgWireMessage::BackendKeyData(BackendKeyData::new(std::process::id() as i32, rand::random::<i32>())),
                                            PgWireMessage::ReadyForQuery(ReadyForQuery::new(READY_STATUS_IDLE))
                        ];
                        let mut message_stream = stream::iter(messages.into_iter().map(Ok));
                        socket.send_all(&mut message_stream).await.unwrap();
                    }
                    _ => {}
                }
            },
            Some(Err(_)) | None => {
                break;
            },
        }
    }
}
