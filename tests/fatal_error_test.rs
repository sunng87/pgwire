//! A FATAL error, like a CancelRequest, ends the connection on the server
//! side, as it does in PostgreSQL, even while the client keeps its socket open.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::Sink;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use pgwire::api::auth::StartupHandler;
use pgwire::api::{ClientInfo, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::cancel::CancelRequest;
use pgwire::messages::simplequery::Query;
use pgwire::messages::startup::{SecretKey, Startup};
use pgwire::messages::{Message, PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

/// Rejects every connection at startup with the given severity. Queries go to
/// the default handler, which answers with a FATAL error.
#[derive(Clone, Copy)]
struct RejectingProcessor {
    severity: &'static str,
}

#[async_trait]
impl StartupHandler for RejectingProcessor {
    async fn on_startup<C>(
        &self,
        _client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            self.severity.to_owned(),
            "28000".to_owned(),
            "rejected".to_owned(),
        ))))
    }
}

impl PgWireServerHandlers for RejectingProcessor {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(*self)
    }
}

/// Serves one connection and sends it a StartupMessage. Returns the client
/// socket, still open, and the server task.
async fn connect(severity: &'static str) -> (TcpStream, JoinHandle<std::io::Result<()>>) {
    let (mut client, server) = serve(severity).await;
    let mut startup = Startup::new();
    startup
        .parameters
        .insert("user".to_owned(), "postgres".to_owned());
    send(&mut client, &startup).await;
    (client, server)
}

/// Serves one connection. Returns the client socket and the server task.
async fn serve(severity: &'static str) -> (TcpStream, JoinHandle<std::io::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        process_socket(stream, None, RejectingProcessor { severity }).await
    });

    let client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    (client, server)
}

async fn send(client: &mut TcpStream, message: &impl Message) {
    let mut buf = BytesMut::new();
    message.encode(&mut buf).unwrap();
    client.write_all(&buf).await.unwrap();
}

/// Reads one backend message and returns its type byte.
async fn read_message(client: &mut TcpStream) -> u8 {
    let tag = client.read_u8().await.unwrap();
    let len = client.read_i32().await.unwrap() as usize;
    let mut body = vec![0; len - 4];
    client.read_exact(&mut body).await.unwrap();
    tag
}

async fn assert_server_ends(server: JoinHandle<std::io::Result<()>>) {
    timeout(Duration::from_secs(5), server)
        .await
        .expect("the server kept the connection")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn fatal_error_ends_connection_while_client_holds_socket() {
    let (mut client, server) = connect("FATAL").await;
    assert_eq!(read_message(&mut client).await, b'E');

    // The client neither hangs up nor sends anything else.
    assert_server_ends(server).await;
}

#[tokio::test]
async fn non_fatal_error_keeps_connection() {
    let (mut client, server) = connect("ERROR").await;
    assert_eq!(read_message(&mut client).await, b'E');
    assert_eq!(read_message(&mut client).await, b'Z');

    // The server still answers, and the FATAL answer then ends it.
    send(&mut client, &Query::new("SELECT 1".to_owned())).await;
    assert_eq!(read_message(&mut client).await, b'E');
    assert_server_ends(server).await;
}

#[tokio::test]
async fn cancel_request_ends_connection_while_client_holds_socket() {
    // A CancelRequest never reaches the startup handler, so its severity is unused.
    let (mut client, server) = serve("FATAL").await;
    send(&mut client, &CancelRequest::new(1, SecretKey::I32(1))).await;

    // The client neither hangs up nor sends anything else.
    assert_server_ends(server).await;
}
