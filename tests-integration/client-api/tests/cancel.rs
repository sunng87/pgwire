//! Query cancellation integration tests: `PgWireClient::cancel` and a raw
//! `CancelRequest` sent over a second connection, both against a real
//! `pg_sleep` query.

mod common;

use std::time::Duration;

use common::connect;
use futures::{SinkExt, StreamExt};
use pgwire::api::client::ClientInfo;
use pgwire::api::client::query::DefaultSimpleQueryHandler;
use pgwire::error::ErrorInfo;
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::PgWireFrontendMessage;
use pgwire::messages::cancel::CancelRequest;
use pgwire::messages::simplequery::Query;
use pgwire::tokio::client::PgWireClient;
use pgwire::tokio::client::PgWireMessageClientCodec;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

const QUERY_CANCELED: &str = "57014";

/// Open a fresh connection and send a raw `CancelRequest` as its first
/// message, the way the protocol requires (a cancel request never goes
/// through a started session).
async fn send_cancel_request(pid: i32, secret_key: pgwire::messages::startup::SecretKey) {
    let host = common::env_or("PGWIRE_ITEST_HOST", "127.0.0.1");
    let port: u16 = common::env_or("PGWIRE_ITEST_PORT", "54329")
        .parse()
        .expect("PGWIRE_ITEST_PORT must be a port number");
    let socket = TcpStream::connect((host.as_str(), port))
        .await
        .expect("failed to open cancel connection");
    let mut framed = Framed::new(socket, PgWireMessageClientCodec::default());
    framed
        .send(PgWireFrontendMessage::CancelRequest(CancelRequest::new(
            pid, secret_key,
        )))
        .await
        .expect("failed to send CancelRequest");
    framed
        .close()
        .await
        .expect("failed to close cancel connection");
}

/// Read messages until `ReadyForQuery`, returning the first `ErrorResponse`
/// on the way (if any).
async fn read_until_ready(client: &mut PgWireClient) -> Option<ErrorInfo> {
    let mut error = None;
    loop {
        let message = client
            .next()
            .await
            .expect("connection closed unexpectedly")
            .expect("io error while reading query response");
        match message {
            PgWireBackendMessage::ErrorResponse(err) => error = Some(ErrorInfo::from(err)),
            PgWireBackendMessage::ReadyForQuery(_) => return error,
            _ => {}
        }
    }
}

#[tokio::test]
async fn cancel_interrupts_running_simple_query() {
    let mut client = connect().await;

    // Send the long-running query directly so we keep control of the client
    // while it executes server-side.
    client
        .send(PgWireFrontendMessage::Query(Query::new(
            "SELECT pg_sleep(30)".to_owned(),
        )))
        .await
        .expect("failed to send query");

    // give the server a moment to start executing the query
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // cancel opens a second connection by itself, carrying this client's
    // pid and secret key
    client
        .cancel()
        .await
        .expect("failed to send cancel request");

    let error = read_until_ready(&mut client)
        .await
        .expect("expected an ErrorResponse after cancellation");
    assert_eq!(error.code, QUERY_CANCELED);
    assert!(
        error.message.contains("canceling statement"),
        "unexpected message: {}",
        error.message
    );

    // the connection is still usable after the cancelled query
    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1")
        .await
        .expect("connection should be usable after cancellation");
    assert_eq!(responses.len(), 1);
}

#[tokio::test]
async fn cancel_request_message_cancels_from_another_connection() {
    let mut slow = connect().await;

    // capture the backend identity of the first connection; a CancelRequest
    // only works when it carries the right pid and secret key
    let pid = slow.process_id();
    let secret_key = slow.secret_key().clone();

    let query = tokio::spawn(async move {
        slow.simple_query(DefaultSimpleQueryHandler::new(), "SELECT pg_sleep(30)")
            .await
    });

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // a fresh, un-started connection sends the raw CancelRequest message
    send_cancel_request(pid, secret_key).await;

    let result = query.await.expect("query task panicked");
    match result.err().expect("query should have been cancelled") {
        pgwire::error::PgWireClientError::RemoteError(info) => {
            assert_eq!(info.code, QUERY_CANCELED);
        }
        error => panic!("expected RemoteError, got: {error:?}"),
    }
}

#[tokio::test]
async fn cancel_request_with_wrong_secret_key_is_ignored() {
    let mut client = connect().await;

    client
        .send(PgWireFrontendMessage::Query(Query::new(
            "SELECT pg_sleep(3)".to_owned(),
        )))
        .await
        .expect("failed to send query");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // wrong secret key: the server closes the cancel connection without
    // cancelling anything (for security reasons it does not report an error)
    send_cancel_request(
        client.process_id(),
        pgwire::messages::startup::SecretKey::I32(0xdeadbeefu32 as i32),
    )
    .await;

    // the original query runs to completion: CommandComplete, no error
    let error = read_until_ready(&mut client).await;
    assert!(error.is_none(), "query should not have been cancelled");
}
