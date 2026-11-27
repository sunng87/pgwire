//! Connection termination after fatal errors, cancel requests, and client shutdown.

use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::{Sink, stream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use pgwire::api::auth::StartupHandler;
use pgwire::api::cancel::CancelHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{QueryResponse, Response};
use pgwire::api::{ClientInfo, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult, SEVERITY_ERROR, SEVERITY_FATAL};
use pgwire::messages::cancel::CancelRequest;
use pgwire::messages::simplequery::Query;
use pgwire::messages::startup::{SecretKey, Startup};
use pgwire::messages::terminate::Terminate;
use pgwire::messages::{Message, PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

/// Rejects every connection at startup and every query, each with its own
/// severity.
#[derive(Clone, Copy)]
struct RejectingProcessor {
    /// Severity the startup handler rejects with.
    startup_severity: &'static str,
    /// Severity the query handler rejects with.
    query_severity: &'static str,
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
            self.startup_severity.to_owned(),
            "28000".to_owned(),
            "rejected".to_owned(),
        ))))
    }
}

#[async_trait]
impl SimpleQueryHandler for RejectingProcessor {
    async fn do_query<C>(&self, _client: &mut C, _query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            self.query_severity.to_owned(),
            "28000".to_owned(),
            "rejected".to_owned(),
        ))))
    }
}

impl PgWireServerHandlers for RejectingProcessor {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(*self)
    }

    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(*self)
    }
}

/// Serves one connection and sends it a StartupMessage. Returns the client
/// socket, still open, and the server task.
async fn connect(
    handlers: impl PgWireServerHandlers + Send,
) -> (TcpStream, JoinHandle<std::io::Result<()>>) {
    let (mut client, server) = serve(handlers).await;
    let mut startup = Startup::new();
    startup
        .parameters
        .insert("user".to_owned(), "postgres".to_owned());
    send(&mut client, &startup).await;
    (client, server)
}

/// Serves one connection. Returns the client socket and the server task.
async fn serve(
    handlers: impl PgWireServerHandlers + Send,
) -> (TcpStream, JoinHandle<std::io::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        process_socket(stream, None, handlers).await
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
    let processor = RejectingProcessor {
        startup_severity: SEVERITY_FATAL,
        query_severity: SEVERITY_ERROR,
    };
    let (mut client, server) = connect(processor).await;
    assert_eq!(read_message(&mut client).await, b'E');

    // The client neither hangs up nor sends anything else.
    assert_server_ends(server).await;
}

#[tokio::test]
async fn non_fatal_error_keeps_connection() {
    let processor = RejectingProcessor {
        startup_severity: SEVERITY_ERROR,
        query_severity: SEVERITY_ERROR,
    };
    let (mut client, server) = connect(processor).await;
    assert_eq!(read_message(&mut client).await, b'E');
    assert_eq!(read_message(&mut client).await, b'Z');

    // The server still answers the query, and the connection stays open.
    send(&mut client, &Query::new("SELECT 1".to_owned())).await;
    assert_eq!(read_message(&mut client).await, b'E');
    assert_eq!(read_message(&mut client).await, b'Z');

    // The server only ends the connection once the client terminates it.
    send(&mut client, &Terminate::default()).await;
    assert_server_ends(server).await;
}

#[tokio::test]
async fn fatal_query_error_ends_connection_while_client_holds_socket() {
    let processor = RejectingProcessor {
        startup_severity: SEVERITY_ERROR,
        query_severity: SEVERITY_FATAL,
    };
    let (mut client, server) = connect(processor).await;
    assert_eq!(read_message(&mut client).await, b'E');
    assert_eq!(read_message(&mut client).await, b'Z');

    // The query's FATAL answer ends the connection.
    send(&mut client, &Query::new("SELECT 1".to_owned())).await;
    assert_eq!(read_message(&mut client).await, b'E');
    assert_server_ends(server).await;
}

#[tokio::test]
async fn cancel_request_ends_connection_while_client_holds_socket() {
    // A CancelRequest uses the default cancel handler.
    let processor = RejectingProcessor {
        startup_severity: SEVERITY_FATAL,
        query_severity: SEVERITY_ERROR,
    };
    let (mut client, server) = serve(processor).await;
    send(&mut client, &CancelRequest::new(1, SecretKey::I32(1))).await;

    // The client neither hangs up nor sends anything else.
    assert_server_ends(server).await;
}

struct RecordingCancelHandler(Arc<AtomicBool>);

#[async_trait]
impl CancelHandler for RecordingCancelHandler {
    async fn on_cancel_request(&self, _: CancelRequest) {
        // Simulate an asynchronous cancel handler that is still pending at EOF.
        sleep(Duration::from_millis(25)).await;
        self.0.store(true, Ordering::Relaxed);
    }
}

impl PgWireServerHandlers for RecordingCancelHandler {
    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::new(Self(self.0.clone()))
    }
}

#[tokio::test]
async fn cancel_request_completes_after_client_shuts_down_write_half() {
    let handled = Arc::new(AtomicBool::new(false));
    let (mut client, server) = serve(RecordingCancelHandler(handled.clone())).await;
    send(&mut client, &CancelRequest::new(1, SecretKey::I32(1))).await;
    client.shutdown().await.unwrap();

    assert_server_ends(server).await;
    assert!(handled.load(Ordering::Relaxed), "CancelRequest was dropped");
}

/// Waits for the cancellation callback before returning a query error.
#[derive(Clone, Default)]
struct CancelAwareProcessor(Arc<PendingQuery>);

#[derive(Default)]
struct PendingQuery {
    row_stream_polled: Notify,
    cancel_requested: Notify,
    cancel_observed: Notify,
    cancel_completed: Notify,
    backend_key: OnceLock<(i32, SecretKey)>,
}

#[async_trait]
impl SimpleQueryHandler for CancelAwareProcessor {
    async fn do_query<C>(&self, client: &mut C, _query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query = self.0.clone();
        assert!(query.backend_key.set(client.pid_and_secret_key()).is_ok());
        Ok(vec![Response::Query(QueryResponse::new(
            Arc::new(vec![]),
            stream::once(async move {
                query.row_stream_polled.notify_one();
                query.cancel_requested.notified().await;
                query.cancel_observed.notify_one();
                query.cancel_completed.notified().await;
                Err(PgWireError::QueryCanceled)
            }),
        ))])
    }
}

#[async_trait]
impl CancelHandler for CancelAwareProcessor {
    async fn on_cancel_request(&self, request: CancelRequest) {
        let (pid, key) = self.0.backend_key.get().unwrap();
        assert_eq!(request.pid, *pid);
        assert!(request.secret_key.eq_value(key));
        self.0.cancel_requested.notify_one();
        self.0.cancel_observed.notified().await;
        self.0.cancel_completed.notify_one();
    }
}

impl PgWireServerHandlers for CancelAwareProcessor {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(self.clone())
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::new(self.clone())
    }
}

#[tokio::test]
async fn client_write_shutdown_lets_dispatch_finish_after_cancellation() {
    let processor = CancelAwareProcessor::default();
    let (mut client, server) = connect(processor.clone()).await;
    timeout(Duration::from_secs(5), async {
        while read_message(&mut client).await != b'Z' {}
    })
    .await
    .expect("the server did not finish startup");
    send(&mut client, &Query::new("SELECT 1".to_owned())).await;
    timeout(
        Duration::from_secs(5),
        processor.0.row_stream_polled.notified(),
    )
    .await
    .expect("the server did not start polling the query results");

    client.shutdown().await.unwrap();
    timeout(Duration::from_secs(5), async {
        while read_message(&mut client).await != b'E' {}
        assert_eq!(read_message(&mut client).await, b'Z');
    })
    .await
    .expect("dispatch did not finish sending the cancellation response");
    assert_server_ends(server).await;
}
