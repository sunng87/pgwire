//! A query is dropped once its client disconnects, even if it never sends it anything, like a
//! query aggregating a large table before returning its first row.

use std::fmt::Debug;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::{Sink, stream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::time::timeout;

use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{QueryResponse, Response};
use pgwire::api::{ClientInfo, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::simplequery::Query;
use pgwire::messages::startup::Startup;
use pgwire::messages::{Message, PgWireBackendMessage};
use pgwire::tokio::process_socket;

/// Answers every query with rows that never come.
#[derive(Default)]
struct EndlessProcessor {
    /// Held by the row stream until it is dropped.
    row_stream_polled: Arc<Notify>,
}

impl NoopStartupHandler for EndlessProcessor {}

#[async_trait]
impl SimpleQueryHandler for EndlessProcessor {
    async fn do_query<C>(&self, _client: &mut C, _query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let row_stream_polled = self.row_stream_polled.clone();
        Ok(vec![Response::Query(QueryResponse::new(
            Arc::new(vec![]),
            stream::poll_fn(move |_| {
                row_stream_polled.notify_one();
                Poll::Pending
            }),
        ))])
    }
}

struct Handlers(Arc<EndlessProcessor>);

impl PgWireServerHandlers for Handlers {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.0.clone()
    }

    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.0.clone()
    }
}

async fn send(client: &mut TcpStream, message: &impl Message) {
    let mut buf = BytesMut::new();
    message.encode(&mut buf).unwrap();
    client.write_all(&buf).await.unwrap();
}

/// Reads backend messages until a ReadyForQuery.
async fn read_until_ready_for_query(client: &mut TcpStream) {
    loop {
        let tag = client.read_u8().await.unwrap();
        let len = client.read_i32().await.unwrap() as usize;
        let mut body = vec![0; len - 4];
        client.read_exact(&mut body).await.unwrap();
        if tag == b'Z' {
            return;
        }
    }
}

#[tokio::test]
async fn client_disconnection_drops_running_query() {
    assert_disconnection_drops_running_query(false).await;
}

#[tokio::test]
async fn client_disconnection_drops_running_query_with_unread_data() {
    assert_disconnection_drops_running_query(true).await;
}

async fn assert_disconnection_drops_running_query(send_pipelined_query: bool) {
    let processor = Arc::new(EndlessProcessor::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut server = tokio::spawn({
        let handlers = Handlers(processor.clone());
        async move {
            let (stream, _) = listener.accept().await.unwrap();
            process_socket(stream, None, handlers).await
        }
    });

    let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut startup = Startup::new();
    startup
        .parameters
        .insert("user".to_owned(), "postgres".to_owned());
    send(&mut client, &startup).await;
    timeout(
        Duration::from_secs(5),
        read_until_ready_for_query(&mut client),
    )
    .await
    .expect("the server did not finish startup");
    send(&mut client, &Query::new("SELECT 1".to_owned())).await;
    timeout(
        Duration::from_secs(5),
        processor.row_stream_polled.notified(),
    )
    .await
    .expect("the server did not start polling the query results");

    if send_pipelined_query {
        // Keep the client connected long enough for the watcher to observe unread data.
        send(&mut client, &Query::new("SELECT 2".to_owned())).await;
        timeout(Duration::from_millis(500), &mut server)
            .await
            .expect_err("the server must keep serving a connected client");
    }

    drop(client);
    timeout(Duration::from_secs(5), server)
        .await
        .expect("the server kept running the query of a disconnected client")
        .unwrap()
        .unwrap();
    assert_eq!(
        Arc::strong_count(&processor.row_stream_polled),
        1,
        "the query must be dropped"
    );
}
