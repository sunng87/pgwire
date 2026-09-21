//! Regression tests for the copy-in sub-protocol state machine:
//!
//! * an extended-protocol COPY FROM STDIN is terminated by the client's
//!   Sync after CopyDone; the connection must return to normal operation
//!   (previously the Sync was swallowed in `CopyInProgress` state and the
//!   client deadlocked waiting for ReadyForQuery),
//! * a Sync received *during* an unfinished copy is a protocol violation,
//!   like PostgreSQL.

use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::{Sink, SinkExt, stream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::timeout;

use pgwire::api::Type;
use pgwire::api::auth::{self, StartupHandler};
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{CopyResponse, FieldInfo, Response, Tag};
use pgwire::api::stmt::QueryParser;
use pgwire::api::{ClientInfo, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::{CopyData, CopyDone};
use pgwire::messages::{PgWireBackendMessage as BackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

type SharedChunks = Arc<Mutex<Vec<Vec<u8>>>>;

/// Server-side processor implementing the minimal handler set: any query
/// starting with `COPY` starts a copy-in, everything else is a no-op.
struct CopyInProcessor {
    received: SharedChunks,
}

fn copy_in_response() -> Response {
    Response::CopyIn(CopyResponse::new(
        0, // text format
        2,
        stream::empty(),
    ))
}

#[async_trait]
impl SimpleQueryHandler for CopyInProcessor {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if query.starts_with("COPY") {
            Ok(vec![copy_in_response()])
        } else {
            Ok(vec![Response::Execution(Tag::new("OK"))])
        }
    }
}

struct StringQueryParser;

#[async_trait]
impl QueryParser for StringQueryParser {
    type Statement = String;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Option<Self::Statement>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(Some(sql.to_string()))
    }

    fn get_parameter_types(&self, _stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(vec![])
    }

    fn get_result_schema(
        &self,
        _stmt: &Self::Statement,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(vec![])
    }
}

#[async_trait]
impl ExtendedQueryHandler for CopyInProcessor {
    type Statement = String;
    type QueryParser = StringQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(StringQueryParser)
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if portal.statement.statement.starts_with("COPY") {
            Ok(copy_in_response())
        } else {
            Ok(Response::Execution(Tag::new("OK")))
        }
    }
}

#[async_trait]
impl StartupHandler for CopyInProcessor {
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
        if let PgWireFrontendMessage::Startup(startup) = &message {
            auth::save_startup_parameters_to_metadata(client, startup);
        }
        auth::finish_authentication(
            client,
            &pgwire::api::auth::DefaultServerParameterProvider::default(),
        )
        .await
    }
}

#[async_trait]
impl CopyHandler for CopyInProcessor {
    async fn on_copy_data<C>(&self, _client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.received.lock().unwrap().push(copy_data.data.to_vec());
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let rows = self.received.lock().unwrap().len();
        client
            .send(BackendMessage::CommandComplete(
                Tag::new("COPY").with_rows(rows).into(),
            ))
            .await?;
        Ok(())
    }
}

struct CopyInHandlers {
    processor: Arc<CopyInProcessor>,
}

impl PgWireServerHandlers for CopyInHandlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.processor.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.processor.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.processor.clone()
    }

    fn copy_handler(&self) -> Arc<impl CopyHandler> {
        self.processor.clone()
    }
}

async fn spawn_server() -> (u16, SharedChunks) {
    let received: SharedChunks = Arc::new(Mutex::new(Vec::new()));
    let handlers = Arc::new(CopyInHandlers {
        processor: Arc::new(CopyInProcessor {
            received: received.clone(),
        }),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let handlers = handlers.clone();
            tokio::spawn(async move {
                let _ = process_socket(stream, None, handlers).await;
            });
        }
    });
    (port, received)
}

/// Raw frontend speaking enough of the wire protocol to drive both query
/// protocols and the copy-in sub-protocol.
struct RawClient {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    buffer: Vec<u8>,
}

impl RawClient {
    async fn connect(port: u16) -> Self {
        let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let (reader, writer) = stream.into_split();
        let mut client = Self {
            reader,
            writer,
            buffer: Vec::new(),
        };
        // StartupMessage: protocol 3.0, user=postgres.
        let mut startup = Vec::new();
        startup.extend_from_slice(&196608u32.to_be_bytes());
        startup.extend_from_slice(b"user\0postgres\0\0");
        client
            .write_raw(&(startup.len() as u32 + 4).to_be_bytes())
            .await;
        client.write_raw(&startup).await;
        client
    }

    async fn write_raw(&mut self, bytes: &[u8]) {
        self.writer.write_all(bytes).await.unwrap();
        self.writer.flush().await.unwrap();
    }

    async fn send(&mut self, type_byte: u8, payload: &[u8]) {
        let mut message = vec![type_byte];
        message.extend_from_slice(&(payload.len() as u32 + 4).to_be_bytes());
        message.extend_from_slice(payload);
        self.write_raw(&message).await;
    }

    async fn query(&mut self, sql: &str) {
        let mut payload = sql.as_bytes().to_vec();
        payload.push(0);
        self.send(b'Q', &payload).await;
    }

    /// Parse: empty statement name, the query, zero parameter types.
    async fn parse(&mut self, sql: &str) {
        let mut payload = Vec::new();
        payload.push(0);
        payload.extend_from_slice(sql.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&0i16.to_be_bytes());
        self.send(b'P', &payload).await;
    }

    /// Bind: empty portal/statement names, no parameter or result formats.
    async fn bind(&mut self) {
        // portal\0, statement\0, 3x i16(0): param format codes,
        // parameters, result format codes.
        let payload = vec![0u8; 8];
        self.send(b'B', &payload).await;
    }

    /// Execute: empty portal name, unlimited rows.
    async fn execute(&mut self) {
        let mut payload = Vec::new();
        payload.push(0);
        payload.extend_from_slice(&0i32.to_be_bytes());
        self.send(b'E', &payload).await;
    }

    async fn sync(&mut self) {
        self.send(b'S', &[]).await;
    }

    async fn copy_data(&mut self, data: &[u8]) {
        self.send(b'd', data).await;
    }

    async fn copy_done(&mut self) {
        self.send(b'c', &[]).await;
    }

    /// Reads the next backend message as `(type, payload)`, bounded by a
    /// timeout so regressions surface as failures instead of hangs.
    async fn next_message(&mut self) -> (u8, Vec<u8>) {
        timeout(Duration::from_secs(10), async {
            loop {
                if self.buffer.len() >= 5 {
                    let len = u32::from_be_bytes([
                        self.buffer[1],
                        self.buffer[2],
                        self.buffer[3],
                        self.buffer[4],
                    ]) as usize;
                    if self.buffer.len() > len {
                        let type_byte = self.buffer[0];
                        let payload = self.buffer[5..len + 1].to_vec();
                        self.buffer.drain(..len + 1);
                        return (type_byte, payload);
                    }
                }
                let mut chunk = [0u8; 1024];
                let read = self.reader.read(&mut chunk).await.unwrap();
                assert!(read > 0, "server closed the connection");
                self.buffer.extend_from_slice(&chunk[..read]);
            }
        })
        .await
        .expect("timed out waiting for a backend message")
    }

    /// Reads messages until one of `wanted` message types arrives and
    /// returns it.
    async fn expect(&mut self, wanted: &str) -> (u8, Vec<u8>) {
        loop {
            let (type_byte, payload) = self.next_message().await;
            if wanted.contains(type_byte as char) {
                return (type_byte, payload);
            }
        }
    }

    async fn expect_ready_for_query(&mut self) {
        let (type_byte, _) = self.expect("Z").await;
        assert_eq!(type_byte as char, 'Z');
    }
}

#[tokio::test]
async fn extended_copy_in_completes_on_sync() {
    let (port, received) = spawn_server().await;
    let mut client = RawClient::connect(port).await;
    client.expect_ready_for_query().await;

    // Start a copy-in through the extended query protocol.
    client.parse("COPY FROM STDIN").await;
    client.bind().await;
    client.execute().await;
    let (type_byte, _) = client.expect("GE").await;
    assert_eq!(type_byte as char, 'G', "expected CopyInResponse");

    client.copy_data(b"chunk1").await;
    client.copy_data(b"chunk2").await;
    client.copy_done().await;

    // CommandComplete for the copy; the regression was that the
    // terminating Sync below was swallowed and this never arrived.
    let (type_byte, payload) = client.expect("CE").await;
    assert_eq!(type_byte as char, 'C');
    assert_eq!(&payload[..payload.len() - 1], b"COPY 2");

    client.sync().await;
    client.expect_ready_for_query().await;

    // The connection must serve further queries.
    client.query("SELECT 1").await;
    client.expect_ready_for_query().await;

    assert_eq!(
        &*received.lock().unwrap(),
        &[b"chunk1".to_vec(), b"chunk2".to_vec()]
    );
}

#[tokio::test]
async fn sync_during_unfinished_copy_is_rejected() {
    let (port, _received) = spawn_server().await;
    let mut client = RawClient::connect(port).await;
    client.expect_ready_for_query().await;

    client.parse("COPY FROM STDIN").await;
    client.bind().await;
    client.execute().await;
    client.expect("G").await;

    // A Sync without a preceding CopyDone abandons the copy; like
    // PostgreSQL this is a protocol violation.
    client.sync().await;
    let (type_byte, payload) = client.expect("EZ").await;
    assert_eq!(
        type_byte as char, 'E',
        "expected ErrorResponse, got {payload:?}"
    );

    // The connection recovers after the error.
    client.sync().await;
    client.expect_ready_for_query().await;
    client.query("SELECT 1").await;
    client.expect_ready_for_query().await;
}

#[tokio::test]
async fn simple_copy_in_round_trip() {
    let (port, received) = spawn_server().await;
    let mut client = RawClient::connect(port).await;
    client.expect_ready_for_query().await;

    client.query("COPY FROM STDIN").await;
    let (type_byte, _) = client.expect("GE").await;
    assert_eq!(type_byte as char, 'G');

    client.copy_data(b"data").await;
    client.copy_done().await;

    let (type_byte, payload) = client.expect("CE").await;
    assert_eq!(type_byte as char, 'C');
    assert_eq!(&payload[..payload.len() - 1], b"COPY 1");
    client.expect_ready_for_query().await;

    assert_eq!(&*received.lock().unwrap(), &[b"data".to_vec()]);
}
