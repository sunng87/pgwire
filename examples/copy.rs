//! Example demonstrating PostgreSQL COPY FROM STDIN and COPY TO STDOUT protocols.
//!
//! # COPY FROM STDIN Workflow:
//!
//! 1. **Client sends query**: `COPY <table> FROM STDIN`
//! 2. **Server responds**: `Response::CopyIn(CopyResponse::new(format, columns, stream))`
//!    - `format`: 0 for text, 1 for binary
//!    - `columns`: number of columns in the table
//!    - `stream`: empty for CopyIn (data comes from client)
//! 3. **Client sends data**: Multiple `CopyData` messages
//!    - Each message triggers `CopyHandler::on_copy_data()` callback
//!    - Server processes/stores the data
//! 4. **Completion**:
//!    - Success: Client sends `CopyDone` → `CopyHandler::on_copy_done()` called
//!    - Failure: Client sends `CopyFail` → `CopyHandler::on_copy_fail()` called
//!
//! # Testing with psql:
//!
//! ```bash
//! # Start the server
//! cargo run --example copy
//!
//! # In another terminal, connect with psql
//! psql -h 127.0.0.1 -p 5432 -U postgres
//!
//! # Send data using COPY FROM STDIN
//! COPY FROM STDIN;
//! 1<TAB>Alice
//! 2<TAB>Bob
//! \.
//!
//! # Or use COPY TO STDOUT to receive data
//! COPY TO STDOUT;
//! ```

use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use futures::{Sink, SinkExt, StreamExt, stream};
use tokio::net::TcpListener;

use pgwire::api::copy::CopyHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{CopyEncoder, CopyResponse, FieldFormat, FieldInfo, Response, Tag};
use pgwire::api::{ClientInfo, PgWireConnectionState, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::response::NoticeResponse;
use pgwire::tokio::process_socket;

pub struct DummyProcessor {
    received_data: Arc<Mutex<VecDeque<Vec<u8>>>>,
}

#[async_trait]
impl SimpleQueryHandler for DummyProcessor {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client
            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                ErrorInfo::new(
                    "NOTICE".to_owned(),
                    "01000".to_owned(),
                    format!("Query received {}", query),
                ),
            )))
            .await?;

        if query.to_ascii_uppercase().starts_with("COPY FROM STDIN") {
            let f1 = FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text);
            let f2 = FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text);
            let schema = Arc::new(vec![f1, f2]);

            Ok(vec![Response::CopyIn(CopyResponse::new(
                0,
                schema.len(),
                futures::stream::empty(),
            ))])
        } else if query.to_ascii_uppercase().starts_with("COPY TO STDOUT") {
            let f1 = FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text);
            let f2 = FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text);
            let schema = Arc::new(vec![f1, f2]);

            let data = vec![
                (Some(0), Some("Tom")),
                (Some(1), Some("Jerry")),
                (Some(2), None),
            ];

            let mut encoder = CopyEncoder::new_binary(schema.clone());
            // we need to chain an empty finish packet
            let copy_stream = stream::iter(data).map(move |r| {
                encoder.encode_field(&r.0)?;
                encoder.encode_field(&r.1)?;

                Ok(encoder.take_copy())
            });

            // copy out
            Ok(vec![Response::CopyOut(CopyResponse::new(
                1, // binary
                schema.len(),
                copy_stream,
            ))])
        } else {
            Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "FATAL".to_owned(),
                "08P01".to_owned(),
                "COPY FROM STDIN / COPY TO STDOUT expected.".to_string(),
            )))])
        }
    }
}

#[async_trait]
impl CopyHandler for DummyProcessor {
    async fn on_copy_data<C>(&self, client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        use PgWireConnectionState::*;
        assert!(matches!(client.state(), CopyInProgress(_)));

        let data = copy_data.data.clone();
        self.received_data.lock().unwrap().push_back(data.to_vec());

        println!("Received {} bytes of copy data", data.len());
        if let Ok(text) = std::str::from_utf8(&data) {
            println!("Data content: {:?}", text);
        }

        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        use PgWireConnectionState::*;
        assert!(matches!(client.state(), CopyInProgress(_)));

        let (chunk_count, total_bytes) = {
            let data = self.received_data.lock().unwrap();
            let total_bytes: usize = data.iter().map(|d| d.len()).sum();
            (data.len(), total_bytes)
        };

        println!(
            "Copy complete! Received {} chunks, {} total bytes",
            chunk_count, total_bytes
        );

        let tag = Tag::new("COPY").with_rows(chunk_count);
        client
            .send(PgWireBackendMessage::CommandComplete(tag.into()))
            .await?;

        Ok(())
    }

    async fn on_copy_fail<C>(&self, client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        use PgWireConnectionState::*;
        // This is set by the `on_query` implementations while handling a
        // `CopyIn`/`CopyOut`/`CopyBoth` response.
        assert!(matches!(client.state(), CopyInProgress(_)));

        println!("copy failed: {:?}", fail);

        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            format!("COPY IN mode terminated by the user: {}", fail.message),
        )))
    }
}

struct DummyProcessorFactory {
    handler: Arc<DummyProcessor>,
}

impl PgWireServerHandlers for DummyProcessorFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<impl CopyHandler> {
        self.handler.clone()
    }
}

#[tokio::main]
pub async fn main() {
    let processor = Arc::new(DummyProcessor {
        received_data: Arc::new(Mutex::new(VecDeque::new())),
    });

    let factory = Arc::new(DummyProcessorFactory { handler: processor });

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    println!("Connect with: psql -h 127.0.0.1 -p 5432 -U postgres");
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();
        tokio::spawn(async move { process_socket(incoming_socket.0, None, factory_ref).await });
    }
}
