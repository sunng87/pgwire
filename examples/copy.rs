use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Sink, SinkExt, StreamExt, stream};
use tokio::net::TcpListener;

use pgwire::api::copy::CopyHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{CopyEncoder, CopyResponse, FieldFormat, FieldInfo, Response};
use pgwire::api::{ClientInfo, PgWireConnectionState, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::response::NoticeResponse;
use pgwire::tokio::process_socket;

pub struct DummyProcessor;

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
            // copy in
            Ok(vec![Response::CopyIn(CopyResponse::new(
                0,
                1,
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
            let copy_stream = stream::iter(data)
                .map(move |r| {
                    encoder.encode_field(&r.0)?;
                    encoder.encode_field(&r.1)?;

                    Ok(encoder.take_copy())
                })
                .chain(stream::once(async move { Ok(CopyEncoder::finish_copy(1)) }));

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
        // This is set by the `on_query` implementations while handling a
        // `CopyIn`/`CopyOut`/`CopyBoth` response.
        assert!(matches!(client.state(), CopyInProgress(_)));

        println!("receiving data: {:?}", copy_data);

        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        use PgWireConnectionState::*;
        // This is set by the `on_query` implementations while handling a
        // `CopyIn`/`CopyOut`/`CopyBoth` response.
        assert!(matches!(client.state(), CopyInProgress(_)));

        println!("copy done");

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
    let factory = Arc::new(DummyProcessorFactory {
        handler: Arc::new(DummyProcessor),
    });

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();
        tokio::spawn(async move { process_socket(incoming_socket.0, None, factory_ref).await });
    }
}
