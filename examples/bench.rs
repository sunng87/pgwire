use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use futures::StreamExt;
use pgwire::api::NoopErrorHandler;
use pgwire::api::PgWireServerHandlers;
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

pub struct DummyProcessor;

impl NoopStartupHandler for DummyProcessor {}

#[async_trait]
impl SimpleQueryHandler for DummyProcessor {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        _query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let f1 = FieldInfo::new("?column?".into(), None, None, Type::INT4, FieldFormat::Text);
        let f2 = FieldInfo::new("?column?".into(), None, None, Type::INT4, FieldFormat::Text);
        let f3 = FieldInfo::new("?column?".into(), None, None, Type::INT4, FieldFormat::Text);
        let f4 = FieldInfo::new(
            "?column?".into(),
            None,
            None,
            Type::TIMESTAMP,
            FieldFormat::Text,
        );
        let f5 = FieldInfo::new(
            "?column?".into(),
            None,
            None,
            Type::FLOAT8,
            FieldFormat::Text,
        );
        let f6 = FieldInfo::new("?column?".into(), None, None, Type::TEXT, FieldFormat::Text);
        let schema = Arc::new(vec![f1, f2, f3, f4, f5, f6]);

        let schema_ref = schema.clone();

        let data_row_stream = stream::iter(0..5000).map(move |n| {
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            encoder.encode_field(&n).unwrap();
            encoder.encode_field(&n).unwrap();
            encoder.encode_field(&n).unwrap();
            encoder.encode_field(&"2004-10-19 10:23:54+02").unwrap();
            encoder.encode_field(&42.0f64).unwrap();
            encoder.encode_field(&"This method splits the slice into three distinct slices: prefix, correctly aligned middle slice of a new type, and the suffix slice. How exactly the slice is split up is not specified; the middle part may be smaller than necessary. However, if this fails to return a maximal middle part, that is because code is running in a context where performance does not matter, such as a sanitizer attempting to find alignment bugs. Regular code running in a default (debug or release) execution will return a maximal middle part.").unwrap();

            encoder.finish()
        });

        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            data_row_stream,
        ))])
    }
}

struct DummyProcessorFactory {
    handler: Arc<DummyProcessor>,
}

impl PgWireServerHandlers for DummyProcessorFactory {
    type StartupHandler = DummyProcessor;
    type SimpleQueryHandler = DummyProcessor;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
pub async fn main() {
    let factory = Arc::new(DummyProcessorFactory {
        handler: Arc::new(DummyProcessor),
    });

    let server_addr = "127.0.0.1:5433";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();

        tokio::spawn(async move { process_socket(incoming_socket.0, None, factory_ref).await });
    }
}
