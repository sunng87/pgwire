use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

pub struct DummyProcessor;

#[async_trait]
impl SimpleQueryHandler for DummyProcessor {
    async fn do_query<'a, C>(&self, _client: &C, _query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let f1 = FieldInfo::new("?column?".into(), None, None, Type::INT4, FieldFormat::Text);
        let schema = Arc::new(vec![f1]);

        let schema_ref = schema.clone();
        let mut encoder = DataRowEncoder::new(schema_ref.clone());
        encoder.encode_field(&1)?;
        let row = encoder.finish();
        let data_row_stream = stream::iter(vec![row]);

        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            data_row_stream,
        ))])
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
pub async fn main() {
    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(DummyProcessor)));
    // We have not implemented extended query in this server, use placeholder instead
    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));

    let server_addr = "127.0.0.1:5433";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                NoopStartupHandler,
                processor_ref,
                placeholder_ref,
            )
            .await
        });
    }
}
