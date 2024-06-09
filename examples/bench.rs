use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use futures::StreamExt;
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

pub struct DummyProcessor;

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

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
pub async fn main() {
    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(DummyProcessor)));
    // We have not implemented extended query in this server, use placeholder instead
    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));
    let noop_copy_handler = Arc::new(NoopCopyHandler);

    let server_addr = "127.0.0.1:5433";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();
        let copy_handler_ref = noop_copy_handler.clone();

        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
                copy_handler_ref,
            )
            .await
        });
    }
}
