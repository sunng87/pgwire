use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, StreamExt};
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{text_query_response, FieldInfo, Response, Tag, TextDataRowEncoder};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

pub struct DummyProcessor;

#[async_trait]
impl SimpleQueryHandler for DummyProcessor {
    async fn do_query<C>(&self, _client: &C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("{:?}", query);
        if query.starts_with("SELECT") {
            let f1 = FieldInfo::new("id".into(), None, None, Type::INT4);
            let f2 = FieldInfo::new("name".into(), None, None, Type::VARCHAR);

            let data = vec![
                (Some(0), Some("Tom")),
                (Some(1), Some("Jerry")),
                (Some(2), None),
            ];
            let data_row_stream = stream::iter(data.into_iter()).map(|r| {
                let mut encoder = TextDataRowEncoder::new(2);
                encoder.append_field(r.0.as_ref())?;
                encoder.append_field(r.1.as_ref())?;

                encoder.finish()
            });

            Ok(vec![Response::Query(text_query_response(
                vec![f1, f2],
                data_row_stream,
            ))])
        } else {
            Ok(vec![Response::Execution(Tag::new_for_execution(
                "OK",
                Some(1),
            ))])
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for DummyProcessor {
    async fn do_query<C>(&self, _client: &mut C, _portal: &Portal) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        todo!()
    }
}

#[tokio::main]
pub async fn main() {
    let processor = Arc::new(DummyProcessor);
    let authenticator = Arc::new(NoopStartupHandler);

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.clone();
        let processor_ref = processor.clone();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref.clone(),
                processor_ref,
            )
            .await
        });
    }
}
