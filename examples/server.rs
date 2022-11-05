use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{FieldInfo, Response, Tag, TextQueryResponseBuilder};
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

            let data = vec![vec![Some("0"), Some("Tom")], vec![Some("1"), Some("Jerry")]];
            let result_builder =
                TextQueryResponseBuilder::new(vec![f1, f2], stream::iter(data.into_iter()));
            Ok(vec![Response::Query(result_builder.into_response())])
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

    let server_addr = "127.0.0.1:5433";
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
