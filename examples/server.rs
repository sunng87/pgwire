use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::portal::Portal;
use tokio::net::TcpListener;

use pgwire::api::auth::CleartextPasswordAuthStartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, QueryResponse, SimpleQueryHandler};
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::{DataRow, FieldDescription, RowDescription, FORMAT_CODE_TEXT};
use pgwire::messages::response::CommandComplete;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;

pub struct DummyProcessor;

#[async_trait]
impl CleartextPasswordAuthStartupHandler for DummyProcessor {
    async fn verify_password(&self, password: &str) -> PgWireResult<bool> {
        Ok(password == "test")
    }

    fn server_parameters<C>(&self, _client: &C) -> std::collections::HashMap<String, String>
    where
        C: ClientInfo,
    {
        let mut data = HashMap::new();
        data.insert("application_name".into(), "psql".into());
        data.insert("integer_datetimes".into(), "on".into());
        data.insert("server_version".into(), "0.0.1".into());

        data
    }
}

#[async_trait]
impl SimpleQueryHandler for DummyProcessor {
    async fn do_query<C>(&self, _client: &C, query: &str) -> PgWireResult<QueryResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("{:?}", query);
        if query.starts_with("SELECT") {
            let mut rd = RowDescription::default();
            // column 0
            rd.fields_mut().push(FieldDescription::new(
                "id".into(),
                123,
                123,
                123,
                132,
                -1,
                FORMAT_CODE_TEXT,
            ));
            // column 1
            rd.fields_mut().push(FieldDescription::new(
                "name".into(),
                123,
                123,
                123,
                132,
                -1,
                FORMAT_CODE_TEXT,
            ));

            let mut data_row = DataRow::default();
            *data_row.fields_mut() = vec![Some("0".as_bytes().to_vec()), None];

            let rows = vec![data_row.clone(), data_row.clone(), data_row.clone()];

            let status = CommandComplete::new("SELECT 3".to_owned());
            Ok(QueryResponse::Data(rd, rows, status))
        } else {
            Ok(QueryResponse::Empty(CommandComplete::new(
                "OK 1".to_owned(),
            )))
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for DummyProcessor {
    async fn do_query<C>(&self, _client: &mut C, _portal: &Portal) -> PgWireResult<QueryResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        todo!()
    }
}

#[tokio::main]
pub async fn main() {
    let processor = Arc::new(DummyProcessor);

    let server_addr = "127.0.0.1:5433";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        process_socket(
            incoming_socket,
            processor.clone(),
            processor.clone(),
            processor.clone(),
        );
    }
}
