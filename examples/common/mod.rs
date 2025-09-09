use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, Sink, StreamExt};

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{
    BoxRowStream, DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

pub struct DummyProcessor;

#[async_trait]
impl NoopStartupHandler for DummyProcessor {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!(
            "Client connected:\n- Socket Address: {}\n- TLS: {}\n- Protocol Version: {:?}\n- ProcessID/SecretKey: {:?}\n- Metadata: {:?}",
            client.socket_addr(),
            client.is_secure(),
            client.protocol_version(),
            client.pid_and_secret_key(),
            client.metadata(),
        );
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for DummyProcessor {
    async fn do_query<C>(
        &self,
        _client: &mut C,
        query: &str,
    ) -> PgWireResult<Vec<Response<BoxRowStream>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("{:?}", query);
        if query.starts_with("SELECT") {
            let f1 = FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text);
            let f2 = FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text);
            let schema = Arc::new(vec![f1, f2]);

            let data = vec![
                (Some(0), Some("Tom")),
                (Some(1), Some("Jerry")),
                (Some(2), None),
            ];
            let schema_ref = schema.clone();
            let data_row_stream = stream::iter(data.into_iter()).map(move |r| {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                encoder.encode_field(&r.0)?;
                encoder.encode_field(&r.1)?;

                encoder.finish()
            });

            Ok(vec![Response::Query(QueryResponse::new(
                schema,
                Box::new(data_row_stream),
            ))])
        } else {
            Ok(vec![Response::Execution(Tag::new("OK").with_rows(1))])
        }
    }
}

pub struct DummyProcessorFactory {
    pub handler: Arc<DummyProcessor>,
}

impl DummyProcessorFactory {
    pub fn new() -> DummyProcessorFactory {
        Self {
            handler: Arc::new(DummyProcessor),
        }
    }
}
