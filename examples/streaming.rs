use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use futures::{Sink, SinkExt, Stream};
use pgwire::messages::data::DataRow;
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, PgWireHandlerFactory, Type};
use pgwire::error::ErrorInfo;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::response::NoticeResponse;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use tokio::time::{interval, Interval};

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
        println!("Connected: {}", client.socket_addr());
        client
            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                ErrorInfo::new(
                    "NOTICE".to_owned(),
                    "01000".to_owned(),
                    "This is an example demos streaming response from backend. Try `SELECT 1;` to see it in action."
                        .to_string(),
                ),
            )))
            .await?;
        Ok(())
    }
}

struct ResultStream {
    schema: Arc<Vec<FieldInfo>>,
    counter: usize,
    interval: Interval,
}

impl Stream for ResultStream {
    type Item = PgWireResult<DataRow>;

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.counter, Some(self.counter))
    }

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.counter >= 10 {
            Poll::Ready(None)
        } else {
            match Pin::new(&mut self.interval).poll_tick(cx) {
                Poll::Ready(_) => {
                    self.counter += 1;
                    let row = {
                        let mut encoder = DataRowEncoder::new(self.schema.clone());
                        encoder.encode_field(&Some(1))?;

                        encoder.finish()
                    };
                    Poll::Ready(Some(row))
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for DummyProcessor {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        _query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let f1 = FieldInfo::new("SELECT 1".into(), None, None, Type::INT4, FieldFormat::Text);
        let schema = Arc::new(vec![f1]);

        // generate 10 results
        let data_row_stream = ResultStream {
            schema: schema.clone(),
            counter: 0,
            interval: interval(Duration::from_secs(1)),
        };

        let resp = Response::Query(QueryResponse::new(schema, data_row_stream));
        Ok(vec![resp])
    }
}

struct DummyProcessorFactory {
    handler: Arc<DummyProcessor>,
}

impl PgWireHandlerFactory for DummyProcessorFactory {
    type StartupHandler = DummyProcessor;
    type SimpleQueryHandler = DummyProcessor;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

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
