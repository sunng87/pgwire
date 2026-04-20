use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{Sink, SinkExt, StreamExt, stream};
use tokio::net::TcpListener;
use tokio::time::sleep;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::cancel::DefaultCancelHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, ConnectionManager, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::NoticeResponse;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

struct SlowProcessor {
    manager: Arc<ConnectionManager>,
}

#[async_trait]
impl NoopStartupHandler for SlowProcessor {
    fn connection_manager(&self) -> Option<Arc<ConnectionManager>> {
        Some(self.manager.clone())
    }

    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client
            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                ErrorInfo::new(
                    "NOTICE".to_owned(),
                    "01000".to_owned(),
                    "This example demonstrates query cancellation.\n\
                     Supported queries:\n\
                     - SELECT 1;            (instant query)\n\
                     - SELECT pg_sleep(10); (sleeps 10s, press Ctrl+C to cancel)"
                        .to_string(),
                ),
            )))
            .await?;
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for SlowProcessor {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        if query.trim().starts_with("SELECT pg_sleep") {
            sleep(Duration::from_secs(10)).await;
            Ok(vec![Response::Execution(Tag::new("SELECT 1"))])
        } else if query.trim().starts_with("SELECT") {
            let f1 = FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text);
            let schema = Arc::new(vec![f1]);
            let data = vec![(Some(1),), (Some(2),), (Some(3),)];
            let schema_ref = schema.clone();
            let data_row_stream = stream::iter(data).map(move |r| {
                let mut encoder = pgwire::api::results::DataRowEncoder::new(schema_ref.clone());
                encoder.encode_field(&r.0)?;
                Ok(encoder.take_row())
            });
            Ok(vec![Response::Query(QueryResponse::new(
                schema,
                data_row_stream,
            ))])
        } else {
            Ok(vec![Response::Execution(Tag::new("OK").with_rows(1))])
        }
    }
}

struct HandlerFactory {
    processor: Arc<SlowProcessor>,
    cancel_handler: Arc<DefaultCancelHandler>,
}

impl PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.processor.clone()
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        self.processor.clone()
    }

    fn cancel_handler(&self) -> Arc<impl pgwire::api::cancel::CancelHandler> {
        self.cancel_handler.clone()
    }
}

#[tokio::main]
pub async fn main() {
    let manager = Arc::new(ConnectionManager::new());
    let processor = Arc::new(SlowProcessor {
        manager: manager.clone(),
    });
    let cancel_handler = Arc::new(DefaultCancelHandler::new(manager));
    let factory = Arc::new(HandlerFactory {
        processor,
        cancel_handler,
    });

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    println!("Connect with: psql -h 127.0.0.1 -p 5432");
    println!("Test cancel: SELECT pg_sleep(10); then press Ctrl+C");
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();
        tokio::spawn(async move { process_socket(incoming_socket.0, None, factory_ref).await });
    }
}
