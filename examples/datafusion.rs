use std::sync::Arc;

use async_trait::async_trait;
use datafusion::prelude::*;
use futures::{stream, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    query_response, DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::tokio::process_socket;

pub struct DfSessionService {
    session_context: Arc<Mutex<SessionContext>>,
}

impl DfSessionService {
    pub fn new() -> DfSessionService {
        DfSessionService {
            session_context: Arc::new(Mutex::new(SessionContext::new())),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for DfSessionService {
    async fn do_query<'a, C>(&self, _client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("{:?}", query);
        if query.starts_with("LOAD") {
            let commands = query.split(" ").collect::<Vec<&str>>();
            let table_name = commands[1];
            let csv_path = commands[2];
            let ctx = self.session_context.lock().await;
            ctx.register_csv(table_name, csv_path, CsvReadOptions::new())
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            Ok(vec![Response::Execution(Tag::new_for_execution(
                "OK",
                Some(1),
            ))])
        } else if query.starts_with("SELECT") {
            let ctx = self.session_context.lock().await;
            let df = ctx
                .sql(query)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let resp = encode_dataframe(df).await?;
            Ok(vec![Response::Query(resp)])
        } else {
            Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "Datafusion is a readonly execution engine.".to_owned(),
            )))])
        }
    }
}

async fn encode_dataframe<'a>(df: DataFrame) -> PgWireResult<QueryResponse<'a>> {
    let schema = df.schema();

    let recordbatch_stream = df
        .execute_stream()
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    todo!()
}

#[tokio::main]
pub async fn main() {
    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(DfSessionService::new())));
    // We have not implemented extended query in this server, use placeholder instead
    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
            )
            .await
        });
    }
}
