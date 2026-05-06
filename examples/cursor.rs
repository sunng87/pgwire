use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Sink, SinkExt, StreamExt, stream};
use tokio::net::TcpListener;

use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::StoredStatement;
use pgwire::api::store::{MemPortalStore, PortalStore};
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::NoticeResponse;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

mod common;

type Statement = String;

struct CursorBackend;

fn make_row_data() -> (Arc<Vec<FieldInfo>>, Vec<(i32, &'static str)>) {
    let schema = Arc::new(vec![
        FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text),
    ]);
    let data = vec![
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),
        (4, "Diana"),
        (5, "Eve"),
        (6, "Frank"),
        (7, "Grace"),
        (8, "Hank"),
        (9, "Ivy"),
        (10, "Jack"),
    ];
    (schema, data)
}

enum CursorCommand {
    Declare { name: String, inner_query: String },
    Fetch { name: String, count: usize },
    Close { name: String },
}

fn parse_cursor_command(query: &str) -> Option<CursorCommand> {
    let upper = query.to_uppercase();
    if upper.starts_with("DECLARE") {
        let rest = &query["DECLARE".len()..].trim_start();
        let parts: Vec<&str> = rest.splitn(2, "FOR").collect();
        if parts.len() == 2 {
            Some(CursorCommand::Declare {
                name: parts[0].trim().to_string(),
                inner_query: parts[1].trim().to_string(),
            })
        } else {
            None
        }
    } else if upper.starts_with("FETCH") {
        let rest = &query["FETCH".len()..].trim_start();
        let parts: Vec<&str> = rest.splitn(2, "FROM").collect();
        if parts.len() == 2 {
            Some(CursorCommand::Fetch {
                count: parts[0].trim().parse().unwrap_or(1),
                name: parts[1].trim().trim_end_matches(';').trim().to_string(),
            })
        } else {
            None
        }
    } else if upper.starts_with("CLOSE") {
        let rest = &query["CLOSE".len()..].trim_start();
        Some(CursorCommand::Close {
            name: rest.trim().trim_end_matches(';').trim().to_string(),
        })
    } else {
        None
    }
}

fn encode_row_data(
    data: Vec<(i32, &'static str)>,
    schema: Arc<Vec<FieldInfo>>,
) -> impl futures::Stream<Item = PgWireResult<pgwire::messages::data::DataRow>> + use<> {
    let mut encoder = DataRowEncoder::new(schema);
    stream::iter(data).map(move |(id, name)| {
        encoder.encode_field(&id)?;
        encoder.encode_field(&name)?;
        Ok(encoder.take_row())
    })
}

/// Demo stub: validates that the query starts with SELECT, then returns
/// hardcoded data. A real implementation would parse and execute the query.
fn execute_inner_query(inner_query: &str) -> PgWireResult<QueryResponse> {
    if !inner_query.to_uppercase().starts_with("SELECT") {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42809".to_owned(),
            "DECLARE CURSOR can only be used with SELECT queries".to_string(),
        ))));
    }

    let (schema, data) = make_row_data();
    let row_stream = encode_row_data(data, schema.clone());
    Ok(QueryResponse::new(schema, row_stream))
}

#[async_trait]
impl NoopStartupHandler for CursorBackend {
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
        println!("Client connected: {}", client.socket_addr());

        let notice = NoticeResponse::from(ErrorInfo::new(
            "INFO".into(),
            "00000".into(),
            "Cursor example server. Supported statements:\n  \
             DECLARE <name> FOR SELECT ...\n  \
             FETCH <n> FROM <name>\n  \
             CLOSE <name>\n  \
             SELECT ..."
                .into(),
        ));
        client
            .send(PgWireBackendMessage::NoticeResponse(notice))
            .await?;

        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for CursorBackend {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        println!("Query: {:?}", query);

        let portal_store = client
            .portal_store()
            .as_any()
            .downcast_ref::<MemPortalStore<Statement>>()
            .expect("expected MemPortalStore<String>");

        if let Some(cmd) = parse_cursor_command(query) {
            match cmd {
                CursorCommand::Declare { name, inner_query } => {
                    handle_declare(portal_store, &name, &inner_query)
                }
                CursorCommand::Fetch { name, count } => {
                    handle_fetch(portal_store, &name, count).await
                }
                CursorCommand::Close { name } => handle_close(portal_store, &name),
            }
        } else if query.to_uppercase().starts_with("SELECT") {
            let (schema, data) = make_row_data();
            let row_stream = encode_row_data(data, schema.clone());
            Ok(vec![Response::Query(QueryResponse::new(
                schema, row_stream,
            ))])
        } else {
            Ok(vec![Response::Execution(Tag::new("OK").with_rows(1))])
        }
    }
}

fn handle_declare(
    portal_store: &MemPortalStore<Statement>,
    cursor_name: &str,
    inner_query: &str,
) -> PgWireResult<Vec<Response>> {
    println!("DECLARE cursor '{}' FOR {}", cursor_name, inner_query);

    if !inner_query.to_uppercase().starts_with("SELECT") {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42809".to_owned(),
            "DECLARE CURSOR can only be used with SELECT queries".to_string(),
        ))));
    }

    let statement = StoredStatement::new(cursor_name.to_string(), inner_query.to_string(), vec![]);
    let portal = Portal::new_cursor(cursor_name.to_string(), Arc::new(statement));
    portal_store.put_portal(Arc::new(portal));

    Ok(vec![Response::Execution(Tag::new("DECLARE CURSOR"))])
}

async fn handle_fetch(
    portal_store: &MemPortalStore<Statement>,
    cursor_name: &str,
    count: usize,
) -> PgWireResult<Vec<Response>> {
    println!("FETCH {} FROM {}", count, cursor_name);

    let Some(portal) = portal_store.get_portal(cursor_name) else {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "34000".to_owned(),
            format!("cursor \"{}\" does not exist", cursor_name),
        ))));
    };

    // Lazy execution: if the cursor hasn't been started yet, execute the
    // stored statement now and transition to Suspended.
    if matches!(
        portal.state().lock().await.deref(),
        pgwire::api::portal::PortalExecutionState::Initial
    ) {
        let inner_query = &portal.statement.statement;
        println!("  -> Lazy execution of: {}", inner_query);
        let response = execute_inner_query(inner_query)?;
        portal.start(response).await;
    }

    let fetch_result = portal.fetch(count).await?;
    println!(
        "  -> Fetched {} rows, has_more: {}",
        fetch_result.rows.len(),
        fetch_result.suspended
    );

    let schema = fetch_result.row_schema;
    let row_stream = stream::iter(fetch_result.rows.into_iter().map(Ok));
    Ok(vec![Response::Query(QueryResponse::new(
        schema, row_stream,
    ))])
}

fn handle_close(
    portal_store: &MemPortalStore<Statement>,
    cursor_name: &str,
) -> PgWireResult<Vec<Response>> {
    println!("CLOSE {}", cursor_name);
    portal_store.rm_portal(cursor_name);
    Ok(vec![Response::Execution(Tag::new("CLOSE CURSOR"))])
}

struct CursorBackendFactory {
    handler: Arc<CursorBackend>,
}

impl PgWireServerHandlers for CursorBackendFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.handler.clone()
    }
}

#[tokio::main]
pub async fn main() {
    let factory = Arc::new(CursorBackendFactory {
        handler: Arc::new(CursorBackend),
    });

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    println!();
    println!("Try these commands with psql:");
    println!("  DECLARE my_cursor FOR SELECT * FROM users;");
    println!("  FETCH 3 FROM my_cursor;");
    println!("  FETCH 3 FROM my_cursor;");
    println!("  FETCH 3 FROM my_cursor;");
    println!("  FETCH 3 FROM my_cursor;");
    println!("  CLOSE my_cursor;");
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();
        tokio::spawn(async move { process_socket(incoming_socket.0, None, factory_ref).await });
    }
}
