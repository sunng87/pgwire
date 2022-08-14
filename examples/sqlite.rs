use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::portal::Portal;
use rusqlite::{Connection, Row, Statement};
use tokio::net::TcpListener;

use pgwire::api::auth::CleartextPasswordAuthStartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, QueryResponse, SimpleQueryHandler};
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::{DataRow, FieldDescription, RowDescription, FORMAT_CODE_TEXT};
use pgwire::messages::response::CommandComplete;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;

pub struct SqliteBackend {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteBackend {
    fn new() -> SqliteBackend {
        SqliteBackend {
            conn: Arc::new(Mutex::new(Connection::open_in_memory().unwrap())),
        }
    }
}

#[async_trait]
impl CleartextPasswordAuthStartupHandler for SqliteBackend {
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
impl SimpleQueryHandler for SqliteBackend {
    async fn do_query<C>(&self, _client: &C, query: &str) -> PgWireResult<QueryResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap();
        if query.to_uppercase().starts_with("SELECT") {
            let mut stmt = conn.prepare(query).unwrap();
            let columns = stmt.column_count();
            let header = row_desc_from_stmt(&stmt);

            let rows = stmt.query(()).unwrap();
            let body = rows
                .mapped(|r| Ok(row_data_from_sqlite_row(r, columns)))
                .map(|r| r.unwrap())
                .collect::<Vec<DataRow>>();

            let tail = CommandComplete::new(format!("SELECT {:?}", body.len()));
            Ok(QueryResponse::Data(header, body, tail))
        } else {
            let affect_rows = conn.execute(query, ()).unwrap();
            Ok(QueryResponse::Empty(CommandComplete::new(format!(
                "OK {:?}",
                affect_rows
            ))))
        }
    }
}

fn row_desc_from_stmt(stmt: &Statement) -> RowDescription {
    // TODO: real field descriptions
    let fields = stmt
        .column_names()
        .into_iter()
        .map(|n| FieldDescription::new(n.to_owned(), 123, 123, 123, 12, 0, FORMAT_CODE_TEXT))
        .collect();
    RowDescription::new(fields)
}

fn row_data_from_sqlite_row(row: &Row, columns: usize) -> DataRow {
    let mut fields = Vec::with_capacity(columns);

    for idx in 0..columns {
        let data = row.get_unwrap::<usize, String>(idx);
        fields.push(Some(data.as_bytes().to_vec()));
    }

    DataRow::new(fields)
}

#[async_trait]
impl ExtendedQueryHandler for SqliteBackend {
    async fn do_query<C>(&self, client: &mut C, portal: &Portal) -> PgWireResult<QueryResponse>
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
    let processor = Arc::new(SqliteBackend::new());

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
