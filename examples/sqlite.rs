use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::Sink;
use postgres_types::Type;
use rusqlite::Rows;
use rusqlite::{types::ValueRef, Connection, Statement, ToSql};
use tokio::net::TcpListener;

use pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{FieldInfo, QueryResponseBuilder, Response, Tag};
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};
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
        data.insert("server_version".into(), rusqlite::version().into());

        data
    }
}

#[async_trait]
impl SimpleQueryHandler for SqliteBackend {
    async fn do_query<C>(&self, _client: &C, query: &str) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap();
        if query.to_uppercase().starts_with("SELECT") {
            let mut stmt = conn.prepare(query).unwrap();
            let columns = stmt.column_count();
            let header = row_desc_from_stmt(&stmt);
            let rows = stmt.query(()).unwrap();

            let mut builder = QueryResponseBuilder::new(header);
            encode_row_data(rows, columns, &mut builder);

            Ok(Response::Query(builder.build()))
        } else {
            let affected_rows = conn.execute(query, ()).unwrap();
            Ok(Response::Execution(
                Tag::new_for_execution("OK", Some(affected_rows)).into(),
            ))
        }
    }
}

fn name_to_type(name: &str) -> Type {
    dbg!(name);
    match name.to_uppercase().as_ref() {
        "INT" => Type::INT8,
        "VARCHAR" => Type::VARCHAR,
        "BINARY" => Type::BYTEA,
        "FLOAT" => Type::FLOAT8,
        _ => unimplemented!("unknown type"),
    }
}

fn row_desc_from_stmt(stmt: &Statement) -> Vec<FieldInfo> {
    stmt.columns()
        .iter()
        .map(|col| {
            FieldInfo::new(
                col.name().to_owned(),
                None,
                None,
                name_to_type(col.decl_type().unwrap()),
            )
        })
        .collect()
}

fn encode_row_data(mut rows: Rows, columns: usize, builder: &mut QueryResponseBuilder) {
    while let Ok(Some(row)) = rows.next() {
        for idx in 0..columns {
            let data = row.get_ref_unwrap::<usize>(idx);
            match data {
                ValueRef::Null => builder.append_field(None::<i8>).unwrap(),
                ValueRef::Integer(i) => {
                    builder.append_field(i).unwrap();
                }
                ValueRef::Real(f) => {
                    builder.append_field(f).unwrap();
                }
                ValueRef::Text(t) => {
                    builder.append_field(t).unwrap();
                }
                ValueRef::Blob(b) => {
                    builder.append_field(b).unwrap();
                }
            }
        }

        builder.finish_row();
    }
}

fn get_params(portal: &Portal) -> Vec<Box<dyn ToSql>> {
    let mut results = Vec::with_capacity(portal.parameter_len());
    for i in 0..portal.parameter_len() {
        let param_type = portal.parameter_types().get(i).unwrap();
        // we only support a small amount of types for demo
        match param_type {
            &Type::BOOL => {
                let param = portal.parameter::<bool>(i).unwrap();
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::INT4 => {
                let param = portal.parameter::<i32>(i).unwrap();
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::INT8 => {
                let param = portal.parameter::<i64>(i).unwrap();
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::TEXT | &Type::VARCHAR => {
                let param = portal.parameter::<String>(i).unwrap();
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::FLOAT4 => {
                let param = portal.parameter::<f32>(i).unwrap();
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::FLOAT8 => {
                let param = portal.parameter::<f64>(i).unwrap();
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            _ => {}
        }
    }

    results
}

#[async_trait]
impl ExtendedQueryHandler for SqliteBackend {
    async fn do_query<C>(&self, _client: &mut C, portal: &Portal) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn = self.conn.lock().unwrap();
        let query = portal.statement();
        let mut stmt = conn.prepare(query).unwrap();
        let params = get_params(portal);
        let params_ref = params
            .iter()
            .map(|f| f.as_ref())
            .collect::<Vec<&dyn rusqlite::ToSql>>();

        if query.to_uppercase().starts_with("SELECT") {
            let columns = stmt.column_count();
            let header = row_desc_from_stmt(&stmt);
            let rows = stmt
                .query::<&[&dyn rusqlite::ToSql]>(params_ref.as_ref())
                .unwrap();

            let mut builder = QueryResponseBuilder::new(header);
            encode_row_data(rows, columns, &mut builder);

            Ok(Response::Query(builder.build()))
        } else {
            let affected_rows = stmt
                .execute::<&[&dyn rusqlite::ToSql]>(params_ref.as_ref())
                .unwrap();
            Ok(Response::Execution(Tag::new_for_execution(
                "OK",
                Some(affected_rows),
            )))
        }
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
