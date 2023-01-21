use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use futures::{Stream, StreamExt};
use pgwire::messages::data::DataRow;
use rusqlite::Rows;
use rusqlite::{types::ValueRef, Connection, Statement, ToSql};
use tokio::net::TcpListener;

// use pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use pgwire::api::auth::md5pass::MakeMd5PasswordAuthStartupHandler;
use pgwire::api::auth::{
    DefaultServerParameterProvider, LoginInfo, Password, PasswordVerifier, ServerParameterProvider,
};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    binary_query_response, text_query_response, BinaryDataRowEncoder, FieldInfo, Response, Tag,
    TextDataRowEncoder,
};
use pgwire::api::{ClientInfo, StatelessMakeHandler, Type};
use pgwire::error::{PgWireError, PgWireResult};
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

struct DummyPasswordVerifier;

#[async_trait]
impl PasswordVerifier for DummyPasswordVerifier {
    async fn verify_password<'a>(
        &self,
        login_info: LoginInfo<'a>,
        password: Password<'a>,
    ) -> PgWireResult<bool> {
        println!("login info: {:?}", login_info);
        println!("password: {:?}", password);
        Ok(true)
    }
}

struct SqliteParameters {
    version: &'static str,
}

impl SqliteParameters {
    fn new() -> SqliteParameters {
        SqliteParameters {
            version: rusqlite::version(),
        }
    }
}

impl ServerParameterProvider for SqliteParameters {
    fn server_parameters<C>(&self, client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let provider = DefaultServerParameterProvider;
        if let Some(mut params) = provider.server_parameters(client) {
            params.insert("server_version".to_owned(), self.version.to_owned());
            Some(params)
        } else {
            None
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for SqliteBackend {
    async fn do_query<C>(&self, _client: &C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap();
        if query.to_uppercase().starts_with("SELECT") {
            let mut stmt = conn
                .prepare(query)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let columns = stmt.column_count();
            let header = row_desc_from_stmt(&stmt);
            stmt.query(())
                .map(|rows| {
                    let s = encode_text_row_data(rows, columns);
                    vec![Response::Query(text_query_response(header, s))]
                })
                .map_err(|e| PgWireError::ApiError(Box::new(e)))
        } else {
            conn.execute(query, ())
                .map(|affected_rows| {
                    vec![Response::Execution(
                        Tag::new_for_execution("OK", Some(affected_rows)).into(),
                    )]
                })
                .map_err(|e| PgWireError::ApiError(Box::new(e)))
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

fn encode_text_row_data(
    mut rows: Rows,
    columns: usize,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    let mut results = Vec::new();
    while let Ok(Some(row)) = rows.next() {
        let mut encoder = TextDataRowEncoder::new(columns);
        for idx in 0..columns {
            let data = row.get_ref_unwrap::<usize>(idx);
            match data {
                ValueRef::Null => encoder.append_field(None::<&i8>).unwrap(),
                ValueRef::Integer(i) => {
                    encoder.append_field(Some(&i)).unwrap();
                }
                ValueRef::Real(f) => {
                    encoder.append_field(Some(&f)).unwrap();
                }
                ValueRef::Text(t) => {
                    encoder
                        .append_field(Some(&String::from_utf8_lossy(t)))
                        .unwrap();
                }
                ValueRef::Blob(b) => {
                    encoder.append_field(Some(&hex::encode(b))).unwrap();
                }
            }
        }

        results.push(encoder.finish());
    }

    stream::iter(results.into_iter())
}

fn encode_binary_row_data(
    mut rows: Rows,
    headers: Arc<Vec<FieldInfo>>,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    let mut results = Vec::new();
    while let Ok(Some(row)) = rows.next() {
        let mut encoder = BinaryDataRowEncoder::new(headers.clone());
        for idx in 0..headers.len() {
            let data = row.get_ref_unwrap::<usize>(idx);
            match data {
                ValueRef::Null => encoder.append_field(&None::<i8>).unwrap(),
                ValueRef::Integer(i) => {
                    encoder.append_field(&i).unwrap();
                }
                ValueRef::Real(f) => {
                    encoder.append_field(&f).unwrap();
                }
                ValueRef::Text(t) => {
                    encoder.append_field(&t).unwrap();
                }
                ValueRef::Blob(b) => {
                    encoder.append_field(&b).unwrap();
                }
            }
        }

        results.push(encoder.finish());
    }

    stream::iter(results.into_iter())
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
    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap();
        let query = portal.statement();
        let mut stmt = conn
            .prepare(query)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let params = get_params(portal);
        let params_ref = params
            .iter()
            .map(|f| f.as_ref())
            .collect::<Vec<&dyn rusqlite::ToSql>>();

        if query.to_uppercase().starts_with("SELECT") {
            let header = Arc::new(row_desc_from_stmt(&stmt));
            stmt.query::<&[&dyn rusqlite::ToSql]>(params_ref.as_ref())
                .map(|rows| {
                    let s = encode_binary_row_data(rows, header.clone()).take(max_rows);
                    Response::Query(binary_query_response(header, s))
                })
                .map_err(|e| PgWireError::ApiError(Box::new(e)))
        } else {
            stmt.execute::<&[&dyn rusqlite::ToSql]>(params_ref.as_ref())
                .map(|affected_rows| {
                    Response::Execution(Tag::new_for_execution("OK", Some(affected_rows)).into())
                })
                .map_err(|e| PgWireError::ApiError(Box::new(e)))
        }
    }
}

#[tokio::main]
pub async fn main() {
    let authenticator = Arc::new(MakeMd5PasswordAuthStartupHandler::new(
        Arc::new(DummyPasswordVerifier),
        Arc::new(SqliteParameters::new()),
    ));
    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(SqliteBackend::new())));

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
