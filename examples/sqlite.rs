use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use futures::Stream;
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::MemPortalStore;
use pgwire::messages::data::DataRow;
use rusqlite::Rows;
use rusqlite::{types::ValueRef, Connection, Statement, ToSql};
use tokio::net::TcpListener;

use pgwire::api::auth::md5pass::{hash_md5_password, MakeMd5PasswordAuthStartupHandler};
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, ServerParameterProvider,
};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{query_response, DataRowEncoder, FieldFormat, FieldInfo, Response, Tag};
use pgwire::api::{ClientInfo, MakeHandler, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::tokio::process_socket;

pub struct SqliteBackend {
    conn: Arc<Mutex<Connection>>,
    portal_store: Arc<MemPortalStore<String>>,
    query_parser: Arc<NoopQueryParser>,
}

struct DummyAuthSource;

#[async_trait]
impl AuthSource for DummyAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        println!("login info: {:?}", login_info);

        let salt = vec![0, 0, 0, 0];
        let password = "pencil";

        let hash_password =
            hash_md5_password(login_info.user().as_ref().unwrap(), password, salt.as_ref());
        Ok(Password::new(Some(salt), hash_password.as_bytes().to_vec()))
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
            let header = row_desc_from_stmt(&stmt, FieldFormat::Text)?;
            stmt.query(())
                .map(|rows| {
                    let s = encode_text_row_data(rows, columns);
                    vec![Response::Query(query_response(Some(header), s))]
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

fn name_to_type(name: &str) -> PgWireResult<Type> {
    dbg!(name);
    match name.to_uppercase().as_ref() {
        "INT" => Ok(Type::INT8),
        "VARCHAR" => Ok(Type::VARCHAR),
        "TEXT" => Ok(Type::TEXT),
        "BINARY" => Ok(Type::BYTEA),
        "FLOAT" => Ok(Type::FLOAT8),
        _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42846".to_owned(),
            format!("Unsupported data type: {name}"),
        )))),
    }
}

fn row_desc_from_stmt(stmt: &Statement, format: FieldFormat) -> PgWireResult<Vec<FieldInfo>> {
    stmt.columns()
        .iter()
        .map(|col| {
            let field_type = name_to_type(col.decl_type().unwrap())?;
            Ok(FieldInfo::new(
                col.name().to_owned(),
                None,
                None,
                field_type,
                format,
            ))
        })
        .collect()
}

fn encode_text_row_data(
    mut rows: Rows,
    columns: usize,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    let mut results = Vec::new();
    while let Ok(Some(row)) = rows.next() {
        let mut encoder = DataRowEncoder::new(columns);
        for idx in 0..columns {
            let data = row.get_ref_unwrap::<usize>(idx);
            match data {
                ValueRef::Null => encoder.encode_text_format_field(None::<&i8>).unwrap(),
                ValueRef::Integer(i) => {
                    encoder.encode_text_format_field(Some(&i)).unwrap();
                }
                ValueRef::Real(f) => {
                    encoder.encode_text_format_field(Some(&f)).unwrap();
                }
                ValueRef::Text(t) => {
                    encoder
                        .encode_text_format_field(Some(&String::from_utf8_lossy(t)))
                        .unwrap();
                }
                ValueRef::Blob(b) => {
                    encoder
                        .encode_text_format_field(Some(&hex::encode(b)))
                        .unwrap();
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
        let mut encoder = DataRowEncoder::new(headers.len());
        for idx in 0..headers.len() {
            let data = row.get_ref_unwrap::<usize>(idx);
            let dtype = headers[idx].datatype();
            match data {
                ValueRef::Null => encoder
                    .encode_binary_format_field(&None::<i8>, dtype)
                    .unwrap(),
                ValueRef::Integer(i) => {
                    encoder.encode_binary_format_field(&i, dtype).unwrap();
                }
                ValueRef::Real(f) => {
                    encoder.encode_binary_format_field(&f, dtype).unwrap();
                }
                ValueRef::Text(t) => {
                    encoder.encode_binary_format_field(&t, dtype).unwrap();
                }
                ValueRef::Blob(b) => {
                    encoder.encode_binary_format_field(&b, dtype).unwrap();
                }
            }
        }

        results.push(encoder.finish());
    }

    stream::iter(results.into_iter())
}

fn get_params(portal: &Portal<String>) -> Vec<Box<dyn ToSql>> {
    let mut results = Vec::with_capacity(portal.parameter_len());
    for i in 0..portal.parameter_len() {
        let param_type = portal.statement().parameter_types().get(i).unwrap();
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
    type Statement = String;
    type PortalStore = MemPortalStore<Self::Statement>;
    type QueryParser = NoopQueryParser;

    fn portal_store(&self) -> Arc<Self::PortalStore> {
        self.portal_store.clone()
    }

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap();
        let query = portal.statement().statement();
        let mut stmt = conn
            .prepare_cached(query)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let params = get_params(portal);
        let params_ref = params
            .iter()
            .map(|f| f.as_ref())
            .collect::<Vec<&dyn rusqlite::ToSql>>();

        if query.to_uppercase().starts_with("SELECT") {
            let header = Arc::new(row_desc_from_stmt(&stmt, FieldFormat::Binary)?);
            stmt.query::<&[&dyn rusqlite::ToSql]>(params_ref.as_ref())
                .map(|rows| {
                    let s = encode_binary_row_data(rows, header);
                    Response::Query(query_response(None, s))
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

    async fn do_describe<C>(
        &self,
        _client: &mut C,
        query: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<Vec<FieldInfo>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap();
        let stmt = conn
            .prepare_cached(query.statement())
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        row_desc_from_stmt(&stmt, FieldFormat::Binary)
    }
}

/// The parent handler that creates a handler instance for each incoming
/// connection.
struct MakeSqliteBackend {
    conn: Arc<Mutex<Connection>>,
    query_parser: Arc<NoopQueryParser>,
}

impl MakeSqliteBackend {
    fn new() -> MakeSqliteBackend {
        MakeSqliteBackend {
            conn: Arc::new(Mutex::new(Connection::open_in_memory().unwrap())),
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }
}

impl MakeHandler for MakeSqliteBackend {
    type Handler = Arc<SqliteBackend>;

    fn make(&self) -> Self::Handler {
        Arc::new(SqliteBackend {
            conn: self.conn.clone(),
            portal_store: Arc::new(MemPortalStore::new()),
            query_parser: self.query_parser.clone(),
        })
    }
}

#[tokio::main]
pub async fn main() {
    let authenticator = Arc::new(MakeMd5PasswordAuthStartupHandler::new(
        Arc::new(DummyAuthSource),
        Arc::new(SqliteParameters::new()),
    ));
    let processor = Arc::new(MakeSqliteBackend::new());

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
