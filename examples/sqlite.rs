use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use futures::Stream;
use pgwire::api::auth::md5pass::{hash_md5_password, MakeMd5PasswordAuthStartupHandler};
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, ServerParameterProvider,
};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler, StatementOrPortal};
use pgwire::api::results::{
    query_response, DataRowEncoder, DescribeResponse, FieldInfo, Response, Tag,
};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::store::MemPortalStore;
use pgwire::api::{ClientInfo, MakeHandler, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;
use rusqlite::Rows;
use rusqlite::{types::ValueRef, Connection, Statement, ToSql};
use tokio::net::TcpListener;

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
    async fn do_query<'a, C>(&self, _client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap();
        if query.to_uppercase().starts_with("SELECT") {
            let mut stmt = conn
                .prepare(query)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let header = Arc::new(row_desc_from_stmt(&stmt, &Format::UnifiedText)?);
            stmt.query(())
                .map(|rows| {
                    let s = encode_row_data(rows, header.clone());
                    vec![Response::Query(query_response(header, s))]
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

fn row_desc_from_stmt(stmt: &Statement, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    stmt.columns()
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let field_type = name_to_type(col.decl_type().unwrap())?;
            Ok(FieldInfo::new(
                col.name().to_owned(),
                None,
                None,
                field_type,
                format.format_for(idx),
            ))
        })
        .collect()
}

fn encode_row_data(
    mut rows: Rows,
    schema: Arc<Vec<FieldInfo>>,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    let mut results = Vec::new();
    let ncols = schema.len();
    while let Ok(Some(row)) = rows.next() {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for idx in 0..ncols {
            let data = row.get_ref_unwrap::<usize>(idx);
            match data {
                ValueRef::Null => encoder.encode_field(&None::<i8>).unwrap(),
                ValueRef::Integer(i) => {
                    encoder.encode_field(&i).unwrap();
                }
                ValueRef::Real(f) => {
                    encoder.encode_field(&f).unwrap();
                }
                ValueRef::Text(t) => {
                    encoder
                        .encode_field(&String::from_utf8_lossy(t).as_ref())
                        .unwrap();
                }
                ValueRef::Blob(b) => {
                    encoder.encode_field(&b).unwrap();
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
            &Type::INT2 => {
                let param = portal.parameter::<i16>(i).unwrap();
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
            _ => {
                unimplemented!("parameter type not supported")
            }
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

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
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
            let header = Arc::new(row_desc_from_stmt(&stmt, portal.result_column_format())?);
            stmt.query::<&[&dyn rusqlite::ToSql]>(params_ref.as_ref())
                .map(|rows| {
                    let s = encode_row_data(rows, header.clone());
                    Response::Query(query_response(header, s))
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
        target: StatementOrPortal<'_, Self::Statement>,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap();
        match target {
            StatementOrPortal::Statement(stmt) => {
                let param_types = Some(stmt.parameter_types().clone());
                let stmt = conn
                    .prepare_cached(stmt.statement())
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                row_desc_from_stmt(&stmt, &Format::UnifiedBinary)
                    .map(|fields| DescribeResponse::new(param_types, fields))
            }
            StatementOrPortal::Portal(portal) => {
                let stmt = conn
                    .prepare_cached(portal.statement().statement())
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                row_desc_from_stmt(&stmt, portal.result_column_format())
                    .map(|fields| DescribeResponse::new(None, fields))
            }
        }
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
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
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
