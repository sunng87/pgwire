use std::sync::{Arc, Mutex};

use arrow_pg::datatypes::arrow_schema_to_pg_fields;
use arrow_pg::datatypes::encode_recordbatch;
use arrow_pg::datatypes::into_pg_type;
use async_trait::async_trait;
use duckdb::{params, Connection, Statement, ToSql};
use futures::stream;
use pgwire::api::auth::md5pass::{hash_md5_password, Md5PasswordAuthStartupHandler};
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

pub struct DuckDBBackend {
    conn: Arc<Mutex<Connection>>,
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

#[async_trait]
impl SimpleQueryHandler for DuckDBBackend {
    async fn do_query<'a, C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap();
        if query.to_uppercase().starts_with("SELECT") {
            let mut stmt = conn
                .prepare(query)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let ret = stmt
                .query_arrow(params![])
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let schema = ret.get_schema();
            let header = Arc::new(arrow_schema_to_pg_fields(
                schema.as_ref(),
                &Format::UnifiedText,
            )?);

            let header_ref = header.clone();
            let data = ret
                .flat_map(move |rb| encode_recordbatch(header_ref.clone(), rb))
                .collect::<Vec<_>>();
            Ok(vec![Response::Query(QueryResponse::new(
                header,
                stream::iter(data.into_iter()),
            ))])
        } else {
            conn.execute(query, params![])
                .map(|affected_rows| {
                    vec![Response::Execution(Tag::new("OK").with_rows(affected_rows))]
                })
                .map_err(|e| PgWireError::ApiError(Box::new(e)))
        }
    }
}

fn get_params(portal: &Portal<String>) -> Vec<Box<dyn ToSql>> {
    let mut results = Vec::with_capacity(portal.parameter_len());
    for i in 0..portal.parameter_len() {
        let param_type = portal.statement.parameter_types.get(i).unwrap();
        // we only support a small amount of types for demo
        match param_type {
            &Type::BOOL => {
                let param = portal.parameter::<bool>(i, param_type).unwrap();
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::INT2 => {
                let param = portal.parameter::<i16>(i, param_type).unwrap();
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::INT4 => {
                let param = portal.parameter::<i32>(i, param_type).unwrap();
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::INT8 => {
                let param = portal.parameter::<i64>(i, param_type).unwrap();
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::TEXT | &Type::VARCHAR => {
                let param = portal.parameter::<String>(i, param_type).unwrap();
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::FLOAT4 => {
                let param = portal.parameter::<f32>(i, param_type).unwrap();
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::FLOAT8 => {
                let param = portal.parameter::<f64>(i, param_type).unwrap();
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            _ => {
                unimplemented!("parameter type not supported")
            }
        }
    }

    results
}

fn row_desc_from_stmt(stmt: &Statement, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    let columns = stmt.column_count();

    (0..columns)
        .map(|idx| {
            let datatype = stmt.column_type(idx);
            let name = stmt.column_name(idx).unwrap();

            Ok(FieldInfo::new(
                name.clone(),
                None,
                None,
                into_pg_type(&datatype).unwrap(),
                format.format_for(idx),
            ))
        })
        .collect()
}

#[async_trait]
impl ExtendedQueryHandler for DuckDBBackend {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap();
        let query = &portal.statement.statement;
        let mut stmt = conn
            .prepare_cached(query)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let params = get_params(portal);
        let params_ref = params
            .iter()
            .map(|f| f.as_ref())
            .collect::<Vec<&dyn duckdb::ToSql>>();

        if query.to_uppercase().starts_with("SELECT") {
            let ret = stmt
                .query_arrow(params![])
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let schema = ret.get_schema();
            let header = Arc::new(arrow_schema_to_pg_fields(
                schema.as_ref(),
                &Format::UnifiedText,
            )?);

            let header_ref = header.clone();
            let data = ret
                .flat_map(move |rb| encode_recordbatch(header_ref.clone(), rb))
                .collect::<Vec<_>>();

            Ok(Response::Query(QueryResponse::new(
                header,
                stream::iter(data.into_iter()),
            )))
        } else {
            stmt.execute::<&[&dyn duckdb::ToSql]>(params_ref.as_ref())
                .map(|affected_rows| Response::Execution(Tag::new("OK").with_rows(affected_rows)))
                .map_err(|e| PgWireError::ApiError(Box::new(e)))
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap();
        let param_types = stmt.parameter_types.clone();
        let stmt = conn
            .prepare_cached(&stmt.statement)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        row_desc_from_stmt(&stmt, &Format::UnifiedBinary)
            .map(|fields| DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap();
        let stmt = conn
            .prepare_cached(&portal.statement.statement)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        row_desc_from_stmt(&stmt, &portal.result_column_format).map(DescribePortalResponse::new)
    }
}

impl DuckDBBackend {
    fn new() -> DuckDBBackend {
        DuckDBBackend {
            conn: Arc::new(Mutex::new(Connection::open_in_memory().unwrap())),
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }
}

struct DuckDBBackendFactory {
    handler: Arc<DuckDBBackend>,
}

impl PgWireServerHandlers for DuckDBBackendFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(Md5PasswordAuthStartupHandler::new(
            Arc::new(DummyAuthSource),
            Arc::new(DefaultServerParameterProvider::default()),
        ))
    }
}

#[tokio::main]
pub async fn main() {
    let factory = Arc::new(DuckDBBackendFactory {
        handler: Arc::new(DuckDBBackend::new()),
    });
    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!(
        "Listening to {}, use password `pencil` to connect",
        server_addr
    );
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();

        tokio::spawn(async move { process_socket(incoming_socket.0, None, factory_ref).await });
    }
}
