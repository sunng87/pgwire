use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::stream;
use futures::StreamExt;

use pgwire::api::auth::scram::{gen_salted_password, MakeSASLScramAuthStartupHandler};
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, MakeHandler, Type};
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

const ITERATIONS: usize = 4096;
struct DummyAuthSource;

#[async_trait]
impl AuthSource for DummyAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        println!("login info: {:?}", login_info);

        let password = "pencil";
        let salt = vec![0, 20, 40, 80];

        let hash_password = gen_salted_password(password, salt.as_ref(), ITERATIONS);
        Ok(Password::new(Some(salt), hash_password))
    }
}

#[derive(Default)]
struct DummyDatabase {
    query_parser: Arc<NoopQueryParser>,
}

impl DummyDatabase {
    fn schema(&self, format: &Format) -> Vec<FieldInfo> {
        let f1 = FieldInfo::new("id".into(), None, None, Type::INT4, format.format_for(0));
        let f2 = FieldInfo::new(
            "name".into(),
            None,
            None,
            Type::VARCHAR,
            format.format_for(1),
        );
        let f3 = FieldInfo::new(
            "ts".into(),
            None,
            None,
            Type::TIMESTAMP,
            format.format_for(2),
        );
        let f4 = FieldInfo::new(
            "signed".into(),
            None,
            None,
            Type::BOOL,
            format.format_for(3),
        );
        vec![f1, f2, f3, f4]
    }
}

#[async_trait]
impl SimpleQueryHandler for DummyDatabase {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("simple query: {:?}", query);
        if query.starts_with("SELECT") {
            let schema = Arc::new(self.schema(&Format::UnifiedText));
            let schema_ref = schema.clone();
            let data = vec![
                (
                    Some(0),
                    Some("Tom"),
                    Some("2023-02-01 22:27:25.042674"),
                    Some(true),
                ),
                (
                    Some(1),
                    Some("Jerry"),
                    Some("2023-02-01 22:27:42.165585"),
                    Some(false),
                ),
                (Some(2), None, None, None),
            ];
            let data_row_stream = stream::iter(data.into_iter()).map(move |r| {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());

                encoder.encode_field(&r.0)?;
                encoder.encode_field(&r.1)?;
                encoder.encode_field(&r.2)?;
                encoder.encode_field(&r.3)?;

                encoder.finish()
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

#[async_trait]
impl ExtendedQueryHandler for DummyDatabase {
    type Statement = String;
    type QueryParser = NoopQueryParser;

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
        let query = &portal.statement.statement;
        println!("extended query: {:?}", query);
        if query.starts_with("SELECT") {
            let data = vec![
                (Some(0), Some("Tom"), Some(SystemTime::now()), Some(true)),
                (
                    Some(1),
                    Some("Jerry"),
                    Some(SystemTime::UNIX_EPOCH + Duration::from_secs(86400 * 5000)),
                    Some(false),
                ),
                (Some(2), None, None, None),
            ];
            let schema = Arc::new(self.schema(&portal.result_column_format));
            let schema_ref = schema.clone();
            let data_row_stream = stream::iter(data.into_iter()).map(move |r| {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());

                encoder.encode_field(&r.0)?;
                encoder.encode_field(&r.1)?;
                encoder.encode_field(&r.2)?;
                encoder.encode_field(&r.3)?;

                encoder.finish()
            });

            Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
        } else {
            Ok(Response::Execution(Tag::new("OK").with_rows(1)))
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
        println!("describe: {:?}", stmt);
        let param_types = vec![Type::INT4];
        let schema = self.schema(&Format::UnifiedText);
        Ok(DescribeStatementResponse::new(param_types, schema))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("describe: {:?}", portal);
        let schema = self.schema(&portal.result_column_format);
        Ok(DescribePortalResponse::new(schema))
    }
}

struct MakeDummyDatabase;

impl MakeHandler for MakeDummyDatabase {
    type Handler = Arc<DummyDatabase>;

    fn make(&self) -> Self::Handler {
        Arc::new(DummyDatabase::default())
    }
}

#[tokio::main]
pub async fn main() {
    let mut authenticator = MakeSASLScramAuthStartupHandler::new(
        Arc::new(DummyAuthSource),
        Arc::new(DefaultServerParameterProvider::default()),
    );
    authenticator.set_iterations(ITERATIONS);
    let processor = Arc::new(MakeDummyDatabase);
    let noop_copy_handler = Arc::new(NoopCopyHandler);

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let copy_handler_ref = noop_copy_handler.clone();

        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref.clone(),
                processor_ref,
                copy_handler_ref,
            )
            .await
        });
    }
}
