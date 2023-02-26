use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::stream;
use futures::StreamExt;
use pgwire::api::auth::scram::{gen_salted_password, MakeSASLScramAuthStartupHandler};
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, ServerParameterProvider,
};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    query_response, DataRowEncoder, DescribeResponse, FieldFormat, FieldInfo, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::MemPortalStore;
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

struct DummyParameters;

impl ServerParameterProvider for DummyParameters {
    fn server_parameters<C>(&self, client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let provider = DefaultServerParameterProvider;
        if let Some(mut params) = provider.server_parameters(client) {
            params.insert("server_version".to_owned(), "15.1".to_owned());
            params.insert("integer_datetimes".to_owned(), "on".to_owned());
            Some(params)
        } else {
            None
        }
    }
}

#[derive(Default)]
struct DummyDatabase {
    portal_store: Arc<MemPortalStore<String>>,
    query_parser: Arc<NoopQueryParser>,
}

#[async_trait]
impl SimpleQueryHandler for DummyDatabase {
    async fn do_query<'a, C>(&self, _client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("simple query: {:?}", query);
        if query.starts_with("SELECT") {
            let f1 = FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text);
            let f2 = FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text);
            let f3 = FieldInfo::new("ts".into(), None, None, Type::TIMESTAMP, FieldFormat::Text);

            let data = vec![
                (Some(0), Some("Tom"), Some("2023-02-01 22:27:25.042674")),
                (Some(1), Some("Jerry"), Some("2023-02-01 22:27:42.165585")),
                (Some(2), None, None),
            ];
            let data_row_stream = stream::iter(data.into_iter()).map(|r| {
                let mut encoder = DataRowEncoder::new(3);
                encoder.encode_text_format_field(r.0.as_ref())?;
                encoder.encode_text_format_field(r.1.as_ref())?;
                encoder.encode_text_format_field(r.2.as_ref())?;

                encoder.finish()
            });

            Ok(vec![Response::Query(query_response(
                Some(vec![f1, f2, f3]),
                data_row_stream,
            ))])
        } else {
            Ok(vec![Response::Execution(Tag::new_for_execution(
                "OK",
                Some(1),
            ))])
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for DummyDatabase {
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
        let query = portal.statement().statement();
        println!("extended query: {:?}", query);
        if query.starts_with("SELECT") {
            let data = vec![
                (Some(0), Some("Tom"), Some(SystemTime::now())),
                (
                    Some(1),
                    Some("Jerry"),
                    Some(SystemTime::UNIX_EPOCH + Duration::from_secs(86400 * 5000)),
                ),
                (Some(2), None, None),
            ];
            let result_column_format = portal.result_column_format();
            let data_row_stream = stream::iter(data.into_iter()).map(|r| {
                let mut encoder = DataRowEncoder::new(3);

                if result_column_format.is_binary(0) {
                    encoder.encode_binary_format_field(&r.0, &Type::INT4)?;
                } else {
                    encoder.encode_text_format_field(r.0.as_ref())?;
                }

                if result_column_format.is_binary(1) {
                    encoder.encode_binary_format_field(&r.1, &Type::VARCHAR)?;
                } else {
                    encoder.encode_text_format_field(r.1.as_ref())?;
                }

                if result_column_format.is_binary(2) {
                    encoder.encode_binary_format_field(&r.2, &Type::TIMESTAMP)?;
                } else {
                    encoder.encode_text_format_field(Some(
                        &"2023-02-01 22:27:42.165585".to_string(),
                    ))?;
                }

                encoder.finish()
            });

            Ok(Response::Query(query_response(None, data_row_stream)))
        } else {
            Ok(Response::Execution(Tag::new_for_execution("OK", Some(1))))
        }
    }

    async fn do_describe<C>(
        &self,
        _client: &mut C,
        query: &StoredStatement<Self::Statement>,
        parameter_type_infer: bool,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("describe: {:?}", query);
        let f1 = FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text);
        let f2 = FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text);
        let f3 = FieldInfo::new(
            "name".into(),
            None,
            None,
            Type::TIMESTAMP,
            FieldFormat::Text,
        );
        Ok(DescribeResponse::new(
            if parameter_type_infer {
                Some(vec![Type::INT4])
            } else {
                None
            },
            vec![f1, f2, f3],
        ))
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
        Arc::new(DefaultServerParameterProvider),
    );
    authenticator.set_iterations(ITERATIONS);
    let processor = Arc::new(MakeDummyDatabase);

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
