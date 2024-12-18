use std::fs::File;
use std::io::{BufReader, Error as IOError, ErrorKind};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::stream;
use futures::StreamExt;
use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

use pgwire::api::auth::scram::{gen_salted_password, SASLScramAuthStartupHandler};
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
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
        let f5 = FieldInfo::new("data".into(), None, None, Type::BYTEA, format.format_for(4));
        vec![f1, f2, f3, f4, f5]
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
                    Some("tomcat".as_bytes()),
                ),
                (
                    Some(1),
                    Some("Jerry"),
                    Some("2023-02-01 22:27:42.165585"),
                    Some(false),
                    Some("".as_bytes()),
                ),
                (Some(2), None, None, None, None),
            ];
            let data_row_stream = stream::iter(data.into_iter()).map(move |r| {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());

                encoder.encode_field(&r.0)?;
                encoder.encode_field(&r.1)?;
                encoder.encode_field(&r.2)?;
                encoder.encode_field(&r.3)?;
                encoder.encode_field(&r.4)?;

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
                (
                    Some(0),
                    Some("Tom"),
                    Some(SystemTime::now()),
                    Some(true),
                    Some("tomcat".as_bytes()),
                ),
                (
                    Some(1),
                    Some("Jerry"),
                    Some(SystemTime::UNIX_EPOCH + Duration::from_secs(86400 * 5000)),
                    Some(false),
                    Some("".as_bytes()),
                ),
                (Some(2), None, None, None, None),
            ];
            let schema = Arc::new(self.schema(&portal.result_column_format));
            let schema_ref = schema.clone();
            let data_row_stream = stream::iter(data.into_iter()).map(move |r| {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());

                encoder.encode_field(&r.0)?;
                encoder.encode_field(&r.1)?;
                encoder.encode_field(&r.2)?;
                encoder.encode_field(&r.3)?;
                encoder.encode_field(&r.4)?;

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

struct DummyDatabaseFactory(Arc<DummyDatabase>);

impl PgWireServerHandlers for DummyDatabaseFactory {
    type StartupHandler =
        SASLScramAuthStartupHandler<DummyAuthSource, DefaultServerParameterProvider>;
    type SimpleQueryHandler = DummyDatabase;
    type ExtendedQueryHandler = DummyDatabase;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.0.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.0.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        let mut authenticator = SASLScramAuthStartupHandler::new(
            Arc::new(DummyAuthSource),
            Arc::new(DefaultServerParameterProvider::default()),
        );
        authenticator.set_iterations(ITERATIONS);

        Arc::new(authenticator)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

fn setup_tls() -> Result<TlsAcceptor, IOError> {
    let cert = certs(&mut BufReader::new(File::open(
        "../../examples/ssl/server.crt",
    )?))
    .collect::<Result<Vec<CertificateDer>, IOError>>()?;

    let key = pkcs8_private_keys(&mut BufReader::new(File::open(
        "../../examples/ssl/server.key",
    )?))
    .map(|key| key.map(PrivateKeyDer::from))
    .collect::<Result<Vec<PrivateKeyDer>, IOError>>()?
    .remove(0);

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert, key)
        .map_err(|err| IOError::new(ErrorKind::InvalidInput, err))?;

    config.alpn_protocols = vec![b"postgresql".to_vec()];

    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[tokio::main]
pub async fn main() {
    let factory = Arc::new(DummyDatabaseFactory(Arc::new(DummyDatabase::default())));

    let server_addr = "127.0.0.1:5432";
    let tls_acceptor = setup_tls().unwrap();
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let tls_acceptor_ref = tls_acceptor.clone();
        let factory_ref = factory.clone();

        tokio::spawn(async move {
            process_socket(incoming_socket.0, Some(tls_acceptor_ref), factory_ref).await
        });
    }
}
